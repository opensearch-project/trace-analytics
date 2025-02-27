/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.observability.index

import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.index.reindex.ReindexRequestBuilder
import org.opensearch.observability.ObservabilityPlugin.Companion.LOG_PREFIX
import org.opensearch.observability.action.GetObservabilityObjectRequest
import org.opensearch.observability.model.ObservabilityObjectDoc
import org.opensearch.observability.model.ObservabilityObjectDocInfo
import org.opensearch.observability.model.ObservabilityObjectSearchResult
import org.opensearch.observability.model.RestTag.ACCESS_LIST_FIELD
import org.opensearch.observability.model.RestTag.TENANT_FIELD
import org.opensearch.observability.model.SearchResults
import org.opensearch.observability.settings.PluginSettings
import org.opensearch.observability.util.SecureIndexClient
import org.opensearch.observability.util.logger
import org.opensearch.search.SearchHit
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.Client
import java.util.concurrent.TimeUnit

/**
 * Class for doing OpenSearch index operation to maintain observability objects in cluster.
 */
@Suppress("TooManyFunctions")
internal object ObservabilityIndex {
    private val log by logger(ObservabilityIndex::class.java)
    const val INDEX_NAME = ".opensearch-observability"
    const val NOTEBOOKS_INDEX_NAME = ".opensearch-notebooks"
    private const val OBSERVABILITY_MAPPING_FILE_NAME = "observability-mapping.yml"
    private const val OBSERVABILITY_SETTINGS_FILE_NAME = "observability-settings.yml"
    private const val SEARCH_ACTION_PREFERENCE = "_primary"

    private var mappingsUpdated: Boolean = false
    private lateinit var client: Client
    private lateinit var clusterService: ClusterService

    private val searchHitParser = object : SearchResults.SearchHitParser<ObservabilityObjectDoc> {
        override fun parse(searchHit: SearchHit): ObservabilityObjectDoc {
            val parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                searchHit.sourceAsString
            )
            parser.nextToken()
            return ObservabilityObjectDoc.parse(parser, searchHit.id)
        }
    }

    /**
     * Initialize the class
     * @param client The OpenSearch client
     * @param clusterService The OpenSearch cluster service
     */
    fun initialize(client: Client, clusterService: ClusterService) {
        this.client = SecureIndexClient(client)
        this.clusterService = clusterService
        this.mappingsUpdated = false
    }

    /**
     * Create index using the mapping and settings defined in resource
     */
    @Suppress("TooGenericExceptionCaught")
    private fun createIndex() {
        if (!isIndexExists(INDEX_NAME)) {
            val classLoader = ObservabilityIndex::class.java.classLoader
            val indexMappingSource = classLoader.getResource(OBSERVABILITY_MAPPING_FILE_NAME)?.readText()!!
            val indexSettingsSource = classLoader.getResource(OBSERVABILITY_SETTINGS_FILE_NAME)?.readText()!!
            val request = CreateIndexRequest(INDEX_NAME)
                .mapping(indexMappingSource, XContentType.YAML)
                .settings(indexSettingsSource, XContentType.YAML)
            try {
                val actionFuture = client.admin().indices().create(request)
                val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
                if (response.isAcknowledged) {
                    log.info("$LOG_PREFIX:Index $INDEX_NAME creation Acknowledged")
                    reindexNotebooks()
                } else {
                    error("$LOG_PREFIX:Index $INDEX_NAME creation not Acknowledged")
                }
            } catch (exception: ResourceAlreadyExistsException) {
                log.warn("message: ${exception.message}")
            } catch (exception: Exception) {
                if (exception.cause !is ResourceAlreadyExistsException) {
                    throw exception
                }
            }
            this.mappingsUpdated = true
        } else if (!this.mappingsUpdated) {
            updateMappings()
        }
    }

    /**
     * Check if the index mappings have changed and if they have, update them
     */
    private fun updateMappings() {
        val classLoader = ObservabilityIndex::class.java.classLoader
        val indexMappingSource = classLoader.getResource(OBSERVABILITY_MAPPING_FILE_NAME)?.readText()!!
        val request = PutMappingRequest(INDEX_NAME)
            .source(indexMappingSource, XContentType.YAML)
        try {
            val actionFuture = client.admin().indices().putMapping(request)
            val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
            if (response.isAcknowledged) {
                log.info("$LOG_PREFIX:Index $INDEX_NAME update mapping Acknowledged")
            } else {
                error("$LOG_PREFIX:Index $INDEX_NAME update mapping not Acknowledged")
            }
            this.mappingsUpdated = true
        } catch (exception: IndexNotFoundException) {
            log.error("$LOG_PREFIX:IndexNotFoundException:", exception)
        }
    }

    /**
     * Reindex .opensearch-notebooks to .opensearch-observability index
     */
    @Suppress("TooGenericExceptionCaught")
    private fun reindexNotebooks() {
        if (isIndexExists(NOTEBOOKS_INDEX_NAME)) {
            try {
                log.info("$LOG_PREFIX:Index - reindex $NOTEBOOKS_INDEX_NAME to $INDEX_NAME")
                val reindexResponse = ReindexRequestBuilder(client, ReindexAction.INSTANCE)
                    .source(NOTEBOOKS_INDEX_NAME)
                    .destination(INDEX_NAME)
                    .abortOnVersionConflict(false)
                    .refresh(true)
                    .get()
                if (reindexResponse.isTimedOut) {
                    error("$LOG_PREFIX:Index - reindex $NOTEBOOKS_INDEX_NAME timed out")
                } else if (reindexResponse.searchFailures.isNotEmpty()) {
                    error("$LOG_PREFIX:Index - reindex $NOTEBOOKS_INDEX_NAME failed with searchFailures")
                } else if (reindexResponse.bulkFailures.isNotEmpty()) {
                    error("$LOG_PREFIX:Index - reindex $NOTEBOOKS_INDEX_NAME failed with bulkFailures")
                } else if (reindexResponse.total != reindexResponse.created + reindexResponse.updated) {
                    throw IllegalStateException(
                        "$LOG_PREFIX:Index - reindex number of docs created:${reindexResponse.created} + " +
                            "updated:${reindexResponse.updated} does not equal requested:${reindexResponse.total}"
                    )
                }
                log.info(
                    "$LOG_PREFIX:Index - reindex ${reindexResponse.created} docs created " +
                        "and ${reindexResponse.updated} docs updated in $INDEX_NAME"
                )
            } catch (exception: ResourceNotFoundException) {
                log.warn("message: ${exception.message}")
            } catch (exception: Exception) {
                if (exception.cause !is ResourceNotFoundException) {
                    throw exception
                }
            }
        }
    }

    /**
     * Check if the index is created and available.
     * @param index
     * @return true if index is available, false otherwise
     */
    private fun isIndexExists(index: String): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(index)
    }

    /**
     * Create observability object
     *
     * @param observabilityObjectDoc
     * @param id
     * @return object id if successful, otherwise null
     */
    fun createObservabilityObject(observabilityObjectDoc: ObservabilityObjectDoc, id: String? = null): String? {
        createIndex()
        val xContent = observabilityObjectDoc.toXContent()
        val indexRequest = IndexRequest(INDEX_NAME)
            .source(xContent)
            .create(true)
        if (id != null) {
            indexRequest.id(id)
        }
        val actionFuture = client.index(indexRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        return if (response.result != DocWriteResponse.Result.CREATED) {
            log.warn("$LOG_PREFIX:createObservabilityObject - response:$response")
            null
        } else {
            response.id
        }
    }

    /**
     * Get observability object
     *
     * @param id
     * @return [ObservabilityObjectDocInfo]
     */
    fun getObservabilityObject(id: String): ObservabilityObjectDocInfo? {
        createIndex()
        val getRequest = GetRequest(INDEX_NAME).id(id)
        val actionFuture = client.get(getRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        return parseObservabilityObjectDoc(id, response)
    }

    /**
     * Get multiple observability objects
     *
     * @param ids
     * @return list of [ObservabilityObjectDocInfo]
     */
    fun getObservabilityObjects(ids: Set<String>): List<ObservabilityObjectDocInfo> {
        createIndex()
        val getRequest = MultiGetRequest()
        ids.forEach { getRequest.add(INDEX_NAME, it) }
        val actionFuture = client.multiGet(getRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        return response.responses.mapNotNull { parseObservabilityObjectDoc(it.id, it.response) }
    }

    /**
     * Parse observability object doc
     *
     * @param id
     * @param response
     * @return [ObservabilityObjectDocInfo]
     */
    private fun parseObservabilityObjectDoc(id: String, response: GetResponse): ObservabilityObjectDocInfo? {
        return if (response.sourceAsString == null) {
            log.warn("$LOG_PREFIX:getObservabilityObject - $id not found; response:$response")
            null
        } else {
            val parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsString
            )
            parser.nextToken()
            val doc = ObservabilityObjectDoc.parse(parser, id)
            ObservabilityObjectDocInfo(id, response.version, response.seqNo, response.primaryTerm, doc)
        }
    }

    /**
     * Get all observability objects
     *
     * @param tenant
     * @param access
     * @param request
     * @return [ObservabilityObjectSearchResult]
     */
    fun getAllObservabilityObjects(
        tenant: String,
        access: List<String>,
        request: GetObservabilityObjectRequest
    ): ObservabilityObjectSearchResult {
        createIndex()
        val queryHelper = ObservabilityQueryHelper(request.types)
        val sourceBuilder = SearchSourceBuilder()
            .timeout(TimeValue(PluginSettings.operationTimeoutMs, TimeUnit.MILLISECONDS))
            .size(request.maxItems)
            .from(request.fromIndex)
        queryHelper.addSortField(sourceBuilder, request.sortField, request.sortOrder)

        val query = QueryBuilders.boolQuery()
        query.filter(QueryBuilders.termsQuery(TENANT_FIELD, tenant))
        if (access.isNotEmpty()) {
            query.filter(QueryBuilders.termsQuery(ACCESS_LIST_FIELD, access))
        }
        queryHelper.addTypeFilters(query)
        queryHelper.addQueryFilters(query, request.filterParams)
        sourceBuilder.query(query)
        val searchRequest = SearchRequest()
            .indices(INDEX_NAME)
            .source(sourceBuilder)
            .preference(SEARCH_ACTION_PREFERENCE)
        val actionFuture = client.search(searchRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        val result = ObservabilityObjectSearchResult(request.fromIndex.toLong(), response, searchHitParser)
        log.info(
            "$LOG_PREFIX:getAllObservabilityObjects types:${request.types} from:${request.fromIndex}, maxItems:${request.maxItems}," +
                " sortField:${request.sortField}, sortOrder=${request.sortOrder}, filters=${request.filterParams}" +
                " retCount:${result.objectList.size}, totalCount:${result.totalHits}"
        )
        return result
    }

    /**
     * Update observability object
     *
     * @param id
     * @param observabilityObjectDoc
     * @return true if successful, otherwise false
     */
    fun updateObservabilityObject(id: String, observabilityObjectDoc: ObservabilityObjectDoc): Boolean {
        createIndex()
        val updateRequest = UpdateRequest()
            .index(INDEX_NAME)
            .id(id)
            .doc(observabilityObjectDoc.toXContent())
            .fetchSource(true)
        val actionFuture = client.update(updateRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        if (response.result != DocWriteResponse.Result.UPDATED) {
            log.warn("$LOG_PREFIX:updateObservabilityObject failed for $id; response:$response")
        }
        return response.result == DocWriteResponse.Result.UPDATED
    }

    /**
     * Delete observability object
     *
     * @param id
     * @return true if successful, otherwise false
     */
    fun deleteObservabilityObject(id: String): Boolean {
        createIndex()
        val deleteRequest = DeleteRequest()
            .index(INDEX_NAME)
            .id(id)
        val actionFuture = client.delete(deleteRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        if (response.result != DocWriteResponse.Result.DELETED) {
            log.warn("$LOG_PREFIX:deleteObservabilityObject failed for $id; response:$response")
        }
        return response.result == DocWriteResponse.Result.DELETED
    }

    /**
     * Delete multiple observability objects
     *
     * @param ids
     * @return map of id to delete status
     */
    fun deleteObservabilityObjects(ids: Set<String>): Map<String, RestStatus> {
        createIndex()
        val bulkRequest = BulkRequest()
        ids.forEach {
            val deleteRequest = DeleteRequest()
                .index(INDEX_NAME)
                .id(it)
            bulkRequest.add(deleteRequest)
        }
        val actionFuture = client.bulk(bulkRequest)
        val response = actionFuture.actionGet(PluginSettings.operationTimeoutMs)
        val mutableMap = mutableMapOf<String, RestStatus>()
        response.forEach {
            mutableMap[it.id] = it.status()
            if (it.isFailed) {
                log.warn("$LOG_PREFIX:deleteObservabilityObjects failed for ${it.id}; response:${it.failureMessage}")
            }
        }
        return mutableMap
    }
}
