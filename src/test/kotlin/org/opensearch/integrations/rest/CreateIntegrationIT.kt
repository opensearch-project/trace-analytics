package org.opensearch.integrations.rest

import org.opensearch.integrations.model.Integration
import org.opensearch.observability.ObservabilityPlugin.Companion.BASE_INTEGRATIONS_URI
import org.opensearch.observability.PluginRestTestCase
import org.opensearch.observability.getJsonString
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus

class CreateIntegrationIT : PluginRestTestCase() {
    private val sampleIntegration = Integration(
        "sample_integration",
        "This is a sample integration"
    )

    fun `test create integration`() {
        val requestBody = getJsonString(sampleIntegration)
        val createResponse = executeRequest(
            RestRequest.Method.POST.name,
            "$BASE_INTEGRATIONS_URI/store",
            requestBody,
            RestStatus.CREATED.status
        )
        assertEquals(sampleIntegration.id, createResponse.get("id").asString)
    }
}
