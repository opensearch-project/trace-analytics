/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { EuiBreadcrumb, EuiTitle } from '@elastic/eui';
import React from 'react';
import { AppAnalyticsComponentDeps } from '../../../../components/application_analytics/home';
import { DashboardContent } from './dashboard_content';

export interface DashboardProps extends AppAnalyticsComponentDeps {
  childBreadcrumbs: EuiBreadcrumb[];
  page: 'dashboard' | 'traces' | 'services' | 'app';
}

export function Dashboard(props: DashboardProps) {
  return (
    <>
      <EuiTitle size="l">
        <h2 style={{ fontWeight: 430 }}>Dashboard</h2>
      </EuiTitle>
      <DashboardContent {...props} />
    </>
  );
}
