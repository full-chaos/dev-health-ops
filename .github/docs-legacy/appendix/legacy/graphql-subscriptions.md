# GraphQL Subscriptions

Real-time data updates via GraphQL subscriptions.

## Overview

Subscriptions provide real-time notifications when data changes, eliminating the need for polling. They use WebSocket connections with the `graphql-ws` protocol.

## Connection

Connect to the GraphQL endpoint via WebSocket:

```
ws://localhost:8000/graphql
wss://your-domain.com/graphql
```

The connection uses the `graphql-ws` subprotocol.

## Available Subscriptions

### Metrics Updated

Get notified when daily metrics are computed:

```graphql
subscription MetricsUpdated($orgId: String!) {
  metricsUpdated(orgId: $orgId) {
    orgId
    day
    updatedAt
    message
  }
}
```

**Use case:** Refresh dashboard when new metrics are available.

### Task Status

Monitor background task progress:

```graphql
subscription TaskStatus($taskId: String!) {
  taskStatus(taskId: $taskId) {
    taskId
    status
    progress
    message
    result
    updatedAt
  }
}
```

**Status values:**
- `pending` - Task queued
- `running` - Task in progress
- `completed` - Task finished successfully
- `failed` - Task failed

**Use case:** Show progress bar for long-running operations.

### Sync Progress

Track data synchronization from external providers:

```graphql
subscription SyncProgress($orgId: String!) {
  syncProgress(orgId: $orgId) {
    orgId
    provider
    status
    itemsProcessed
    itemsTotal
    message
    updatedAt
  }
}
```

**Provider values:** `github`, `gitlab`, `jira`

**Status values:**
- `starting` - Sync beginning
- `syncing` - Sync in progress
- `completed` - Sync finished
- `failed` - Sync failed

## Client Implementation

### Using urql (React)

```typescript
import { useSubscription } from 'urql';

const METRICS_SUBSCRIPTION = `
  subscription MetricsUpdated($orgId: String!) {
    metricsUpdated(orgId: $orgId) {
      orgId
      day
      updatedAt
      message
    }
  }
`;

function Dashboard({ orgId }) {
  const [result] = useSubscription({
    query: METRICS_SUBSCRIPTION,
    variables: { orgId },
  });

  useEffect(() => {
    if (result.data?.metricsUpdated) {
      // Refresh data when metrics update
      refetchDashboard();
    }
  }, [result.data]);

  return <div>...</div>;
}
```

### Using JavaScript WebSocket

```javascript
import { createClient } from 'graphql-ws';

const client = createClient({
  url: 'ws://localhost:8000/graphql',
});

// Subscribe to metrics updates
const unsubscribe = client.subscribe(
  {
    query: `
      subscription MetricsUpdated($orgId: String!) {
        metricsUpdated(orgId: $orgId) {
          day
          message
        }
      }
    `,
    variables: { orgId: 'my-org' },
  },
  {
    next: (data) => console.log('Update:', data),
    error: (err) => console.error('Error:', err),
    complete: () => console.log('Subscription complete'),
  }
);

// Later: unsubscribe()
```

## Publishing Events

Backend code can publish subscription events:

```python
from api.graphql.subscriptions import (
    publish_metrics_update,
    publish_task_status,
    publish_sync_progress,
)

# After computing metrics
await publish_metrics_update(
    org_id="my-org",
    day="2024-01-15",
    message="Daily metrics computed",
)

# During task execution
await publish_task_status(
    task_id="abc123",
    status="running",
    progress=50.0,
    message="Processing files...",
)

# During data sync
await publish_sync_progress(
    org_id="my-org",
    provider="github",
    status="syncing",
    items_processed=150,
    items_total=500,
)
```

## PubSub Backend

Subscriptions use Redis PubSub for distributed deployments:

- **With Redis:** Events broadcast to all API instances
- **Without Redis:** In-memory channels (single instance only)

Configure via `REDIS_URL` environment variable.

## Best Practices

1. **Reconnection:** Implement automatic reconnection for dropped WebSocket connections
2. **Heartbeats:** The server sends periodic heartbeats to keep connections alive
3. **Unsubscribe:** Always unsubscribe when components unmount
4. **Error handling:** Handle subscription errors gracefully
5. **Debouncing:** Debounce UI updates if events arrive rapidly
