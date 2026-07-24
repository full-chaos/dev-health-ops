# GraphQL Client Guide

This guide covers using the GraphQL client in dev-health-web.

## Overview

The frontend uses [urql](https://formidable.com/open-source/urql/) for GraphQL operations with:

- **Normalized caching** - Automatic cache updates and deduplication
- **React hooks** - Easy data fetching in components
- **Subscriptions** - Real-time updates via WebSocket
- **Zod validation** - Runtime type checking of responses

## Setup

### Provider

Wrap your app or page with the GraphQL provider:

```tsx
import { GraphQLProvider } from '@/lib/graphql/provider';

export default function Layout({ children }) {
  return (
    <GraphQLProvider orgId="my-org">
      {children}
    </GraphQLProvider>
  );
}
```

### Environment

GraphQL is enabled by default. To disable:

```bash
NEXT_PUBLIC_USE_GRAPHQL_ANALYTICS=false
```

## Hooks

### useAnalytics

Fetch analytics data (breakdowns, timeseries, sankey):

```tsx
import { useAnalytics } from '@/lib/graphql/hooks';

function InvestmentChart() {
  const { data, loading, error } = useAnalytics({
    orgId: 'my-org',
    batch: {
      breakdowns: [
        {
          dimension: 'THEME',
          measure: 'COUNT',
          dateRange: { startDate: '2024-01-01', endDate: '2024-01-31' },
          topN: 10,
        },
      ],
      timeseries: [],
    },
  });

  if (loading) return <Spinner />;
  if (error) return <Error message={error.message} />;

  return (
    <Chart data={data.breakdowns[0].items} />
  );
}
```

### useBreakdown

Simplified hook for single breakdown queries:

```tsx
import { useBreakdown } from '@/lib/graphql/hooks';

function TeamBreakdown() {
  const { data, loading } = useBreakdown({
    orgId: 'my-org',
    dimension: 'TEAM',
    measure: 'THROUGHPUT',
    startDate: '2024-01-01',
    endDate: '2024-01-31',
    topN: 10,
  });

  // ...
}
```

### useSankey

Fetch Sankey flow data:

```tsx
import { useSankey } from '@/lib/graphql/hooks';

function FlowChart() {
  const { data, loading } = useSankey({
    orgId: 'my-org',
    path: ['THEME', 'TEAM'],
    measure: 'COUNT',
    startDate: '2024-01-01',
    endDate: '2024-01-31',
    maxNodes: 50,
    maxEdges: 200,
  });

  // data.sankey.nodes, data.sankey.edges
}
```

### useCatalog

Fetch catalog metadata:

```tsx
import { useCatalog } from '@/lib/graphql/hooks';

function FilterBar() {
  const { data, loading } = useCatalog({
    orgId: 'my-org',
  });

  // data.dimensions, data.measures, data.limits
}
```

### useDimensionValues

Fetch distinct values for a dimension:

```tsx
import { useDimensionValues } from '@/lib/graphql/hooks';

function TeamSelect() {
  const { values, loading } = useDimensionValues({
    orgId: 'my-org',
    dimension: 'TEAM',
  });

  return (
    <Select>
      {values.map((v) => (
        <Option key={v.value} value={v.value}>
          {v.value} ({v.count})
        </Option>
      ))}
    </Select>
  );
}
```

## Subscriptions

### useMetricsUpdated

Subscribe to metrics updates:

```tsx
import { useMetricsUpdated } from '@/lib/graphql/hooks';

function Dashboard() {
  useMetricsUpdated({
    orgId: 'my-org',
    onUpdate: (update) => {
      console.log(`Metrics updated for ${update.day}`);
      refetchData();
    },
  });

  // ...
}
```

### useTaskStatus

Monitor background task progress:

```tsx
import { useTaskStatus } from '@/lib/graphql/hooks';

function TaskProgress({ taskId }) {
  const { data } = useTaskStatus({
    taskId,
    onUpdate: (status) => {
      if (status.status === 'completed') {
        showSuccess('Task complete!');
      }
    },
  });

  return (
    <ProgressBar value={data?.progress ?? 0} />
  );
}
```

## Validation

Use Zod schemas to validate responses:

```tsx
import { validateAnalyticsResponse } from '@/lib/graphql/validate';
import { AnalyticsResultSchema } from '@/lib/graphql/schemas';

// Validate raw data
const result = validateAnalyticsResponse(rawData);
if (!result.success) {
  console.error('Validation failed:', result.error);
}

// Or validate with schema directly
import { safeParse } from '@/lib/graphql/validate';
const data = safeParse(AnalyticsResultSchema, rawData);
```

## Direct Client Usage

For non-React contexts or custom logic:

```tsx
import { getUrqlClient } from '@/lib/graphql/urqlClient';
import { INVESTMENT_BREAKDOWN_QUERY } from '@/lib/graphql/queries';

async function fetchData() {
  const client = getUrqlClient('my-org');

  const result = await client.query(INVESTMENT_BREAKDOWN_QUERY, {
    orgId: 'my-org',
    batch: { ... },
  });

  return result.data;
}
```

## Error Handling

```tsx
function MyComponent() {
  const { data, loading, error } = useAnalytics({ ... });

  if (error) {
    // Check for specific error types
    if (error.message.includes('COST_LIMIT_EXCEEDED')) {
      return <Error>Query too expensive. Try reducing the date range.</Error>;
    }
    return <Error>Something went wrong: {error.message}</Error>;
  }

  // ...
}
```

## Best Practices

1. **Use hooks at component level** - Let urql handle caching and deduplication
2. **Validate responses** - Use Zod schemas in development to catch API changes
3. **Handle loading states** - Always show feedback during data fetching
4. **Unsubscribe on unmount** - Hooks handle this automatically
5. **Prefer persisted queries** - For production, use server-registered queries
