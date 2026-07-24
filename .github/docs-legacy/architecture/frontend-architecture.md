# Frontend Architecture (dev-health-web)

## Technology Stack

- **Framework:** Next.js (App Router)
- **Language:** TypeScript
- **Styling:** Tailwind CSS
- **Testing:** Vitest (unit), Playwright (e2e)
- **Data:** GraphQL client to dev-health-ops API

---

## Directory Structure

```
src/
├── app/           # Next.js pages and routes
├── components/    # Reusable UI components
│   ├── charts/    # Visualization components
│   ├── filters/   # Filter controls
│   └── navigation/# Nav components
├── lib/           # Data transforms, mappers, helpers
├── data/          # Sample data for demos/tests
└── types/         # TypeScript type definitions

tests/             # Playwright e2e tests
test-results/      # Test artifacts
```

---

## Key Architectural Patterns

### 1. Server vs Client Components

- **Server Components:** Default for pages, data fetching colocated with page
- **Client Components:** Interactive elements, charts, filters
- Mark client components with `'use client'` directive

### 2. Data Flow

```
GraphQL API → src/lib transforms → Chart components → Rendered UI
```

- Sample data in `src/data/` for demos and unit tests
- Real data from dev-health-ops GraphQL API
- Test mode uses `DEV_HEALTH_TEST_MODE` env var

### 3. Type Safety

- All components use TypeScript
- Shared types in `src/types/`
- Filter types must use union types (e.g., `"repo" | "org" | "team"`)

---

## Development Conventions

### Component Guidelines

```typescript
// Good: Typed props, clear interface
interface MetricCardProps {
  metric: Metric;
  scope: MetricFilter;
  onDrillDown?: (id: string) => void;
}

export function MetricCard({ metric, scope, onDrillDown }: MetricCardProps) {
  // Implementation
}
```

### State Management

- Avoid synchronous `setState` in effects
- Derive sample data via memo + computed loading
- Use React Server Components for data fetching when possible

### ESLint Rules

Active rules to follow:
- `react-hooks/set-state-in-effect` — No sync setState in effects
- `react-hooks/exhaustive-deps` — Include all dependencies

---

## Testing Strategy

### Unit Tests (Vitest)

- Located next to modules: `src/lib/__tests__/`
- Test transforms, mappers, helpers independently

```typescript
// src/lib/__tests__/transforms.test.ts
import { transformMetrics } from '../transforms';

describe('transformMetrics', () => {
  it('converts hours to days', () => {
    expect(transformMetrics({ hours: 24 })).toEqual({ days: 1 });
  });
});
```

### E2E Tests (Playwright)

- Located in `tests/` directory
- Use test mode environment variables
- Include visual regression when possible

```typescript
// tests/dashboard.spec.ts
test('dashboard loads metrics', async ({ page }) => {
  await page.goto('/dashboard');
  await expect(page.getByTestId('metric-card')).toBeVisible();
});
```

### Test Environment

Playwright config sets these env vars:
- `DEV_HEALTH_TEST_MODE=1`
- `NEXT_PUBLIC_DEV_HEALTH_TEST_MODE=1`

Components should support sample data when these are set.

---

## GraphQL Client

### Setup

```typescript
// src/lib/graphql-client.ts
import { createClient } from '@urql/core';

export const client = createClient({
  url: process.env.NEXT_PUBLIC_GRAPHQL_URL || 'http://localhost:8000/graphql',
});
```

### Query Patterns

```typescript
// Fetch with typing
const result = await client.query<MetricsQuery>(METRICS_QUERY, {
  scope: { type: 'team', id: teamId },
  range: { days: 14 },
});
```

See `docs/graphql-client.md` for full client documentation.

---

## Visualization Integration

### Chart Components Location

- `src/components/charts/` — All chart implementations
- Use visualization patterns from `docs/visualizations.md`

### Data Transform Pattern

```typescript
// src/lib/transforms/metric-transforms.ts
export function toChartData(metrics: Metric[]): ChartDataPoint[] {
  return metrics.map(m => ({
    x: m.day,
    y: m.value,
    label: m.label,
  }));
}
```

---

## PR & Review Guidelines

- Use descriptive PR titles referencing related tests
- Keep changes scoped to one feature or bugfix
- Include screenshot or Playwright trace for visual changes
- Run `npm run lint` and `npm run test` before submitting

---

## Common Gotchas

### 1. Filter Type Unions

```typescript
// Wrong: string type loses type safety
const scope: { level: string } = { level: 'team' };

// Correct: Use union type
const scope: { level: 'repo' | 'org' | 'team' } = { level: 'team' };
```

### 2. Test Mode Detection

```typescript
// Check for test mode in components
const isTestMode = process.env.NEXT_PUBLIC_DEV_HEALTH_TEST_MODE === '1';
const data = isTestMode ? sampleData : await fetchRealData();
```

### 3. Hydration Mismatches

- Server and client must render same initial content
- Use `useEffect` for client-only logic
- Check `typeof window !== 'undefined'` when needed
