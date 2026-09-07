# CHAOS-5349 wire-form fixtures (capacity / throughput forecast)

`capacityForecast.graphql`, `capacityForecasts.graphql` and
`throughputForecast.graphql` are the exact document text
`query_route.go`'s `registeredCapacityForecastDocument`,
`registeredCapacityForecastsDocument` and
`registeredThroughputForecastDocument` must digest to for a real client's
request to reach this route at all.

## Provenance — and how this differs from `../wire_capture/`

These were produced by the WEB REPO'S OWN wire-parity tooling,
`web/scripts/graphql-wire-parity.ts`'s `wireForm()`, which is
`createRequest` → `formatDocument` (urql's `__typename` injection) →
`stringifyDocument` (urql's `print()`). That is the same function the web
repo's CI check (`pnpm graphql:wire-parity:check`, wired into
`web/ci/run_tests.sh`) compares this Go registry against, so it is the
authority the two repos have actually agreed on.

It is NOT an HTTP capture, and that distinction is deliberate rather than
a shortcut taken quietly. `../wire_capture/featureflags_captured.graphql`
is a capture off a REAL `fetch()` — CHAOS-4696's evidence bar — and its
README explains why: at the time, nothing proved that
`createRequest`/`stringifyDocument` composed the way a real request does.
That proof now exists and is checked in next door. These three fixtures
rely on it rather than re-establishing it three more times; if the
featureFlags capture ever stops matching `wireForm()`, that test fails
first and these three are known to be suspect by the same finding.

What these fixtures DO buy, which the digests alone would not: the
document text is readable in the tree, so a reviewer can see the
`__typename` selections and confirm the const was not hand-copied from
`web/src/lib/graphql/queries.ts` — which is the mistake CHAOS-4696 was.

## Digests

| operation | sha256 of the trimmed wire text |
| --- | --- |
| `capacityForecast` | `b4fb8f075aba9954714f10f7f4451242d969548d6392d778dae1177759f80780` |
| `capacityForecasts` | `43890adbbf75ac3ad71c29f728a82e35d035c3f2c701f0ced116559fa28b6c20` |
| `throughputForecast` | `fc08dea094ec832290b4f528141eacb5bbc463beea45794cf205fe43952379f9` |

These are cross-checked twice, by two paths that do not share code:
`registered_forecast_documents_test.go` digests the Go consts against
these files, and `src/dev_health_ops/api/graphql/go_api_operations.json`
carries the same three values, produced independently by
`cmd/query-api/tools/registrydump` parsing `query_route.go`'s AST.

## Refreshing

Change the query text in `web/src/lib/graphql/queries.ts`, then re-run the
web repo's `pnpm graphql:wire-parity:generate --ops-root <ops>` and paste
the new text into both the Go const and the file here. Regenerating one
without the other is what
`registered_forecast_documents_test.go` exists to catch.
