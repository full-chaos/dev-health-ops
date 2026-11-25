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
| `capacityForecast` | `61534928326d9e7c1ad83451056cec1d73b66e89a5eca9c6170d60ecf8bbdeb6` |
| `capacityForecast` (legacy V3, `capacityForecast.v3.graphql`, dual accept CHAOS-7508) | `d95197012e32056fcece1f8975d953e7465bb112354cf4201e87b39375f933ef` |
| `capacityForecast` (legacy V2, `capacityForecast.v2.graphql`, dual accept CHAOS-8477) | `7ec8e919d784ed0dea1b87e85d64ffe18d05557379a397d07da0ab958694ffd1` |
| `capacityForecast` (legacy V1, `capacityForecast.v1.graphql`, dual accept CHAOS-7994) | `b4fb8f075aba9954714f10f7f4451242d969548d6392d778dae1177759f80780` |
| `capacityForecasts` | `976b1a117d82f699eef98bcee1a1fc5cec44086026706fd501eddc4f2ba6fd77` |
| `capacityForecasts` (legacy V1, `capacityForecasts.v1.graphql`, dual accept CHAOS-7508) | `43890adbbf75ac3ad71c29f728a82e35d035c3f2c701f0ced116559fa28b6c20` |
| `throughputForecast` | `9ace3bd179910a7c79ec8dade33c8f93c7244fc45fe455d4f844a5838eb6a18f` |
| `throughputForecast` (legacy V3, `throughputForecast.v3.graphql`, dual accept CHAOS-7508) | `a265a9550e4cdab2bc77395257777ec366ac554b19c617af094de47c7a9e2b03` |
| `throughputForecast` (legacy V2, `throughputForecast.v2.graphql`, dual accept CHAOS-14142) | `933efa3840bb7afb600135bc4470d6523842a908f0ffb949a35439e15a76e09d` |
| `throughputForecast` (legacy V1, `throughputForecast.v1.graphql`, dual accept CHAOS-8167) | `fc08dea094ec832290b4f528141eacb5bbc463beea45794cf205fe43952379f9` |

These are cross-checked twice, by two paths that do not share code:
`registered_forecast_documents_test.go` digests the Go consts against
these files, and `contracts/graphql/v1/go_api_operations.json`
carries these values (each legacy text as an entry with `legacy: true`), produced independently by
`cmd/registrydump` parsing `query_route.go`'s AST.

## Refreshing

Change the query text in `web/src/lib/graphql/queries.ts`, then re-run the
web repo's `pnpm graphql:wire-parity:generate --ops-root <ops>` and paste
the new text into both the Go const and the file here. Regenerating one
without the other is what
`registered_forecast_documents_test.go` exists to catch.

## hotspots (CHAOS-8488, dual accept CHAOS-8000)

`hotspots.v1.graphql` is the wire form (`wireForm()` of `web/scripts/graphql-wire-parity.ts`) of web main's
`HOTSPOTS_QUERY` before it asked for `repos`: sha256 of the trimmed text
`6ccfcc785f38dc4a2d3ef5c9bdc2a00e76c02ccf58d11fd1198d2d321165e781`, the legacy `registeredHotspotsV1Document`.

## cognitiveLoad (CHAOS-14628, dual accept)

`cognitiveLoad.v1.graphql` is the text of web main's `COGNITIVE_LOAD_QUERY` (the document acr's catalogue carries too)
before it asked for `developerState`: sha256 of the text
`dcdfb2692addf52ed6117162963c698b628af37d01d08e4a8724f90388d8427d`, the legacy `registeredCognitiveLoadV1Document`. The
current `registeredCognitiveLoadDocument` selects `developerState` after `reviewRequestLoad`
(`d16e00aeeae95af56d88173fb817892002471985a4f11ae5f760e57fab79ed9c`).
