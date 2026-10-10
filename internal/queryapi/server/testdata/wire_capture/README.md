# featureFlags wire-capture fixture (CHAOS-4696)

# Investment Evidence Quality wire-capture fixture (CHAOS-8104 / CHAOS-8745)

`investmentevidencequality_captured.graphql` is the exact named query captured
from CHAOS-8745's real browser client/cache exchange. It selects the served
`analytics.evidenceQualityByGroup` values for the Investment Evidence table.
The capture digest is
`5e31cdbd14dde65165c635a8a3f02fb03f85fae0ef9c21eeb39808c199323c2a`.

`TestRegisteredInvestmentEvidenceQualityDocument_MatchesCapturedWireFixture`
pins both that supplied client-capture digest and query-api's registered
document. The test prevents the catalog from accepting an unmeasured,
reconstructed document. The query uses the existing `analytics` root and its
existing `mcp:analytics` class; it introduces no new route or MCP class.

Captured: 2026-10-05T14:43:26Z. The client capture is the source of the wire
text; the exact route execution is covered separately by the integration test.

`featureflags_captured.graphql` is the RAW `query` text captured off a
real HTTP request, produced by this repo's own UNMODIFIED `graphqlFetch`
(`src/lib/graphql/server.ts`) calling the real `@urql/core` client's
exchange chain (`createClient` -> `timingExchange` -> `errorExchange`
-> `cacheExchange` -> `fetchExchange`) against a real local HTTP
listener, with the real `FEATURE_FLAG_REGISTRY_QUERY` export
(`src/lib/feature-flags/queries.ts`) as input variables. Nothing in this
capture path calls `createRequest`/`stringifyDocument` directly, and
nothing hand-reprints the query -- the bytes below are what a real
`fetch()` call actually put on the wire.

**Transport observed: `GET`.** This repo's client never sets
`preferGetMethod`, so `createClient`'s default (`'within-url-limit'`)
applies: a query whose fully-encoded URL fits under 2047 characters goes
out as `GET` with the query in a URL search parameter, not a POST JSON
body -- featureFlags's short variable set falls under that limit. The WIRE
FORM TEXT is byte-identical either way (both transports build it via the
same `stringifyDocument(request.query)` call inside `@urql/core` --
see `makeFetchURL`/`makeFetchBody` in
`@urql/core/dist/urql-core-chunk.js`); only the encoding differs, and
this script extracts the `query` value from whichever transport the real
client actually used. **Separately reported, out of this PR's scope:**
query-api's `/query` route currently accepts POST only
(`query_route.go`'s method check) and returns 405 for a spec-valid GET;
whatever proxies real traffic to query-api must normalize this, or GET
requests under the URL-length threshold never reach the digest check at
all.

Capture mechanism: `scripts/capture-graphql-wire-fixture.ts`. Re-run it
to refresh this fixture (e.g. after an intentional query text change).

## Digests

| digest of | value |
| --- | --- |
| `FEATURE_FLAG_REGISTRY_QUERY` (web source text, unprinted) | `555bc9f82339b8321f309a26d310c4a7e41e79b9b155da41f62d8e97b50da8b7` |
| this captured fixture (real wire bytes) | `06ca28a0517a34c0f5a6cc25b193da7b5682bea5192ae93e5a79edc7e7742208` |

These two digests are DIFFERENT for TWO reasons, not one:
1. `cacheExchange` maps every query through `formatDocument`, injecting
   a `__typename` selection into every non-root selection set --
   unconditional for any client (like this repo's) that runs
   `cacheExchange` before `fetchExchange`.
2. The source text has a 122-character single-line `featureFlags(...)`
   field argument list; urql's real `print()` (`fetchExchange`'s
   `stringifyDocument`) reflows it past 80 characters.

Both are part of CHAOS-4696's defect -- a fix that only reflowed the
argument list (skipping `__typename`) would still digest-miss a real
request; see `scripts/graphql-wire-parity.ts` in the web repo. `internal/queryapi/server/query_route.go`'s
`registeredFeatureFlagsDocument` const must digest to
`06ca28a0517a34c0f5a6cc25b193da7b5682bea5192ae93e5a79edc7e7742208`, not
`555bc9f82339b8321f309a26d310c4a7e41e79b9b155da41f62d8e97b50da8b7`, for
query-api to accept a real client's request.

Captured: 2026-09-01T01:40:06.129Z, ops tip at capture time: see the
lane's PR description for the exact SHA this was verified against.

# featureFlagEvents wire-capture fixture (CHAOS-5523)

`featureflagevents_captured.graphql` is the RAW `query` text captured off
a real HTTP request, produced the same way as `featureflags_captured.
graphql` above: this repo's own UNMODIFIED `graphqlFetch`
(`src/lib/graphql/server.ts`) calling the real `@urql/core` client's
exchange chain against a real local HTTP listener, with the real
`FEATURE_FLAG_EVENTS_QUERY` export (`src/lib/feature-flags/queries.ts`)
as input variables.

Capture mechanism: a same-shape variant of
`scripts/capture-graphql-wire-fixture.ts`, retargeted at
`FEATURE_FLAG_EVENTS_QUERY` (that script itself hardcodes the
featureFlags query only) -- run once from an unmodified `dev-health-web`
checkout with absolute imports pointing at that checkout's own
`src/lib/graphql/server` and `src/lib/feature-flags/queries`, so the
graphqlFetch code path exercised is byte-for-byte the repo's real,
unmodified one; not committed anywhere as a script (single-use, deleted
after the capture ran).

**Transport observed: `GET`** (same `within-url-limit` default as
featureFlags above; featureFlagEvents's variable set is also short
enough to stay under the 2047-character threshold).

## Digests

| digest of | value |
| --- | --- |
| `FEATURE_FLAG_EVENTS_QUERY` (web source text, unprinted) | `4a3e3f98adb538466e4a7963af6ba88bc1b1fbcc4c08c56895f838c1d00210b9` |
| this captured fixture (real wire bytes) | `e5e5fd3aba6d00c5413407046dfa68acadeb72c6ff1bb5f40b983bd0ddc3b632` |

Same CHAOS-4696-class divergence as featureFlags: `cacheExchange`'s
`formatDocument` injects `__typename` into every non-root selection set,
and `fetchExchange`'s real `print()` reflows the `featureFlagEvents(...)`
field's argument list onto its own lines. `internal/queryapi/server/query_route.go`'s
`registeredFeatureFlagEventsDocument` const must digest to
`e5e5fd3aba6d00c5413407046dfa68acadeb72c6ff1bb5f40b983bd0ddc3b632`, not
`4a3e3f98adb538466e4a7963af6ba88bc1b1fbcc4c08c56895f838c1d00210b9`, for
query-api to accept a real client's request --
`query_route_wire_capture_test.go`'s
`TestRegisteredFeatureFlagEventsDocument_MatchesCapturedWireFixture`
enforces this on every run, independent of this README's own claim.

Captured: 2026-09-09T19:34:39Z, ops tip at capture time: fe03a111bee2
(this lane's worktree base).

# pr wire-capture fixture (CHAOS-4991)

`pr_captured.graphql` is the wire-form `query` text for the `pr`
operation (`PR_DETAIL_QUERY`, `web/src/lib/graphql/queries.ts:94-138`,
operation name `PrDetail`). Unlike `featureflags_captured.graphql`
above, this fixture was NOT captured off a live HTTP listener -- it was
produced by IMPORTING the web repo's own, live, pinned
`scripts/graphql-wire-parity.ts` export `wireForm()` (the exact function
that repo's own CI wire-parity gate calls) and invoking it directly
against the real `PR_DETAIL_QUERY` export, via `tsx` so `@urql/core`
resolved from the web repo's own pinned `node_modules`:

```
tsx <script importing wireForm from web/scripts/graphql-wire-parity.ts
     and PR_DETAIL_QUERY from web/src/lib/graphql/queries.ts>
```

`wireForm()` itself calls the SAME three real, pinned functions in the
SAME order `featureflags_captured.graphql`'s live-HTTP capture exercised
(`createRequest` -> `formatDocument` -> `stringifyDocument`, see that
function's own doc comment in `graphql-wire-parity.ts`) -- the only
difference from a live-HTTP capture is that no HTTP request/listener was
actually built around it. This was the AVAILABLE, verified path in this
environment (Node + this repo's own `tsx`/`@urql/core` were present and
used directly, not reconstructed or hand-reflowed) -- not the
"hand-apply the two known transforms" fallback CHAOS-4991's brief
describes as the last resort when no tool is available.

## Digest

| digest of | value |
| --- | --- |
| this captured fixture (wireForm(PR_DETAIL_QUERY)) | `564852769ff2397df5c7c0364ee6d1cf7ef0172e62566d575c2b9d5c9e77d577` |

`internal/queryapi/server/query_route.go`'s `registeredPrDetailDocument` const must
digest to `564852769ff2397df5c7c0364ee6d1cf7ef0172e62566d575c2b9d5c9e77d577`
-- verified equal by
`query_route_wire_capture_test.go`'s
`TestRegisteredPrDetailDocument_MatchesCapturedWireFixture`.

Captured: 2026-09-09T19:41Z, ops tip at capture time:
fe03a111bee28542293b34ba8c72bb16e9579858 (this lane's worktree base).

# catalog wire-capture fixtures

`catalog_values_captured.graphql` (operation `CatalogValues`, the web
client's `CATALOG_VALUES_QUERY`) and `acr_repository_scopes_captured.graphql`
(operation `ACRRepositoryScopes`, the agent-context runtime's repository
scope read) are the wire-form query text of the two documents that select the
`catalog` root field. Each was produced by importing the web repo's own
`wireForm()` (`scripts/graphql-wire-parity.ts`) and applying it to the
document's source text, so `@urql/core` resolved from the web repo's pinned
`node_modules`.

| fixture | sha256(wire form) |
| --- | --- |
| `catalog_values_captured.graphql` | `d069072e06541e799679f355cb343a6408d660d17f1e7a33659063b423017fba` |
| `acr_repository_scopes_captured.graphql` | `1fe3d5c84c8c047b57cdaf71a71370583d615156e78dcb8991b8f954fae41773` |

`query_route_wire_capture_test.go`'s
`TestRegisteredCatalogDocuments_MatchCapturedWireFixtures` asserts each
registered const digests to its fixture.

# data-health wire-capture fixtures

`data_health_connectors_captured.graphql` (`GetConnectorsDataHealth`),
`data_health_identity_captured.graphql` (`DataHealthIdentity`),
`data_health_metric_lineage_captured.graphql` (`MetricLineage`) and
`data_health_mapping_coverage_captured.graphql` (`GetMappingCoverageHealth`)
are the wire-form text of the four documents that select `dataHealth`. Each was
produced by importing the web repo's own `wireForm()`
(`scripts/graphql-wire-parity.ts`) and applying it to the `toString()` of the
generated document the page sends. The literal `team: "ALL"` argument of the
lineage document prints across two lines; that is what the pinned printer
emits, and the fixture keeps it byte for byte.

| fixture | sha256(wire form) |
| --- | --- |
| `data_health_connectors_captured.graphql` | `0e43d67d571539081b2006ae8b2148dd4ea31c959be22be1db9e927083060baa` |
| `data_health_identity_captured.graphql` | `a31987266aad8c1e9eb7cb1b572cc45aecc417f566773e9b5ba6e1bf10ec1a94` |
| `data_health_metric_lineage_captured.graphql` | `4d93b59c335d6b8e5709f60bf4186030aa1b6b454c438e73c9177c675ccd705d` |
| `data_health_mapping_coverage_captured.graphql` | `f87ebae2df502280d389cfd3bf6452e2e1ef8ed6965280f5ba809350ac6a0d01` |

# experiments wire-capture fixture

`experiments_captured.graphql` (`Experiments`) is the wire-form text of the
document that selects `experiments`, produced by importing the web repo's own
`wireForm()` (`scripts/graphql-wire-parity.ts`) and applying it to
`EXPERIMENTS_QUERY`.

| fixture | sha256(wire form) |
| --- | --- |
| `experiments_captured.graphql` | `133cf72191722912e8833ee46a1670c89d74c06dc9a174bfefb7c137fd74bfe5` |

# product telemetry wire-capture fixtures

`product_telemetry_dashboard_captured.graphql` (`ProductTelemetryDashboard`) and
`product_telemetry_platform_dashboard_captured.graphql`
(`ProductTelemetryPlatformDashboard`) are the wire-form text of the two
documents that select the product telemetry dashboards, produced by importing
the web repo's own `wireForm()` (`scripts/graphql-wire-parity.ts`) and applying
it to the query constants in `src/lib/graphql/productTelemetryFetchers.ts`.

| fixture | sha256(wire form) |
| --- | --- |
| `product_telemetry_dashboard_captured.graphql` | `3bcefc8f84200705229f8195e785682bf9adbfc01e60c4e775d0ff9d36954707` |
| `product_telemetry_platform_dashboard_captured.graphql` | `8e2847048e3b70b0391f45d9cd8804735ea4401b7e78cd614e35e49ca39e348f` |

# saved-report wire-capture fixtures

`saved_reports_captured.graphql` (`savedReports`), `saved_report_captured.graphql`
(`savedReport`) and `report_runs_captured.graphql` (`reportRuns`) are the
wire-form text of the three web documents that read saved reports and their
runs (`SAVED_REPORTS_QUERY`, `SAVED_REPORT_QUERY`, `REPORT_RUNS_QUERY` in the
web repo's `src/lib/reports/queries.ts`). Each was produced by importing the
web repo's own `wireForm()` (`scripts/graphql-wire-parity.ts`) and applying it
to the query source text.

| fixture | sha256(wire form) |
| --- | --- |
| `saved_reports_captured.graphql` | `095ac91596b0baea1b8100074d7f8c0beb39291c999274d1e978444e8d25f6a5` |
| `saved_report_captured.graphql` | `02fc81f826285965c94a564e490d1dec4e4831079bb3788ff7f742dd06aa6a22` |
| `report_runs_captured.graphql` | `16fbdefaa0f4f095d43b934b5fefb3eaed5878aeb20685303aba6f3a67c4a8de` |

# home wire-capture fixture

Newest text (CHAOS-9098): `home_captured.graphql` is NOT a capture. It is
`home_v9_captured.graphql` (the previous current text, byte-identical, kept as a
legacy registration) plus one line, `filterEmptyReason`, in `home` after
`scopeDataConfidence`, written in the form urql prints. The web change that selects it
must put the field at that place in `HOME_QUERY` and run
`scripts/capture-graphql-wire-fixture.ts --operation home`; if the real capture
differs, the capture wins and this file and `registeredHomeDocument` move to it.

Newest text (CHAOS-9094): `home_captured.graphql` is NOT a capture. It is
`home_v8_captured.graphql` (the previous current text, byte-identical, kept as a
legacy registration) plus the selections `repoLinkState`, `repoLinkBasis { native
explicitText heuristic }`, `repoLinkMultiRepoItems` and `repoLinkCoverage
{ linkedItems itemsInWindow }` in `deltas` after `repoFilterApplied`, in the form
urql prints. The web change that selects them must put the fields at those places in
`HOME_QUERY` and run `scripts/capture-graphql-wire-fixture.ts --operation home`; if the
real capture differs, the capture wins and this file and `registeredHomeDocument` move to it.

Newest text (CHAOS-9093): `home_captured.graphql` is NOT a capture. It is
`home_v7_captured.graphql` (the previous current text, byte-identical, kept as a
legacy registration) plus two lines, `repoFilterApplied` in `deltas` after
`rateCoverage` and in `signals` after `coverage`, written in the form urql prints.
The web change that selects them must put the fields at those places in `HOME_QUERY` and
run `scripts/capture-graphql-wire-fixture.ts --operation home`; if the real capture
differs, the capture wins and this file and `registeredHomeDocument` move to it.

Newest text (CHAOS-6545 after CHAOS-9072): `home_captured.graphql` is NOT a capture. It is
`home_v6_captured.graphql` (main's previous current text, byte-identical, kept as a legacy
registration) plus one line, `coverage`, in `signals` before `attribution`, in the form urql
prints. The web change that selects it must put the field at that place in `HOME_QUERY` and
run `scripts/capture-graphql-wire-fixture.ts --operation home`; if the real capture differs,
the capture wins and this file and `registeredHomeDocument` move to it.

`home_captured.graphql` (`Home`) is the current registered text. It is
NOT a capture: it is `home_v4_captured.graphql` plus two lines in
`deltas`, after `spark { ... }`: `rateState` (CHAOS-8981, the state of a
rate that is a ratio of stored counts) and, after it, `rateCoverage`
(CHAOS-9072, the coverage of the pull request rework ratio), written in
the form urql prints. No web build sends it yet. The web change that
selects the two fields must put them at that place, in that order, in
`HOME_QUERY` and run
`scripts/capture-graphql-wire-fixture.ts --operation home`; if the real
capture differs from this file, the capture wins and this file and
`registeredHomeDocument` move to it.

`home_v5_captured.graphql` is the text that was current before
CHAOS-9072: `home_v4_captured.graphql` plus the one line `rateState`.
It is no capture either. It was a registered text, so a client can have
been built against it, and it stays a legacy text.

`home_v4_captured.graphql` is the last real capture and the text every
current web build sends: captured on 2026-10-04 from web commit
`9e4315766c6a0345a9bd84d5a5038a6f1b4da52d` with the normal `graphqlFetch`
path, using `scripts/capture-graphql-wire-fixture.ts --operation home`
(the real `HOME_QUERY` through the same pinned urql client path that
production uses). CHAOS-8981 keeps it as a legacy text.

`home_v3_captured.graphql` is the real captured Home document before
CHAOS-8102 added `HomeSignal.attribution`.
`home_v2_captured.graphql` is the earlier text before CHAOS-8107 added
`scopeDataConfidence`. `home_v1_captured.graphql` is the text before
CHAOS-8169 added `deltas.hasData` and `deltas.hasPriorData`. The route maps
all six digests to the `home` operation. Remove a legacy registration only
with its cleanup ticket after no supported web build sends it.

`query_route_wire_capture_test.go` verifies each registered text against
its file's bytes. `query_route_home_no_data_test.go` verifies the
current and five legacy documents’ selections and that all resolve to `home`.

| fixture | sha256(wire form) |
| --- | --- |
| `home_captured.graphql` | `c63a70236aa38c12f3cb4f062bf549af507e264d93268b74fe951f8b627270fb` |
| `home_v5_captured.graphql` | `5351e92b61599543ce5913591687ac01f642e27b6ab01577357e442b15e27dce` |
| `home_v4_captured.graphql` | `f920722d8e56ae44b85ce535ab0364ab10eb6baf70da1b49cc60a1f915abe7a1` |
| `home_v3_captured.graphql` | `cff105a9f8c5d5f2363f3d50510db34081c84c9c652afe380975b49c15a4c6eb` |
| `home_v2_captured.graphql` | `c02bb493d709b8c445e2ac711bdf93d6a2cdb7933d1073a20bf7dad3ffd06545` |
| `home_v1_captured.graphql` | `9776798e809030868e3a7fc8643b06d122573f03a3c48a7710d86b1842b33554` |

For contrast, `sha256(HOME_QUERY.trim())` on the raw, unprinted source
text of that web commit is `bcba9a4e032d9664672cc828ac4cfe9406807139b13d2f66fe04b3ee3787dc09`.
It differs from every wire-form digest because the source text has
no injected `__typename`. The negative control in
`TestRegisteredHomeDocument_MatchesCapturedWireFixture` pins that
difference.

# Home wire fixture, signal coverage

`home_captured.graphql` is the current registered text: it is
`home_v5_captured.graphql` plus the one line `coverage` in `signals`, after
`scopeEntity` (CHAOS-6545: the share of a compounding-risk score's weight that
was present). `home_v5_captured.graphql` is the text a web build sends until it
selects `coverage`; it stays accepted as a legacy text of `home`
(`legacyDigestsByOperation`) beside V1 to V4.

# compoundingRisk wire fixture

`compoundingrisk_captured.graphql` is the current registered text. It is NOT
a capture: it is `compoundingrisk_v1_captured.graphql` plus the one line
`coverage` in `rows`, after `score` (CHAOS-6545: the share of the score's
weight that was present). `compoundingrisk_v1_captured.graphql` (digest
`2b50958a4670a434bbfe46be7344486488d3a278e62b09b257252441e962bb67`) is the text a
web build sends until it selects `coverage`; it stays accepted as the
operation's legacy text (`legacyDigestsByOperation`). Remove it with the
cleanup ticket once no client sends it.

# operatingReview wire fixture

`operatingreview_captured.graphql` is the current registered text. Like
the Home file it is NOT a capture: it is
`operatingreview_v3_captured.graphql` plus the one line `rateState` in
`metrics`, after `scope` (CHAOS-8981). `operatingreview_v3_captured.graphql`
(digest `fef895a373f536846afdb3bb93ef12dd03d8af8d23f3663b290c350e622ed8c3`)
is the text every current web build sends. The web change that selects
`rateState` must put the field at that place in `OPERATING_REVIEW_QUERY`,
and its real wire text wins over this file if they differ. The current
digest is `54a442197685a26436f6287e8377a26b6451de33e251e857afa30abe32df9ac9`.
