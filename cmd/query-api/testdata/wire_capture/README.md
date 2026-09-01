# featureFlags wire-capture fixture (CHAOS-4696)

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

These two digests are DIFFERENT (the source text has a 122-character
single-line `featureFlags(...)` field argument list; urql's real
`print()` reflows it past 80 characters) -- this is exactly CHAOS-4696's
defect. `cmd/query-api/query_route.go`'s `registeredFeatureFlagsDocument`
const must digest to `06ca28a0517a34c0f5a6cc25b193da7b5682bea5192ae93e5a79edc7e7742208`, not `555bc9f82339b8321f309a26d310c4a7e41e79b9b155da41f62d8e97b50da8b7`, for
query-api to accept a real client's request.

Captured: 2026-09-01T01:40:06.129Z, ops tip at capture time: see the
lane's PR description for the exact SHA this was verified against.
