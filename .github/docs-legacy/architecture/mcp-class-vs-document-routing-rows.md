# Class decisions and catalog operations: two routes, one table (CHAOS-7833, CHAOS-8702)

Engineering note, not customer documentation. Cross-linked from the doc comment of `internal/queryapi/server/mcp_route.go` and pinned by
`TestDocumentRoutesAndMCPClassDecisionsAreDecidedApart` (`internal/queryapi/server/mcp_class_routing_integration_test.go`).

There are two kinds of operation, decided by different switches on different routes. Neither stands in for the other. Only one of them has routing
state.

| Operation | Switch | Route | Listener | acr consumer |
|---|---|---|---|---|
| a catalog (document) operation, e.g. `hotspots` | `routeswitch.NewCatalogSwitch(digestByOperation)` (`server/query_route.go`, `newQueryHandler`) | `POST /query`, named-operation route | `:8090` / `:8091` | `run_operation` |
| `mcp:<root>`, e.g. `mcp:hotspots` | `routeswitch.NewClassDecisionSwitch(pgPool, mcpRoutingDigests())` | `POST /query`, MCP class route (`mcpMux`) | `:8092` | `graphql_query` (acr `ACR_DATA_GRAPHQL_URL`) |

**A catalog operation has no routing state.** query-api serves every operation in the registered-document inventory of the process and reads no database for
it: a missing, stale or dark row cannot hold it dark, because there is no row (CHAOS-8702, which removed `go_api_routing_state` from the serving path).
`status` lists each catalog operation against the deployed registry (`AGREE`, `MISMATCH`, `UNREGISTERED`).

**A class root has one decision row**, in `go_api_class_decision` (CHAOS-8735), keyed by `mcp:<root>` alone, so a schema change moves nothing.
`Enabled("mcp:"+root)` (`routeswitch/class_decision_switch.go`) is true only when the decision's mode is `canary` or `primary`. A root whose decision is
absent, `shadow`, `python` or `disabled` is refused with HTTP 404, reason `root_field_not_enabled`; `shadow` serves only the measurement-only proof route.
There is no pass-through to another plane: what passes is served by the Go server. `/query/run-operation` checks the class decision of every allowlisted
root BEFORE the catalog switch (`server/class_row_gate.go`), so a root cannot be reached there through an operation that is a catalog operation.

Consequences:

- "Dark" for a class root (no decision, or shadow) means `graphql_query` on that root is refused. `run_operation` on the same data is unaffected: a catalog
  operation is served.
- Enabling a class root (`dho goapi routing enable -operations mcp:<root>`) changes only the `:8092` route for that root.
- A catalog operation being served does not serve the MCP listener, and an enabled class decision does not change what the catalog switch serves.
- The class receipt proves the root through the MCP pipeline against the Go document route (`dho goapi prove -mcp-roots`); it does not prove
  `run_operation`. Read the real consumer through the tool that uses the route under test.
