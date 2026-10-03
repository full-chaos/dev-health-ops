# Two routing-row families, two routes (CHAOS-7833)

Engineering note, not customer documentation. Cross-linked from the doc comment of `internal/queryapi/server/mcp_route.go` and pinned by
`TestDocumentRowsAndMCPClassRowsGovernSeparateRoutes` (`internal/queryapi/server/mcp_class_routing_integration_test.go`).

`go_api_routing_state` holds two kinds of row. They are looked up by different switches and govern different routes. Neither stands in for the other.

| Row key (`selected_operation`) | Switch | Route | Listener | acr consumer |
|---|---|---|---|---|
| a named document operation, e.g. `hotspots` | `routeswitch.NewCatalogSwitchWithLegacy(pgPool, schemaDigest, digestByOperation, legacyDigestsByOperation)` (`server/query_route.go`, `newQueryHandler`) | `POST /query`, named-operation route (`server/server.go:222` public, `internalMux.Handle("/", ...)` at `server.go:888`) | `:8090` / `:8091` | `run_operation` (acr `ACR_DATA_QUERY_URL`) |
| `mcp:<root>`, e.g. `mcp:hotspots` | `routeswitch.NewPostgresSwitch(pgPool, schemaDigest, mcpRoutingDigests())` (`server/query_route.go:2742`) | `POST /query`, MCP class route (`server/server.go:311` `mcpMux`) | `:8092` | `graphql_query` (acr `ACR_DATA_GRAPHQL_URL`) |

What a row does on the MCP listener (`server/mcp_route.go:548`, `routeswitch/postgres_switch.go:30-32,125`): `Enabled("mcp:"+root)` is true only when a row
exists for the current schema digest AND its mode is `canary` or `primary`. A root whose class row is absent, `shadow`, `python` or `disabled`
is refused with HTTP 404, reason `root_field_not_enabled`. There is no pass-through to another plane: what passes is served by the Go server.

What a row does on the named-operation route differs in one state (CHAOS-8517, `routeswitch/catalog_switch.go`): a document operation with NO row at
any schema digest is served. A document operation that has a row keeps the rule of its row (served only in `canary` or `primary`; a row left at
another digest holds it dark). The class rows have no such state: a class root with no row is dark, and `/query/run-operation` checks the class
row of every allowlisted root BEFORE the document switch (`server/class_row_gate.go`), so a root cannot be reached there through an operation
that has no document row.

Consequences:

- "Dark" for a class root (no class row, or shadow) means `graphql_query` on that root is refused. `run_operation` on the same data is unaffected:
  it is decided by the document rows.
- Enabling a class root (`dho goapi routing enable -operations mcp:<root>`) changes only the `:8092` route for that root.
- A canary document row does not serve the MCP listener, and an enabled class row does not enable a document operation.
- The class receipt proves the root through the MCP pipeline against the Go document route (`dho goapi prove -mcp-roots`); it does not prove
  `run_operation`. Read the real consumer through the tool that uses the route under test.
