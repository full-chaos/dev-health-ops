package routeswitch

// The catalog switch (CHAOS-8702, owner: "get rid of that damn check"; builds on CHAOS-8517).
//
// go_api_routing_state chose, per operation and per schema digest, between the Python api and Go. The
// Python api is deleted, so a routing row can no longer choose anything: a missing, stale or dark row
// only made query-api refuse an operation it can serve. After every schema change every row was stale
// and every page read no data until someone ran `dho goapi routing enable`.
//
// The rule: an operation in the registered-document inventory of the process is served. No routing row
// is read: not at the live digest, not at another one, in no mode.
//
// Not touched: the class-row switch (server/class_row_gate.go) and the proof switches (proof_switch.go)
// are still PostgresSwitch values that read rows. An MCP class root with no class row stays dark, and
// a measurement route still needs a row.

// NewCatalogSwitch is the switch /query and /graphql serve registered documents through.
// documentDigests must be the registered-document inventory of the process (server/query_route.go
// digestByOperation): an operation absent from it is never served.
func NewCatalogSwitch(documentDigests map[string]string) StaticSwitch {
	sw := make(StaticSwitch, len(documentDigests))
	for operation := range documentDigests {
		sw[operation] = true
	}
	return sw
}
