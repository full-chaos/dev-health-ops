package goapiproof

// Digest states of an MCP class decision as `status` reports it (class decisions are digest-agnostic, so a
// decision is either present or missing). A catalog operation has no routing state to report: query-api serves
// every registered operation (routeswitch.NewCatalogSwitch) and no row decides it.
const (
	// DigestMatch: the class operation has a decision row in go_api_class_decision.
	DigestMatch = "MATCH"
	// DigestMissing: no decision row. The root is dark.
	DigestMissing = "MISSING"
)

// servedMode reports whether a decision mode serves a real request: canary and primary only. shadow is
// measurement-only, python and disabled are decisions to keep the root dark.
func servedMode(mode string) bool {
	return mode == TargetModeCanary || mode == TargetModePrimary
}
