package licensing

// ACR entitlement bases (CHAOS-3695): which grant opened ACR for an org while
// ACR is ask_dev OR the legacy agent_context_runtime grant. Logged at Info by
// the three ACR decision sites so CHAOS-12292 can count the orgs that still
// depend only on the legacy grant before it retires.
const (
	ACRBasisAskDev = "ask_dev"
	ACRBasisLegacy = "agent_context_runtime"
	ACRBasisBoth   = "both"
	ACRBasisNone   = "none"
)

// ACREntitlementBasis names the grant behind an ACR decision: ask_dev,
// agent_context_runtime (the legacy grant alone), both, or none.
func ACREntitlementBasis(askDev, legacy bool) string {
	switch {
	case askDev && legacy:
		return ACRBasisBoth
	case askDev:
		return ACRBasisAskDev
	case legacy:
		return ACRBasisLegacy
	}
	return ACRBasisNone
}
