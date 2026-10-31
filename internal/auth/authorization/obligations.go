package authorization

// AuditReasonOwnerExempt marks an action a platform_owner session may take
// although the deny list would refuse it for anyone else.
const AuditReasonOwnerExempt = "owner_exempt"

// Obligations are the enforceable obligations of an allow
// (authorization_decision.v1). Redaction and step_up are never set here.
type Obligations struct {
	TenantScope      string `json:"tenant_scope,omitempty"`
	MinimumAssurance string `json:"minimum_assurance,omitempty"`
}

// DelegationAudit is the impersonation context of an exempted allow.
type DelegationAudit struct {
	ActorPrincipalID string
	OrganizationID   string
	Action           string
	Reason           string
}
