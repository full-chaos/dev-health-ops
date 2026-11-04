package authorization

// AuditReasonOwnerExempt marks an action a platform_owner session may take
// although the deny list would refuse it for anyone else.
const AuditReasonOwnerExempt = "owner_exempt"

// MaxRepositoryAllowlist is the largest repository allowlist a decision carries: the bound of
// authorization_decision.v1 repository_allowlist (maxItems). A principal with more readable
// repositories is denied with ReasonRepositoryAllowlistTooLarge; a list is never truncated.
const MaxRepositoryAllowlist = 256

// RepositoryScopeOrganization is the one repository_scope value: a read of every repository of the tenant.
const RepositoryScopeOrganization = "organization"

// Obligations are the enforceable obligations of an allow
// (authorization_decision.v1). step_up is never set here.
type Obligations struct {
	TenantScope      string `json:"tenant_scope,omitempty"`
	MinimumAssurance string `json:"minimum_assurance,omitempty"`
	// RepositoryScope is RepositoryScopeOrganization when the allow reads every repository of the tenant.
	// It and the allowlist member are never both present.
	RepositoryScope string `json:"repository_scope,omitempty"`
	// RepositoryAllowlist is the repository keys the allow may read, sorted; never "*".
	RepositoryAllowlist []string `json:"repository_allowlist,omitempty"`
	// RepositoryRestricted is true when the allowlist member is present, an
	// empty list included: an empty allowlist means NONE, and a slice that is
	// empty cannot say it was present.
	RepositoryRestricted bool `json:"-"`
	// Redaction is the field classes the consumer must remove.
	Redaction []string `json:"redaction,omitempty"`
}

// DelegationAudit is the impersonation context of an exempted allow.
type DelegationAudit struct {
	ActorPrincipalID string
	OrganizationID   string
	Action           string
	Reason           string
}
