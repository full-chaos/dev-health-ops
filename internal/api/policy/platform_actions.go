package policy

import "github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"

// The platform actions a decision site names, for packages that already
// import policy.
const (
	ActionAdminUsers       = rule.ActionAdminUsers
	ActionAdminOrgs        = rule.ActionAdminOrgs
	ActionAdminBilling     = rule.ActionAdminBilling
	ActionAdminImpersonate = rule.ActionAdminImpersonate
	ActionAdminFlags       = rule.ActionAdminFlags
	ActionAdminAudit       = rule.ActionAdminAudit
	ActionAdminTelemetry   = rule.ActionAdminTelemetry
	ActionAdminStats       = rule.ActionAdminStats
)
