package riverstore

// This file exists solely to give the CHAOS-3033 grant-surface-deriver
// checker (internal/domaingrants) a read-only, in-process way to obtain the
// EXACT statements runtimeGrantStatements (migrate.go) produces, without
// duplicating or re-parsing that logic from source text. It does not modify
// migrate.go and does not change production behavior: runtimeGrantStatements
// is called with fixed, checker-only identifiers so its returned SQL text is
// deterministic and diffable across runs.
//
// The role partition this checks against, and why the split exists, are
// documented in the role-partition manifest (removed in e23ede618; see git
// history at eda2d6b91). The consumer is internal/domaingrants
// (LoadGroundTruth). An earlier version of this comment pointed at an
// absolute path under .remember/, which is ephemeral scratch rather than
// shipped documentation and did not exist.

// GrantCheckSchema, GrantCheckDomainRole, and GrantCheckQueueRole are the
// fixed, arbitrary-but-valid identifiers the checker renders
// runtimeGrantStatements with. They must satisfy ValidateMigrationOptions
// (lowercase, <=63 chars, distinct) and are never used against a real
// database.
const (
	GrantCheckSchema     = "river"
	GrantCheckDomainRole = "domain_role"
	GrantCheckQueueRole  = "queue_role"
)

// DerivedRuntimeGrantStatements returns the exact statement list
// runtimeGrantStatements(options) produces for the fixed checker identifiers
// above. Callers should treat the returned strings as opaque SQL text and
// parse them the same way regardless of which identifiers were used, since
// the checker only cares about privilege/table shape, not the literal
// role/schema names.
func DerivedRuntimeGrantStatements() []string {
	return runtimeGrantStatements(MigrationOptions{
		Schema:     GrantCheckSchema,
		DomainRole: GrantCheckDomainRole,
		QueueRole:  GrantCheckQueueRole,
	})
}
