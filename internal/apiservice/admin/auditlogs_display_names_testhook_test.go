package admin

import "github.com/jackc/pgx/v5"

// AuditLogProjectionSQLForTest gives the external real-Postgres venue test
// the exact production projection. This _test.go seam is absent from the
// production binary.
func AuditLogProjectionSQLForTest() string {
	return `SELECT ` + auditLogColumns + auditLogFrom
}

// AuditLogJoinBranchStateForTest holds only branch booleans. It carries no
// labels, IDs, or row content.
type AuditLogJoinBranchStateForTest struct {
	ResourceTypeMatches bool
	UUIDGuardMatches    bool
	TargetIDMatches     bool
	TargetScopeMatches  bool
	JoinedRowPresent    bool
	DisplayPresent      bool
}

// AuditLogJoinDiagnosticStateForTest is the bounded state from one exact
// auditLogFrom statement. The target checks use correlated EXISTS predicates
// over the same audit row; the final two booleans in each branch read the
// joined alias from auditLogFrom itself.
type AuditLogJoinDiagnosticStateForTest struct {
	ResourceType string
	Organization AuditLogJoinBranchStateForTest
	Provider     AuditLogJoinBranchStateForTest
	Source       AuditLogJoinBranchStateForTest
	Token        AuditLogJoinBranchStateForTest
}

// AuditLogJoinDiagnosticSQLForTest keeps auditLogFrom byte-for-byte. Each
// branch captures its resource-type guard, UUID guard, ID lookup, audited-org
// lookup, and the resulting joined alias in one statement. The CASE projection
// is measured by AuditLogResourceDisplayNameStateForTest, which scans
// AuditLogProjectionSQLForTest. auditLogColumns has no COALESCE branch; its
// string branches use nullif(btrim(...), empty string).
func AuditLogJoinDiagnosticSQLForTest() string {
	const uuidGuard = `a.resource_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'`
	const resourceID = `CASE WHEN ` + uuidGuard + ` THEN a.resource_id::uuid END`
	return `SELECT a.resource_type,
	a.resource_type = 'organization',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM organizations probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM organizations probe WHERE probe.id = ` + resourceID + ` AND probe.id = a.org_id),
	resource_org.id IS NOT NULL,
	resource_org.name IS NOT NULL,
	a.resource_type = 'sso_provider',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM sso_providers probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM sso_providers probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id),
	provider.id IS NOT NULL,
	provider.name IS NOT NULL,
	a.resource_type = 'ingest_source',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM external_ingest_sources probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM external_ingest_sources probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id::text),
	source.id IS NOT NULL,
	nullif(btrim(source.display_name), '') IS NOT NULL,
	a.resource_type = 'ingest_token',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM external_ingest_tokens probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM external_ingest_tokens probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id::text),
	token.id IS NOT NULL,
	token.name IS NOT NULL` + auditLogFrom
}

// AuditLogJoinStateForTest reads the bounded exact-statement diagnostic.
func AuditLogJoinStateForTest(row pgx.Row) (state AuditLogJoinDiagnosticStateForTest, err error) {
	err = row.Scan(
		&state.ResourceType,
		&state.Organization.ResourceTypeMatches,
		&state.Organization.UUIDGuardMatches,
		&state.Organization.TargetIDMatches,
		&state.Organization.TargetScopeMatches,
		&state.Organization.JoinedRowPresent,
		&state.Organization.DisplayPresent,
		&state.Provider.ResourceTypeMatches,
		&state.Provider.UUIDGuardMatches,
		&state.Provider.TargetIDMatches,
		&state.Provider.TargetScopeMatches,
		&state.Provider.JoinedRowPresent,
		&state.Provider.DisplayPresent,
		&state.Source.ResourceTypeMatches,
		&state.Source.UUIDGuardMatches,
		&state.Source.TargetIDMatches,
		&state.Source.TargetScopeMatches,
		&state.Source.JoinedRowPresent,
		&state.Source.DisplayPresent,
		&state.Token.ResourceTypeMatches,
		&state.Token.UUIDGuardMatches,
		&state.Token.TargetIDMatches,
		&state.Token.TargetScopeMatches,
		&state.Token.JoinedRowPresent,
		&state.Token.DisplayPresent,
	)
	return state, err
}

// AuditLogResourceDisplayNameStateForTest executes the production scanner and
// object encoder for the exact projection supplied by
// AuditLogProjectionSQLForTest. The external venue test records only
// presence, never a fixture label or row contents.
func AuditLogResourceDisplayNameStateForTest(row pgx.Row) (found, scannedPresent, encodedPresent bool, err error) {
	log, err := scanAuditLog(row)
	if err != nil || log == nil {
		return false, false, false, err
	}
	object, err := auditLogObject(log)
	if err != nil {
		return true, log.ResourceDisplayName != nil, false, err
	}
	value, ok := object.Get("resource_display_name")
	if !ok {
		return true, log.ResourceDisplayName != nil, false, nil
	}
	_, encodedPresent = value.(string)
	return true, log.ResourceDisplayName != nil, encodedPresent, nil
}
