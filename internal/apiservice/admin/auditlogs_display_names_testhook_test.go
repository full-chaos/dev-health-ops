package admin

import "github.com/jackc/pgx/v5"

// AuditLogProjectionSQLForTest gives the external real-Postgres venue test
// the exact production projection. This _test.go seam is absent from the
// production binary.
func AuditLogProjectionSQLForTest() string {
	return `SELECT ` + auditLogColumns + auditLogFrom
}

// AuditLogJoinDiagnosticSQLForTest keeps auditLogFrom byte-for-byte while
// selecting relevant joined-row and display-column booleans. The CASE
// projection is measured by AuditLogResourceDisplayNameStateForTest, which
// scans AuditLogProjectionSQLForTest. auditLogColumns has no COALESCE branch;
// its string branches use nullif(btrim(...), ”).
func AuditLogJoinDiagnosticSQLForTest() string {
	return `SELECT a.resource_type,
	resource_org.id IS NOT NULL,
	resource_org.name IS NOT NULL,
	provider.id IS NOT NULL,
	provider.name IS NOT NULL,
	source.id IS NOT NULL,
	nullif(btrim(source.display_name), '') IS NOT NULL,
	token.id IS NOT NULL,
	token.name IS NOT NULL` + auditLogFrom
}

// AuditLogJoinStateForTest reads the bounded join-branch diagnostic. It
// returns no labels, IDs, or row content.
func AuditLogJoinStateForTest(row pgx.Row) (
	resourceType string,
	organizationRow, organizationDisplay bool,
	providerRow, providerDisplay bool,
	sourceRow, sourceDisplay bool,
	tokenRow, tokenDisplay bool,
	err error,
) {
	err = row.Scan(
		&resourceType,
		&organizationRow,
		&organizationDisplay,
		&providerRow,
		&providerDisplay,
		&sourceRow,
		&sourceDisplay,
		&tokenRow,
		&tokenDisplay,
	)
	return resourceType, organizationRow, organizationDisplay, providerRow, providerDisplay, sourceRow, sourceDisplay, tokenRow, tokenDisplay, err
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
