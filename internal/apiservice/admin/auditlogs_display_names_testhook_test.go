package admin

import "github.com/jackc/pgx/v5"

// AuditLogProjectionSQLForTest gives the external real-Postgres venue test
// the exact production projection. This _test.go seam is absent from the
// production binary.
func AuditLogProjectionSQLForTest() string {
	return `SELECT ` + auditLogColumns + auditLogFrom
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
