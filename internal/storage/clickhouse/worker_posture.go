package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

// WorkerUser is the ClickHouse login the prod workers connect as. It exists in
// prod (ClickHouse Cloud) only: compose and bigboy workers log in as the admin
// user.
const WorkerUser = "dho_app"

// WorkerGrant is one privilege of the worker login's posture, spelled as
// system.grants writes it: AccessType is the server's canonical name ("ALTER
// DELETE", never its alias "DELETE"), and Table "" is every table of Database
// (system.grants writes NULL).
type WorkerGrant struct {
	Database   string
	Table      string
	AccessType string
	// Reader is one file:line that needs the privilege.
	Reader string
}

// WorkerPosture is exactly the grants prod dho_app held at the 2026-11-10
// system.grants read: no more, no less.
func WorkerPosture() []WorkerGrant {
	return []WorkerGrant{
		{Database: "default", AccessType: "SELECT", Reader: "internal/jobs/workgraph/edges/clickhouse.go:44"},
		{Database: "default", AccessType: "INSERT", Reader: "internal/streamhandlers/external_clickhouse.go:371"},
		{Database: "default", AccessType: "ALTER DELETE", Reader: "internal/jobs/metrics/remaining/membership_write.go:251"},
		{Database: "default", AccessType: "OPTIMIZE", Reader: "internal/jobs/dimensionfold/fold.go:202"},
		{Database: "system", Table: "columns", AccessType: "SELECT", Reader: "internal/jobs/metrics/remaining/capacity_native_clickhouse.go:719"},
		{Database: "system", Table: "parts", AccessType: "SELECT", Reader: "internal/jobs/dimensionfold/fold.go:166"},
		{Database: "system", Table: "tables", AccessType: "SELECT", Reader: "internal/jobs/metrics/remaining/capacity_native_clickhouse.go:752"},
		{Database: "system", Table: "view_refreshes", AccessType: "SELECT", Reader: "internal/apiservice/admin/orgdeletion.go:631"},
	}
}

func (grant WorkerGrant) scope() string {
	if grant.Table == "" {
		return grant.Database + ".*"
	}
	return grant.Database + "." + grant.Table
}

// WorkerGrantStatements renders WorkerPosture as the GRANT SQL an operator
// runs for role: one statement per database/table, privileges in posture order.
func WorkerGrantStatements(role string) []string {
	var scopes []string
	privileges := map[string][]string{}
	for _, grant := range WorkerPosture() {
		scope := grant.scope()
		if _, seen := privileges[scope]; !seen {
			scopes = append(scopes, scope)
		}
		privileges[scope] = append(privileges[scope], grant.AccessType)
	}
	statements := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		statements = append(statements, fmt.Sprintf("GRANT %s ON %s TO %s", strings.Join(privileges[scope], ", "), scope, role))
	}
	return statements
}

// WorkerGrantsColumns are the system.grants columns WorkerGrantsSelect reads,
// in its order.
var WorkerGrantsColumns = []string{
	"user_name", "role_name", "access_type", "database", "table", "column", "is_partial_revoke", "grant_option",
}

// WorkerGrantsSelect reads one user's raw system.grants rows ({user:String})
// and the rows of every role granted to that user, so a role grant shows up as
// a row with role_name set. A role that holds no privilege of its own adds no
// row.
const WorkerGrantsSelect = `SELECT user_name, role_name, access_type, database, table, column, is_partial_revoke, grant_option
FROM system.grants
WHERE user_name = {user:String}
   OR role_name IN (SELECT granted_role_name FROM system.role_grants WHERE user_name = {user:String})
ORDER BY user_name, role_name, database, table, access_type`

// SystemGrantRow is one row of WorkerGrantsSelect. A nil pointer is NULL.
type SystemGrantRow struct {
	UserName        *string
	RoleName        *string
	AccessType      string
	Database        *string
	Table           *string
	Column          *string
	IsPartialRevoke uint8
	GrantOption     uint8
}

func (row SystemGrantRow) scope() string {
	database, table := "*", "*"
	if row.Database != nil {
		database = *row.Database
	}
	if row.Table != nil {
		table = *row.Table
	}
	return database + "." + table
}

// ErrWorkerPostureMismatch is CompareWorkerGrants' refusal.
var ErrWorkerPostureMismatch = errors.New("clickhouse: worker grants do not match the declared posture")

type workerGrantKey struct{ scope, accessType string }

func nonEmpty(value *string) bool { return value != nil && *value != "" }

// CompareWorkerGrants proves rows (WorkerGrantsSelect for WorkerUser) are
// exactly WorkerPosture. Names compare byte for byte: an alias or a broader
// privilege is an extra privilege, and the declared one stays missing.
func CompareWorkerGrants(rows []SystemGrantRow) error {
	declared := map[workerGrantKey]bool{}
	declaredScopes := map[string]bool{}
	for _, grant := range WorkerPosture() {
		declared[workerGrantKey{grant.scope(), grant.AccessType}] = true
		declaredScopes[grant.scope()] = true
	}
	held := map[workerGrantKey]bool{}
	var problems []string
	for _, row := range rows {
		scope := row.scope()
		where := row.AccessType + " ON " + scope
		switch {
		case nonEmpty(row.RoleName):
			problems = append(problems, fmt.Sprintf("role grant: role %s holds %s", *row.RoleName, where))
			continue
		case row.UserName == nil:
			problems = append(problems, "row for another user NULL: "+where)
			continue
		case *row.UserName != WorkerUser:
			problems = append(problems, fmt.Sprintf("row for another user %s: %s", *row.UserName, where))
			continue
		case nonEmpty(row.Column):
			problems = append(problems, fmt.Sprintf("column-level grant: %s(%s)", where, *row.Column))
			continue
		case row.IsPartialRevoke != 0:
			problems = append(problems, "partial revoke: "+where)
			continue
		// A NULL database or table renders as "*"; a stored "*" (or a
		// prefix wildcard grant) must not read as that NULL.
		case row.Database != nil && strings.Contains(*row.Database, "*"),
			row.Table != nil && strings.Contains(*row.Table, "*"):
			problems = append(problems, "wildcard scope: "+where)
			continue
		}
		if row.GrantOption != 0 {
			problems = append(problems, "grant option: "+where)
		}
		key := workerGrantKey{scope, row.AccessType}
		switch {
		case held[key]:
			problems = append(problems, "duplicate row: "+where)
		case !declaredScopes[scope]:
			problems = append(problems, "extra database/table: "+where)
		case !declared[key]:
			problems = append(problems, "extra privilege: "+where)
		}
		held[key] = true
	}
	for _, grant := range WorkerPosture() {
		if !held[workerGrantKey{grant.scope(), grant.AccessType}] {
			problems = append(problems, "missing privilege: "+grant.AccessType+" ON "+grant.scope())
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrWorkerPostureMismatch, strings.Join(problems, "; "))
	}
	return nil
}
