package postgres

import (
	"fmt"
	"sort"
	"strings"
)

const queryAPIAuthSchema = "auth"

var (
	queryAPIMembershipColumns = []string{"org_id", "role", "user_id"}
	queryAPISessionColumns    = []string{"created_at", "id", "legacy_token_version", "principal_id", "revoked_at", "web_session_id"}
	// queryAPIStampColumns is the one users read: the password stamp sessionrule.Revoked compares, by principal.
	queryAPIStampColumns = []string{"password_updated_at", "principal_id"}
)

// queryAPIAuthSurfaceDefects is the CHAOS-10985 pin on the declared query-api posture: public.memberships is held through
// exactly three SELECT columns and never as a whole table; no relation is schema-qualified (the manifest resolves to public);
// and what query-api reads in the auth schema is the sessions columns and the users password stamp (sessionrule.Revoked),
// never memberships or organizations, and no other users column.
func queryAPIAuthSurfaceDefects(p RolePosture, session QueryAPISessionRead) []string {
	var out []string
	memberships := map[string][]string{}
	for _, c := range p.ColumnScoped {
		if strings.Contains(c.TableName, ".") {
			out = append(out, fmt.Sprintf("column grant on the schema-qualified relation %s.%s (%s): the manifest names public relations only", c.TableName, c.ColumnName, c.Privilege))
		}
		if c.TableName == "memberships" {
			memberships[c.ColumnName] = append(memberships[c.ColumnName], c.Privilege)
		}
	}
	for _, t := range p.RequiredTables {
		if strings.Contains(t.TableName, ".") {
			out = append(out, fmt.Sprintf("table grant on the schema-qualified relation %s: the manifest names public relations only", t.TableName))
		}
		if t.TableName == "memberships" {
			out = append(out, "memberships is declared as a whole table: query-api holds three columns of it (user_id, org_id, role), SELECT only")
		}
	}
	var got []string
	for column, privileges := range memberships {
		got = append(got, column)
		if len(privileges) != 1 || privileges[0] != "SELECT" {
			out = append(out, fmt.Sprintf("memberships.%s is granted %v: SELECT only", column, privileges))
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(queryAPIMembershipColumns, ",") {
		out = append(out, fmt.Sprintf("memberships columns = %v, want exactly %v", got, queryAPIMembershipColumns))
	}
	if session.Schema != queryAPIAuthSchema {
		out = append(out, fmt.Sprintf("the session read is in schema %q, want the auth schema %q", session.Schema, queryAPIAuthSchema))
	}
	columns := append([]string{}, session.Columns...)
	sort.Strings(columns)
	if session.Table != "sessions" {
		out = append(out, fmt.Sprintf("the auth-schema read is table %q, want sessions: query-api holds nothing of the auth memberships or organizations", session.Table))
	}
	if strings.Join(columns, ",") != strings.Join(queryAPISessionColumns, ",") {
		out = append(out, fmt.Sprintf("the auth session read columns = %v, want exactly %v", columns, queryAPISessionColumns))
	}
	stamp := false
	for _, also := range session.Also {
		cols := append([]string{}, also.Columns...)
		sort.Strings(cols)
		switch also.Table {
		case "users":
			stamp = true
			if strings.Join(cols, ",") != strings.Join(queryAPIStampColumns, ",") {
				out = append(out, fmt.Sprintf("the auth users read columns = %v, want exactly %v", cols, queryAPIStampColumns))
			}
		case "platform_role_assignments", "principals":
		default:
			out = append(out, fmt.Sprintf("the auth session read also reads table %q: query-api holds nothing of the auth memberships or organizations", also.Table))
		}
	}
	if !stamp {
		out = append(out, "the auth session read holds no users password stamp: sessionrule.Revoked reads it")
	}
	return out
}
