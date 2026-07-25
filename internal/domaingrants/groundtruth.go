package domaingrants

import (
	"fmt"
	"regexp"
	"strings"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// GroundTruth is the fully-parsed content of the two hand-maintained
// artefacts, keyed by public-schema table name.
type GroundTruth struct {
	// Grants is parsed from runtimeGrantStatements(...) (migrate.go), called
	// in-process via the grantcheck_export.go shim -- i.e. this is the exact
	// SQL migrate.go would execute, not a restatement of it.
	Grants map[string]PrivilegeSet
	// RequiredTablePrivileges is parsed from the required_table_privileges
	// VALUES rows embedded in domainAuthorizationQuery
	// (domain_authorization.go), obtained the same way.
	RequiredTablePrivileges map[string]PrivilegeSet
	// ColumnScopedTables lists tables domain_authorization.go grants only
	// column-scoped privileges on (e.g. worker_job_completion_fences).
	// These are deliberately excluded from RequiredTablePrivileges/table-wide
	// comparison: the checker cannot yet reason about column-level privilege
	// requirements from the derived query surface, so it reports these
	// separately rather than silently misclassifying them. See the handoff
	// README's known-limitations list.
	ColumnScopedTables map[string]bool
}

var grantStatementTableRE = regexp.MustCompile(
	`(?is)GRANT\s+([A-Z, ]+?)\s+ON\s+TABLE\s+public\.([a-zA-Z_][a-zA-Z0-9_]*)\s+TO\s+"?` + regexp.QuoteMeta(riverstore.GrantCheckDomainRole) + `"?\b`,
)

var requiredRowRE = regexp.MustCompile(
	`(?is)\(\s*'([a-zA-Z_][a-zA-Z0-9_]*)'\s*,\s*(true|false)\s*,\s*(true|false)\s*\)`,
)

var columnScopedRowRE = regexp.MustCompile(
	`(?is)\(\s*'([a-zA-Z_][a-zA-Z0-9_]*)'\s*,\s*'[a-zA-Z_][a-zA-Z0-9_]*'\s*,\s*'(SELECT|INSERT|UPDATE|DELETE|REFERENCES)'\s*\)`,
)

// LoadGroundTruth calls into internal/storage/river and
// internal/storage/postgres (via their grantcheck_export.go shims) to obtain
// the real, currently-shipping text of both hand-maintained artefacts, then
// parses each into a table->privilege map. No file on disk is read or
// restated: this is the same code path production uses to build the GRANT
// statements and the same constant string production uses for the
// readiness query.
func LoadGroundTruth() (*GroundTruth, error) {
	gt := &GroundTruth{
		Grants:                  map[string]PrivilegeSet{},
		RequiredTablePrivileges: map[string]PrivilegeSet{},
		ColumnScopedTables:      map[string]bool{},
	}

	for _, stmt := range riverstore.DerivedRuntimeGrantStatements() {
		for _, m := range grantStatementTableRE.FindAllStringSubmatch(stmt, -1) {
			privList, table := m[1], strings.ToLower(m[2])
			set := gt.Grants[table]
			for _, p := range strings.Split(privList, ",") {
				switch strings.ToUpper(strings.TrimSpace(p)) {
				case "SELECT":
					set.add(PrivSelect)
				case "INSERT":
					set.add(PrivInsert)
				case "UPDATE":
					set.add(PrivUpdate)
				case "DELETE":
					set.add(PrivDelete)
				}
			}
			gt.Grants[table] = set
		}
	}
	if len(gt.Grants) == 0 {
		return nil, fmt.Errorf("domaingrants: parsed zero GRANT statements for %s out of runtimeGrantStatements; "+
			"the extraction regex has drifted from migrate.go's actual statement shape", riverstore.GrantCheckDomainRole)
	}

	query := postgresstore.DomainAuthorizationQueryForCheck()
	reqSection, columnSection := splitRequiredPrivilegesSections(query)

	for _, m := range requiredRowRE.FindAllStringSubmatch(reqSection, -1) {
		table := strings.ToLower(m[1])
		var set PrivilegeSet
		set.add(PrivSelect) // every required_table_privileges row is baseline-SELECT
		if strings.EqualFold(m[2], "true") {
			set.add(PrivInsert)
		}
		if strings.EqualFold(m[3], "true") {
			set.add(PrivUpdate)
		}
		gt.RequiredTablePrivileges[table] = set
	}
	if len(gt.RequiredTablePrivileges) == 0 {
		return nil, fmt.Errorf("domaingrants: parsed zero required_table_privileges rows from domainAuthorizationQuery; " +
			"the extraction regex has drifted from domain_authorization.go's actual VALUES shape")
	}

	for _, m := range columnScopedRowRE.FindAllStringSubmatch(columnSection, -1) {
		gt.ColumnScopedTables[strings.ToLower(m[1])] = true
	}

	return gt, nil
}

// splitRequiredPrivilegesSections separates the required_table_privileges
// VALUES block from any column_scoped_privileges VALUES block that follows
// it in the same query, so a column-scoped row's privilege literal
// ('SELECT') is never mistaken for a required_table_privileges row's
// allow_insert/allow_update booleans (which happen to share the token
// shape only coincidentally -- true/false vs quoted privilege names, so in
// practice they cannot cross-match, but the split keeps the two regexes
// scoped to their own CTE and makes that guarantee explicit rather than
// incidental).
func splitRequiredPrivilegesSections(query string) (required, columnScoped string) {
	const columnMarker = "column_scoped_privileges"
	idx := strings.Index(query, columnMarker)
	if idx < 0 {
		return query, ""
	}
	return query[:idx], query[idx:]
}
