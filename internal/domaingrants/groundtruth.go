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
	// RequiredTablePrivileges is the domain role's declared posture, read as
	// data from postgresstore.DomainPosture(). It used to be regexed out of
	// the readiness query's required_table_privileges VALUES rows; Phase 2 of
	// the Option B role split parameterized that query over posture data bound
	// through unnest, so the rows are no longer in the SQL text and the
	// declaration itself is what gets read.
	RequiredTablePrivileges map[string]PrivilegeSet
	// ColumnScopedTables lists tables the domain posture declares only
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

// LoadGroundTruth reads both hand-maintained artefacts through the same code
// paths production uses, and restates neither. The GRANT side still comes as
// SQL text, from riverstore.DerivedRuntimeGrantStatements() (grantcheck_export.go),
// because migrate.go genuinely builds statements. The posture side is read as
// data from postgresstore.DomainPosture(), because since Phase 2 of the Option
// B role split it genuinely IS data — the readiness query binds it through
// unnest rather than embedding VALUES rows, so there is no SQL text left to
// parse there.
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

	posture := postgresstore.DomainPosture()
	for _, table := range posture.RequiredTables {
		var set PrivilegeSet
		set.add(PrivSelect) // SELECT is implied by every posture row
		if table.AllowInsert {
			set.add(PrivInsert)
		}
		if table.AllowUpdate {
			set.add(PrivUpdate)
		}
		if table.AllowDelete {
			set.add(PrivDelete)
		}
		gt.RequiredTablePrivileges[strings.ToLower(table.TableName)] = set
	}
	if len(gt.RequiredTablePrivileges) == 0 {
		return nil, fmt.Errorf("domaingrants: DomainPosture() declared zero required tables, " +
			"which cannot be right for a role the runtime asserts a posture for")
	}

	for _, column := range posture.ColumnScoped {
		gt.ColumnScopedTables[strings.ToLower(column.TableName)] = true
	}

	return gt, nil
}
