package domaingrants

import (
	"fmt"
	"sort"
	"strings"
)

// Severity distinguishes a provable defect (the tool found real code
// executing a statement neither hand-maintained list authorizes) from an
// advisory note (a granted privilege the tool found no evidence for, which
// may be a genuine over-grant OR simply a call path this analyzer's static
// scope can't see -- see the handoff README's known-limitations list. Under
// this tool's design, ONLY Critical findings fail CI; Advisory findings are
// printed for a human to triage, never silently dropped, never fatal.
type Severity int

const (
	Critical Severity = iota
	Advisory
)

func (s Severity) String() string {
	if s == Critical {
		return "CRITICAL"
	}
	return "ADVISORY"
}

// Finding is one row of the comparison between the derived query surface and
// the two hand-maintained artefacts.
type Finding struct {
	Severity Severity
	// Role is which pool/database role this finding is about. Empty for the
	// single-role Compare path; always set by CompareRoles, because "which role"
	// is the whole point once there is more than one.
	Role      PoolRole
	Table     string
	Privilege Privilege
	Summary   string
	Evidence  []Evidence
}

// Report is the full comparison result.
type Report struct {
	Findings []Finding
	// Stats for the human-readable summary.
	DerivedTableCount  int
	DynamicSiteCount   int
	UnresolvedCount    int
	DevirtualizedCount int
}

// HasCritical reports whether any finding is Critical -- the CI-gate signal.
func (r *Report) HasCritical() bool {
	for _, f := range r.Findings {
		if f.Severity == Critical {
			return true
		}
	}
	return false
}

// Compare cross-checks the derived surface against both hand-maintained
// artefacts (and them against each other) and returns every disagreement,
// most-severe first.
func Compare(derived *DerivedSurface, gt *GroundTruth) *Report {
	report := &Report{
		DerivedTableCount:  len(derived.Tables),
		DynamicSiteCount:   len(derived.Dynamic),
		UnresolvedCount:    len(derived.Unresolved),
		DevirtualizedCount: len(derived.Devirtualized),
	}

	// 1) The two hand-maintained lists vs each other -- sound, no derivation
	// needed: if migrate.go grants a privilege required_table_privileges
	// doesn't also require (or vice versa), CheckDomainAuthorization and the
	// real grants have drifted from EACH OTHER, independent of whether the
	// derived surface agrees with either.
	allTables := map[string]bool{}
	for t := range gt.Grants {
		allTables[t] = true
	}
	for t := range gt.RequiredTablePrivileges {
		allTables[t] = true
	}
	for _, table := range sortedKeys(allTables) {
		granted := gt.Grants[table]
		required := gt.RequiredTablePrivileges[table]
		_, grantedExists := gt.Grants[table]
		_, requiredExists := gt.RequiredTablePrivileges[table]
		if grantedExists != requiredExists {
			side := "migrate.go grants it but domain_authorization.go's required_table_privileges has no row"
			if requiredExists {
				side = "domain_authorization.go's required_table_privileges requires it but migrate.go grants nothing"
			}
			report.Findings = append(report.Findings, Finding{
				Severity: Critical, Table: table,
				Summary: fmt.Sprintf("migrate.go and domain_authorization.go disagree on table %q: %s", table, side),
			})
			continue
		}
		for p := Privilege(0); p < numPrivileges; p++ {
			if granted.Has(p) != required.Has(p) {
				report.Findings = append(report.Findings, Finding{
					Severity: Critical, Table: table, Privilege: p,
					Summary: fmt.Sprintf(
						"table %q: runtimeGrantStatements grants %s=%v but required_table_privileges requires %s=%v (the two hand-maintained lists disagree with each other)",
						table, p, granted.Has(p), p, required.Has(p),
					),
				})
			}
		}
	}

	// 2) Derived requirements vs both lists -- the actual defect class this
	// tool exists to catch: real code executing a statement neither list
	// authorizes.
	for _, table := range derived.SortedTables() {
		surface := derived.Tables[table]
		granted := gt.Grants[table]
		required := gt.RequiredTablePrivileges[table]
		columnScoped := gt.ColumnScopedTables[table]
		for p := Privilege(0); p < numPrivileges; p++ {
			if !surface.Privileges.Has(p) {
				continue
			}
			evidence := evidenceFor(surface, p)
			// DELETE used to be special-cased Critical here: the posture was
			// a (table, allow_insert, allow_update) triple with no way to
			// express allow_delete, so a derived DELETE was unrepresentable
			// rather than merely unlisted. Phase 2 of the Option B role split
			// added AllowDelete to TablePrivilege and the matching DELETE
			// grants to runtimeGrantStatements, so DELETE is now an ordinary
			// privilege and comparing it like the others is what keeps this
			// checker honest — leaving the special case in place would report
			// three permanent false Criticals and hide any real DELETE gap
			// behind them.
			if columnScoped {
				// Column-scoped tables are handled by a different mechanism
				// this tool does not model (see GroundTruth.ColumnScopedTables
				// doc comment) -- report as advisory so a human checks the
				// column-level grant matches, rather than a false Critical
				// against the (deliberately absent) table-wide row.
				if !granted.Has(p) {
					report.Findings = append(report.Findings, Finding{
						Severity: Advisory, Table: table, Privilege: p,
						Summary: fmt.Sprintf(
							"table %q: derived surface requires %s and the table is column-scoped in domain_authorization.go -- verify the column-level grant covers this (tool does not model column grants)",
							table, p,
						),
						Evidence: evidence,
					})
				}
				continue
			}
			if !granted.Has(p) || !required.Has(p) {
				missing := []string{}
				if !granted.Has(p) {
					missing = append(missing, "runtimeGrantStatements (migrate.go)")
				}
				if !required.Has(p) {
					missing = append(missing, "required_table_privileges (domain_authorization.go)")
				}
				report.Findings = append(report.Findings, Finding{
					Severity: Critical, Table: table, Privilege: p,
					Summary: fmt.Sprintf(
						"table %q needs %s (proven by %d call site(s)) but it is missing from: %s",
						table, p, len(evidence), strings.Join(missing, " AND "),
					),
					Evidence: evidence,
				})
			}
		}
		if surface.LockRequirement != nil && !columnScoped {
			// Postgres's LOCK demand is a DISJUNCTION over a MODE-SPECIFIC set,
			// not "any write privilege": INSERT satisfies ROW SHARE and ROW
			// EXCLUSIVE and nothing stricter. See LockRequirement in sql.go for
			// the measured mapping. An earlier version of this check accepted any
			// of INSERT/UPDATE/DELETE for every mode, which passed a SELECT+INSERT
			// posture against a SHARE ROW EXCLUSIVE lock -- the CHAOS-3113 defect
			// family exactly.
			requirement := *surface.LockRequirement
			if !lockSatisfiedBy(requirement, granted) {
				report.Findings = append(report.Findings, Finding{
					Severity: Critical, Table: table,
					Summary: fmt.Sprintf(
						"table %q: LOCK IN %s MODE requires at least ONE of %s -- a disjunction, NOT a conjunction with SELECT -- but runtimeGrantStatements grants none of them",
						table, requirement.Mode, privilegeSetNames(requirement.Satisfying),
					),
					Evidence: surface.WriteLockEvidence,
				})
			}
		}
	}

	// 3) Granted/required privileges with no derived evidence -- advisory
	// only: this tool's derivation is a sound-but-incomplete
	// under-approximation (dynamic SQL, indirection the static analysis
	// can't follow), so absence of evidence is not evidence of an
	// over-grant. Still worth a human's attention.
	for _, table := range sortedKeys(unionKeys(gt.Grants, gt.RequiredTablePrivileges)) {
		granted := gt.Grants[table]
		required := gt.RequiredTablePrivileges[table]
		surface := derived.Tables[table]
		for p := Privilege(0); p < numPrivileges; p++ {
			if !granted.Has(p) && !required.Has(p) {
				continue
			}
			if surface != nil && surface.Privileges.Has(p) {
				continue
			}
			report.Findings = append(report.Findings, Finding{
				Severity: Advisory, Table: table, Privilege: p,
				Summary: fmt.Sprintf(
					"table %q: hand-maintained lists grant/require %s but the static derivation found no call site needing it -- may be a legitimate over-grant, OR a call path this tool's static scope can't see (dynamic SQL, unresolved interface dispatch); see the handoff README",
					table, p,
				),
			})
		}
	}

	// 4) Transaction-consistency cross-check: group tables by the
	// function-body-scoped TxGroup their evidence shares (see
	// Evidence.TxGroup's doc comment -- this does NOT trace a pgx.Tx handed
	// across function/type boundaries, only same-function-body co-writes),
	// and flag when one table in a group is fully covered by both
	// hand-maintained lists while another table in the SAME group is not.
	// This is the "quiet co-writer" failure shape: a loudly-named table gets
	// sized correctly while a table that happens to be written a few lines
	// later in the same transaction is missed entirely, and the loud
	// table's grant makes the change LOOK complete. Every table this
	// surfaces is already independently Critical from pass 2 above -- this
	// pass exists to make the transaction relationship visible, not to find
	// new tables.
	txGroups := map[string]map[string]bool{} // txGroup -> set of tables
	for _, table := range derived.SortedTables() {
		for _, e := range derived.Tables[table].Evidence {
			if e.TxGroup == "" {
				continue
			}
			set := txGroups[e.TxGroup]
			if set == nil {
				set = map[string]bool{}
				txGroups[e.TxGroup] = set
			}
			set[table] = true
		}
	}
	for _, txGroup := range sortedKeys(boolMapKeys(txGroups)) {
		tables := sortedKeys(txGroups[txGroup])
		if len(tables) < 2 {
			continue
		}
		var covered, uncovered []string
		for _, table := range tables {
			if isFullyCovered(derived.Tables[table], gt) {
				covered = append(covered, table)
			} else {
				uncovered = append(uncovered, table)
			}
		}
		if len(covered) == 0 || len(uncovered) == 0 {
			continue // whole transaction agrees (all covered or all missing) -- nothing distinctive to say
		}
		report.Findings = append(report.Findings, Finding{
			Severity: Critical, Table: strings.Join(uncovered, ", "),
			Summary: fmt.Sprintf(
				"transaction %s writes %s together: %s %s already fully granted, but %s %s NOT -- a quiet co-writer in an otherwise-correct-looking transaction (each missing table also has its own finding above; this one exists to show the transaction relationship)",
				txGroup, strings.Join(tables, "+"),
				pluralTables(covered), strings.Join(covered, ", "),
				pluralTables(uncovered), strings.Join(uncovered, ", "),
			),
		})
	}

	sort.SliceStable(report.Findings, func(i, j int) bool {
		if report.Findings[i].Severity != report.Findings[j].Severity {
			return report.Findings[i].Severity < report.Findings[j].Severity
		}
		if report.Findings[i].Table != report.Findings[j].Table {
			return report.Findings[i].Table < report.Findings[j].Table
		}
		return report.Findings[i].Privilege < report.Findings[j].Privilege
	})

	return report
}

func evidenceFor(surface *TableSurface, p Privilege) []Evidence {
	var out []Evidence
	for _, e := range surface.Evidence {
		if e.Privilege == p {
			out = append(out, e)
			if len(out) >= 3 {
				break
			}
		}
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unionKeys(a, b map[string]PrivilegeSet) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

func boolMapKeys[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// isFullyCovered reports whether every privilege the derivation proved table
// needs (including the any-write-lock requirement) is present in BOTH
// hand-maintained lists (or, for column-scoped tables, at least granted --
// this tool cannot verify column-level correctness, see compare.go's
// column-scoped handling above).
func isFullyCovered(surface *TableSurface, gt *GroundTruth) bool {
	granted := gt.Grants[surface.Table]
	required := gt.RequiredTablePrivileges[surface.Table]
	for p := Privilege(0); p < numPrivileges; p++ {
		if !surface.Privileges.Has(p) {
			continue
		}
		// No DELETE special case: the posture expresses AllowDelete since
		// Phase 2 of the Option B role split, so DELETE is covered when both
		// lists carry it, exactly like the other privileges.
		if !granted.Has(p) || !required.Has(p) {
			return false
		}
	}
	if surface.LockRequirement != nil && !lockSatisfiedBy(*surface.LockRequirement, granted) {
		return false
	}
	return true
}

// lockSatisfiedBy reports whether held contains ANY ONE privilege the lock mode
// accepts. Single definition on purpose: three call sites checked this
// disjunction, and an inlined copy is how one of them ends up still accepting
// INSERT for a mode that does not.
func lockSatisfiedBy(requirement LockRequirement, held PrivilegeSet) bool {
	for p := Privilege(0); p < numPrivileges; p++ {
		if requirement.Satisfying.Has(p) && held.Has(p) {
			return true
		}
	}
	return false
}

func pluralTables(tables []string) string {
	if len(tables) == 1 {
		return "table"
	}
	return "tables"
}
