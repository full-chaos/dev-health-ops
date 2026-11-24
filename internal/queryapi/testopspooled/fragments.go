package testopspooled

import (
	"fmt"
	"regexp"
)

// SuiteScopeUses, NotRetiredScopeUses and CoverageScopeUses are the number of inner selects that carry the caller's Scope. Suites
// have 6: the newest-attempt select twice, the legacy-id select, the retired-slot select, and the retired slots' legacy-id select
// with its own retired-slot select (CHAOS-14259). The not-retired rule has 3: the retired-slot select and the retired slots'
// legacy-id select with its own. Coverage has 2: its legacy arm is one run-set select, not the listed-id select and its
// newest-attempt select (CHAOS-14146), and it has no retired slot.
const (
	SuiteScopeUses      = 6
	NotRetiredScopeUses = 3
	CoverageScopeUses   = 2
)

var (
	aliasPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	paramPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// Scope bounds the inner selects of a fragment to a repository list and a window of runs. It holds the NAMES of bound server
// parameters only: {<RepoIDsParam>:Array(UUID)}, {<WindowStartParam>:DateTime64(3, 'UTC')} and
// {<WindowEndParam>:DateTime64(3, 'UTC')}; the predicates themselves are rendered here, so a caller never passes SQL. The zero
// value reads the organization. The window needs both names and a repository list.
type Scope struct {
	RepoIDsParam     string
	WindowStartParam string
	WindowEndParam   string
}

func (s Scope) validate() {
	for _, name := range []string{s.RepoIDsParam, s.WindowStartParam, s.WindowEndParam} {
		if name != "" && !paramPattern.MatchString(name) {
			panic(fmt.Sprintf("testopspooled: parameter name %q is not a lower-case identifier", name))
		}
	}
	if (s.WindowStartParam == "") != (s.WindowEndParam == "") {
		panic("testopspooled: a window scope needs both its start and its end parameter names")
	}
	if s.WindowStartParam != "" && s.RepoIDsParam == "" {
		panic("testopspooled: a window scope needs a repository list parameter name")
	}
}

func (s Scope) repoPredicate() string {
	if s.RepoIDsParam == "" {
		return ""
	}
	return " AND repo_id IN {" + s.RepoIDsParam + ":Array(UUID)}"
}

// SuiteWindowSource is the source read of the suite rows whose start (or, without one, finish) falls in the window of one
// repository list: the daily readers' source and, as a run-id sub-select, the window bound of the suite fragment. One text.
func SuiteWindowSource(repoIDsParam, startParam, endParam string) string {
	Scope{RepoIDsParam: repoIDsParam, WindowStartParam: startParam, WindowEndParam: endParam}.validate()
	if repoIDsParam == "" || startParam == "" {
		panic("testopspooled: SuiteWindowSource needs all three parameter names")
	}
	return "\nFROM test_suite_results FINAL\nWHERE org_id = {org_id:String} AND repo_id IN {" + repoIDsParam + ":Array(UUID)}\n" +
		"  AND coalesce(started_at, finished_at) >= {" + startParam + ":DateTime64(3, 'UTC')}\n" +
		"  AND coalesce(started_at, finished_at) < {" + endParam + ":DateTime64(3, 'UTC')}"
}

// pipelineWindowRuns is the coverage window bound: the runs started in the window.
func pipelineWindowRuns(repoIDsParam, startParam, endParam string) string {
	return "SELECT run_id FROM ci_pipeline_runs FINAL WHERE org_id = {org_id:String} AND repo_id IN {" + repoIDsParam + ":Array(UUID)}" +
		" AND started_at >= {" + startParam + ":DateTime64(3, 'UTC')} AND started_at < {" + endParam + ":DateTime64(3, 'UTC')}"
}

type attemptTable struct {
	table, id, legacyIDs string
	windowRuns           func(repo, start, end string) string
	// legacyByRun keeps a legacy row only when its run has no slotted row, instead of when no kept slotted row lists its id.
	legacyByRun bool
	// retractable: the table holds the retracted flag (CHAOS-14259). A slot whose newest row of its newest attempt is retracted
	// is retired, and a retracted row is never kept.
	retractable bool
}

func suiteWindowRuns(repo, start, end string) string {
	return "SELECT run_id" + SuiteWindowSource(repo, start, end)
}

// SuiteRowsNewestAttempt is the boolean SQL expression that keeps the suite rows of a run the readers count: the newest attempt
// of each report slot, and a legacy row (no slot) only when no kept slotted row lists its id in legacy_suite_ids. alias names the
// table in the caller's statement ("" = unqualified columns); the inner selects read the table unqualified. It binds
// {org_id:String} and the parameters named by scope.
func SuiteRowsNewestAttempt(alias string, scope Scope) string {
	return rowsNewestAttempt(suiteTable, alias, scope)
}

var suiteTable = attemptTable{table: "test_suite_results", id: "suite_id", legacyIDs: "legacy_suite_ids", windowRuns: suiteWindowRuns, retractable: true}

// SuiteRowsNotRetired is the boolean SQL expression that keeps the suite rows of every attempt except a retracted row, the rows
// of a retired slot, and a pre-slot copy of a retired slot (a legacy row whose id any row of the slot lists, at any attempt)
// (CHAOS-14259): the suite subselects of the case reads, which keep the older attempts of a kept slot and the pre-slot rows of
// a kept slot for the flake measures. alias names the table in the caller's statement ("" = unqualified columns); the inner
// selects read the table unqualified. It binds {org_id:String} and the parameters named by scope, which must bound the runs of
// the rows it tests.
func SuiteRowsNotRetired(alias string, scope Scope) string {
	column, bound := outerColumns(alias, scope), suiteTable.bound(scope)
	return fmt.Sprintf("(%s = 0 AND ((%s = '' AND (%s, %s, %s) NOT IN (%s)) OR (%s != '' AND (%s, %s, %s) NOT IN (%s))))", column("retracted"),
		column("report_slot"), column("repo_id"), column("run_id"), column("suite_id"), retiredLegacyIDs(suiteTable, bound),
		column("report_slot"), column("repo_id"), column("run_id"), column("report_slot"), retiredSlots(suiteTable, bound))
}

// retiredSlots names the slots whose newest row of the newest attempt is a retraction row (CHAOS-14259). One scan: the slot NAME
// groups every attempt, and argMax takes the row of the highest (run_attempt, last_synced); at equal last_synced across keys of
// one slot the retraction row wins.
func retiredSlots(t attemptTable, bound string) string {
	return fmt.Sprintf("SELECT repo_id, run_id, report_slot FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' GROUP BY repo_id, run_id, report_slot HAVING argMax(retracted, (run_attempt, last_synced, retracted)) = 1", t.table, bound)
}

// retiredLegacyIDs names the pre-slot keys that any row of a retired slot lists, at any attempt (CHAOS-14259): a legacy copy of a
// retired slot is retired with it, also when only an older attempt of the slot lists its id.
func retiredLegacyIDs(t attemptTable, bound string) string {
	return fmt.Sprintf("SELECT repo_id, run_id, arrayJoin(%s) FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' AND (repo_id, run_id, report_slot) IN (%s)",
		t.legacyIDs, t.table, bound, retiredSlots(t, bound))
}

// RevivedSlots is the statement that counts the report slots a later sync brought back after their retirement (CHAOS-14555): a
// slot that holds a retraction row and whose newest row of its newest attempt is live again. The cases of its retired attempts
// stay hidden (their case rows were retracted at their keys), so the daily read makes the revival loud. It binds {org_id:String}
// and the parameters named by scope.
func RevivedSlots(scope Scope) string {
	scope.validate()
	return fmt.Sprintf("SELECT count() FROM (SELECT repo_id, run_id, report_slot FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' GROUP BY repo_id, run_id, report_slot HAVING max(retracted) = 1 AND argMax(retracted, (run_attempt, last_synced, retracted)) = 0)",
		suiteTable.table, suiteTable.bound(scope))
}

// outerColumns validates alias and scope and returns the namer of the outer columns.
func outerColumns(alias string, scope Scope) func(string) string {
	if alias != "" && !aliasPattern.MatchString(alias) {
		panic(fmt.Sprintf("testopspooled: alias %q is not an identifier", alias))
	}
	scope.validate()
	return func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}
}

// bound is the predicate every inner select of t carries: the repository list and, with a window, the window's runs.
func (t attemptTable) bound(scope Scope) string {
	bound := scope.repoPredicate()
	if scope.WindowStartParam != "" {
		bound += " AND run_id IN (" + t.windowRuns(scope.RepoIDsParam, scope.WindowStartParam, scope.WindowEndParam) + ")"
	}
	return bound
}

// CoverageRowsNewestAttempt keeps the newest attempt of each coverage slot, and a legacy row only when its run has no slotted
// coverage row (CHAOS-14146): every current writer slots coverage, so such a run was read again, and a legacy row the re-read
// did not list is an older copy (the daily read serves one snapshot per repo-day, so it must never win). Its window bound is
// the runs of ci_pipeline_runs started in the window.
func CoverageRowsNewestAttempt(alias string, scope Scope) string {
	return rowsNewestAttempt(attemptTable{table: "coverage_snapshots", id: "snapshot_id", legacyIDs: "legacy_snapshot_ids", windowRuns: pipelineWindowRuns, legacyByRun: true}, alias, scope)
}

func rowsNewestAttempt(t attemptTable, alias string, scope Scope) string {
	column, bound := outerColumns(alias, scope), t.bound(scope)
	newest := fmt.Sprintf("SELECT repo_id, run_id, report_slot, max(run_attempt) FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' GROUP BY repo_id, run_id, report_slot", t.table, bound)
	retired := ""
	if t.retractable {
		// After the newest attempt is chosen: a retracted newest attempt still hides the older attempts of its slot, and its
		// legacy ids (the legacy arm reads `newest`) still hide the pre-slot copy.
		retired = fmt.Sprintf(" AND %s = 0 AND (%s, %s, %s) NOT IN (%s)", column("retracted"),
			column("repo_id"), column("run_id"), column("report_slot"), retiredSlots(t, bound))
	}
	slotted := fmt.Sprintf("(%s != '' AND (%s, %s, %s, %s) IN (%s)%s)",
		column("report_slot"), column("repo_id"), column("run_id"), column("report_slot"), column("run_attempt"), newest, retired)
	retiredLegacy := ""
	if t.retractable {
		retiredLegacy = fmt.Sprintf(" AND (%s, %s, %s) NOT IN (%s)", column("repo_id"), column("run_id"), column(t.id), retiredLegacyIDs(t, bound))
	}
	legacy := fmt.Sprintf("(%s = '' AND (%s, %s, %s) NOT IN (SELECT repo_id, run_id, arrayJoin(%s) FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (%s))%s)",
		column("report_slot"), column("repo_id"), column("run_id"), column(t.id), t.legacyIDs, t.table, bound, newest, retiredLegacy)
	if t.legacyByRun {
		legacy = fmt.Sprintf("(%s = '' AND (%s, %s) NOT IN (SELECT repo_id, run_id FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != ''))",
			column("report_slot"), column("repo_id"), column("run_id"), t.table, bound)
	}
	return "(" + slotted + " OR " + legacy + ")"
}
