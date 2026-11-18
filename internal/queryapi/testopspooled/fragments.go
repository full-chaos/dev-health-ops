package testopspooled

import (
	"fmt"
	"regexp"
)

// SuiteScopeUses and CoverageScopeUses are the number of inner selects that carry the caller's Scope. Coverage has 2: its
// legacy arm is one run-set select, not the listed-id select and its newest-attempt select (CHAOS-14146).
const (
	SuiteScopeUses    = 3
	CoverageScopeUses = 2
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
}

func suiteWindowRuns(repo, start, end string) string {
	return "SELECT run_id" + SuiteWindowSource(repo, start, end)
}

// SuiteRowsNewestAttempt is the boolean SQL expression that keeps the suite rows of a run the readers count: the newest attempt
// of each report slot, and a legacy row (no slot) only when no kept slotted row lists its id in legacy_suite_ids. alias names the
// table in the caller's statement ("" = unqualified columns); the inner selects read the table unqualified. It binds
// {org_id:String} and the parameters named by scope.
func SuiteRowsNewestAttempt(alias string, scope Scope) string {
	return rowsNewestAttempt(attemptTable{"test_suite_results", "suite_id", "legacy_suite_ids", suiteWindowRuns, false}, alias, scope)
}

// CoverageRowsNewestAttempt keeps the newest attempt of each coverage slot, and a legacy row only when its run has no slotted
// coverage row (CHAOS-14146): every current writer slots coverage, so such a run was read again, and a legacy row the re-read
// did not list is an older copy (the daily read serves one snapshot per repo-day, so it must never win). Its window bound is
// the runs of ci_pipeline_runs started in the window.
func CoverageRowsNewestAttempt(alias string, scope Scope) string {
	return rowsNewestAttempt(attemptTable{"coverage_snapshots", "snapshot_id", "legacy_snapshot_ids", pipelineWindowRuns, true}, alias, scope)
}

func rowsNewestAttempt(t attemptTable, alias string, scope Scope) string {
	if alias != "" && !aliasPattern.MatchString(alias) {
		panic(fmt.Sprintf("testopspooled: alias %q is not an identifier", alias))
	}
	scope.validate()
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}
	bound := scope.repoPredicate()
	if scope.WindowStartParam != "" {
		bound += " AND run_id IN (" + t.windowRuns(scope.RepoIDsParam, scope.WindowStartParam, scope.WindowEndParam) + ")"
	}
	newest := fmt.Sprintf("SELECT repo_id, run_id, report_slot, max(run_attempt) FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' GROUP BY repo_id, run_id, report_slot", t.table, bound)
	slotted := fmt.Sprintf("(%s != '' AND (%s, %s, %s, %s) IN (%s))",
		column("report_slot"), column("repo_id"), column("run_id"), column("report_slot"), column("run_attempt"), newest)
	legacy := fmt.Sprintf("(%s = '' AND (%s, %s, %s) NOT IN (SELECT repo_id, run_id, arrayJoin(%s) FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (%s)))",
		column("report_slot"), column("repo_id"), column("run_id"), column(t.id), t.legacyIDs, t.table, bound, newest)
	if t.legacyByRun {
		legacy = fmt.Sprintf("(%s = '' AND (%s, %s) NOT IN (SELECT repo_id, run_id FROM %s FINAL WHERE org_id = {org_id:String}%s AND report_slot != ''))",
			column("report_slot"), column("repo_id"), column("run_id"), t.table, bound)
	}
	return "(" + slotted + " OR " + legacy + ")"
}
