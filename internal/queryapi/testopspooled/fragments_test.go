package testopspooled

import (
	"strings"
	"testing"
)

var (
	repoOnly   = Scope{RepoIDsParam: "repo_ids"}
	repoWindow = Scope{RepoIDsParam: "repo_ids", WindowStartParam: "start", WindowEndParam: "end"}
)

func fragments() map[string]func(alias string, scope Scope) string {
	return map[string]func(alias string, scope Scope) string{
		"suite":    SuiteRowsNewestAttempt,
		"coverage": CoverageRowsNewestAttempt,
	}
}

func uses(name string) int {
	if name == "suite" {
		return SuiteScopeUses
	}
	return CoverageScopeUses
}

func tableOf(name string) string {
	if name == "suite" {
		return "test_suite_results"
	}
	return "coverage_snapshots"
}

// The scope lands in EVERY inner select, and the exported use counts are exact.
func TestScopeLandsInEveryInnerSelect(t *testing.T) {
	for name, build := range fragments() {
		for label, scope := range map[string]Scope{"repo": repoOnly, "repo+window": repoWindow} {
			sql := build("s", scope)
			if got := strings.Count(sql, "repo_id IN {repo_ids:Array(UUID)}"); got < uses(name) {
				t.Errorf("%s/%s: the repository bound occurs %d times, want at least %d (one per inner select)", name, label, got, uses(name))
			}
			segments := strings.Split(sql, "FROM "+tableOf(name)+" FINAL")
			want := uses(name)
			if name == "suite" && label == "repo+window" {
				want *= 2 // each window bound is itself a select over the suite table, bounded to the same repositories
			}
			if len(segments) != want+1 {
				t.Fatalf("%s/%s: %d selects over %s, want %d", name, label, len(segments)-1, tableOf(name), want)
			}
			for i, segment := range segments[1:] {
				head := segment
				if len(head) > 140 {
					head = head[:140]
				}
				if !strings.Contains(head, "org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}") {
					t.Errorf("%s/%s: inner select %d has no repository bound right after the organization predicate: %q", name, label, i+1, head)
				}
			}
			if label == "repo+window" {
				if got := strings.Count(sql, "run_id IN (SELECT run_id"); got != uses(name) {
					t.Errorf("%s: the window run-set bound occurs %d times, want %d (one per inner select)", name, got, uses(name))
				}
			}
			if label == "repo+window" {
				if got := strings.Count(sql, "{start:DateTime64(3, 'UTC')}"); got != uses(name) {
					t.Errorf("%s: the window start occurs %d times, want %d", name, got, uses(name))
				}
				if got := strings.Count(sql, "{end:DateTime64(3, 'UTC')}"); got != uses(name) {
					t.Errorf("%s: the window end occurs %d times, want %d", name, got, uses(name))
				}
			}
		}
	}
}

// An empty scope adds nothing, and the only bound parameter is the organization.
func TestEmptyScopeBindsOnlyTheOrganization(t *testing.T) {
	for name, build := range fragments() {
		sql := build("", Scope{})
		if strings.Count(sql, "{") != strings.Count(sql, "{org_id:String}") {
			t.Errorf("%s: a parameter other than {org_id:String} appears:\n%s", name, sql)
		}
		if strings.Contains(sql, "repo_id IN {") {
			t.Errorf("%s: an empty scope bound the repositories:\n%s", name, sql)
		}
	}
}

// The alias qualifies the outer columns only; the inner selects read the table unqualified.
func TestAliasQualifiesTheOuterColumnsOnly(t *testing.T) {
	for name, build := range fragments() {
		sql := build("a", repoOnly)
		for _, column := range []string{"a.report_slot", "a.repo_id", "a.run_id", "a.run_attempt"} {
			if !strings.Contains(sql, column) {
				t.Errorf("%s: %s is not qualified", name, column)
			}
		}
		qualified := 0
		for _, column := range []string{"a.report_slot", "a.repo_id", "a.run_id", "a.run_attempt", "a.suite_id", "a.snapshot_id"} {
			qualified += strings.Count(sql, column)
		}
		if strings.Count(sql, "a.") != qualified {
			t.Errorf("%s: the alias is used for something other than the outer columns:\n%s", name, sql)
		}
		if bare := build("", Scope{}); strings.Contains(bare, ".") {
			t.Errorf("%s: an unqualified fragment holds a qualified name:\n%s", name, bare)
		}
	}
}

// The legacy arm names the persisted id arrays and identity columns of its table.
func TestLegacyArmMatchesByPersistedIdentity(t *testing.T) {
	if sql := SuiteRowsNewestAttempt("", Scope{}); !strings.Contains(sql, "(repo_id, run_id, suite_id) NOT IN (SELECT repo_id, run_id, arrayJoin(legacy_suite_ids)") {
		t.Errorf("the suite legacy arm is not the identity match:\n%s", sql)
	}
	// Coverage keeps a legacy row only in a run with no slotted coverage row (CHAOS-14146), not by its listed id.
	if sql := CoverageRowsNewestAttempt("", Scope{}); !strings.Contains(sql, "(report_slot = '' AND (repo_id, run_id) NOT IN (SELECT repo_id, run_id FROM coverage_snapshots FINAL WHERE org_id = {org_id:String} AND report_slot != ''))") ||
		strings.Contains(sql, "legacy_snapshot_ids") {
		t.Errorf("the coverage legacy arm is not the run rule:\n%s", sql)
	}
	// The ids come from KEPT slotted rows only: the legacy-id select is itself bounded to the newest attempt of each slot.
	sql := SuiteRowsNewestAttempt("", Scope{})
	i := strings.Index(sql, "arrayJoin(legacy_suite_ids)")
	if i < 0 || !strings.Contains(sql[i:], "(repo_id, run_id, report_slot, run_attempt) IN (SELECT repo_id, run_id, report_slot, max(run_attempt)") {
		t.Errorf("the legacy ids are not taken from the newest slotted attempt only:\n%s", sql)
	}
}

// Hostile names are refused: the scope holds parameter names only.
func TestHostileAliasAndScopeNamesPanic(t *testing.T) {
	mustPanic := func(what string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", what)
			}
		}()
		f()
	}
	for _, alias := range []string{"a b", "a;b", "a.b", "1a", "a'"} {
		mustPanic("alias "+alias, func() { SuiteRowsNewestAttempt(alias, Scope{}) })
	}
	for _, name := range []string{"repo ids", "repo_ids}; DROP", "Repo", "1repo", "repo'", "a.b", "repo-ids"} {
		mustPanic("repo param "+name, func() { CoverageRowsNewestAttempt("", Scope{RepoIDsParam: name}) })
		mustPanic("start param "+name, func() {
			SuiteRowsNewestAttempt("", Scope{RepoIDsParam: "r", WindowStartParam: name, WindowEndParam: "end"})
		})
	}
	mustPanic("window with only a start", func() { SuiteRowsNewestAttempt("", Scope{RepoIDsParam: "r", WindowStartParam: "start"}) })
	mustPanic("window without a repository list", func() { SuiteRowsNewestAttempt("", Scope{WindowStartParam: "s", WindowEndParam: "e"}) })
	mustPanic("source without names", func() { SuiteWindowSource("", "s", "e") })
}

// SuiteWindowSource is byte-equal to the text the daily readers carried before it moved here (the zero-row source check reads
// the same text): the move changed no byte.
func TestSuiteWindowSourceIsTheDailyReadersText(t *testing.T) {
	const before = `
FROM test_suite_results FINAL
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND coalesce(started_at, finished_at) >= {start:DateTime64(3, 'UTC')}
  AND coalesce(started_at, finished_at) < {end:DateTime64(3, 'UTC')}`
	if got := SuiteWindowSource("repo_ids", "start", "end"); got != before {
		t.Fatalf("the moved window source differs from the daily readers' text:\n%s\nwant\n%s", got, before)
	}
}
