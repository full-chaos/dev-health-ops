package teamscope

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

var asOf = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// TestRepoConditionIsABareBoolean pins the contract every caller relies on
// when splicing: no leading "AND", no trailing statement, and the caller's
// own column on the left.
func TestRepoConditionIsABareBoolean(t *testing.T) {
	condition, _ := RepoCondition("org-1", "work_unit_investments.repo_id", []string{"ABC-123"}, asOf)
	if strings.HasPrefix(strings.TrimSpace(condition), "AND") {
		t.Fatalf("condition leads with AND, which every caller adds itself:\n%s", condition)
	}
	if strings.Contains(condition, ";") {
		t.Fatalf("condition carries a statement terminator:\n%s", condition)
	}
	if !strings.HasPrefix(condition, "work_unit_investments.repo_id IN (") {
		t.Fatalf("condition does not test the caller's own column:\n%s", condition)
	}
}

// TestRepoConditionBindsOrgTeamsAndInstant pins the three bindings, their
// prefixed names (a caller already binds org_id and scope_ids of its own),
// and that blank team ids are dropped.
func TestRepoConditionBindsOrgTeamsAndInstant(t *testing.T) {
	condition, bindings := RepoCondition("org-1", "repo_id", []string{"ABC-123", "", "DEF-456"}, asOf)
	if condition == "" {
		t.Fatal("condition is empty for a non-empty team list")
	}
	values := map[string]any{}
	for _, binding := range bindings {
		values[binding.Name] = binding.Value
	}
	if len(values) != 3 {
		t.Fatalf("bindings = %+v, want exactly three", bindings)
	}
	if got := values[BindingOrgID]; got != "org-1" {
		t.Errorf("%s = %v, want org-1", BindingOrgID, got)
	}
	if got := fmt.Sprint(values[BindingTeamIDs]); got != "[ABC-123 DEF-456]" {
		t.Errorf("%s = %v, want [ABC-123 DEF-456] (a blank id is dropped)", BindingTeamIDs, got)
	}
	if got := values[BindingAsOf]; got != asOf {
		t.Errorf("%s = %v, want %v", BindingAsOf, got, asOf)
	}
	for _, name := range []string{"org_id", "scope_ids", "repo_ids", "start_date", "end_date"} {
		if _, clash := values[name]; clash {
			t.Errorf("binding %q collides with a name callers already bind", name)
		}
	}
}

// TestRepoConditionEmptyTeamsIsNoCondition pins the empty-input contract: no
// team ids means the CALLER has no team scope to apply. It never means "this
// team owns nothing", which is a real answer the condition itself returns.
func TestRepoConditionEmptyTeamsIsNoCondition(t *testing.T) {
	for _, teamIDs := range [][]string{nil, {}, {""}, {"", ""}} {
		condition, bindings := RepoCondition("org-1", "repo_id", teamIDs, asOf)
		if condition != "" || bindings != nil {
			t.Fatalf("RepoCondition(%q) = (%q, %+v), want no condition and no bindings", teamIDs, condition, bindings)
		}
	}
}

// TestRepoConditionReadsBothTablesFinal pins the dedup requirement. A
// revocation is a replacement row under the same sorting key, so a raw read
// sees the stale open version beside it and readmits a revoked repository;
// repos carries the same hazard for a renamed or re-synced row.
func TestRepoConditionReadsBothTablesFinal(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	if !strings.Contains(condition, "FROM team_repo_ownership AS o FINAL") {
		t.Errorf("team_repo_ownership is read without FINAL:\n%s", condition)
	}
	if strings.Contains(condition, "FROM repos\n") || !strings.Contains(condition, "FROM repos FINAL") {
		t.Errorf("repos is read without FINAL:\n%s", condition)
	}
}

// TestRepoConditionScopesEveryReadByOrg pins that the org predicate sits in
// the SAME statement as each FINAL source rather than narrowing afterwards,
// the convention every reader in this binary follows for a tenant filter.
func TestRepoConditionScopesEveryReadByOrg(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	depths := sqlshape.Depths(condition)
	orgPredicate := fmt.Sprintf("org_id = {%s:String}", BindingOrgID)

	sources := indexesOf(condition, "FROM repos FINAL")
	sources = append(sources, indexesOf(condition, "FROM team_repo_ownership AS o FINAL")...)
	predicates := indexesOf(condition, orgPredicate)
	if len(sources) != 3 {
		t.Fatalf("expected three reads (the join's repos, the ownership table, the catalog guard), found %d:\n%s", len(sources), condition)
	}
	if len(predicates) != 3 {
		t.Fatalf("expected an org predicate per read, found %d:\n%s", len(predicates), condition)
	}
	for _, source := range sources {
		matched := false
		for _, predicate := range predicates {
			if depths[predicate] == depths[source] {
				matched = true
			}
		}
		if !matched {
			t.Errorf("a read at depth %d carries no org predicate at its own depth:\n%s", depths[source], condition)
		}
	}
}

// TestRepoConditionResolvesANullRepoIDByName pins the read-time resolution
// the GitHub team-autoimport writer depends on: it writes every row with no
// repo_id, carrying only repo_full_name. It also pins the `matched` sentinel
// rather than a NULL test, because ClickHouse fills an unmatched LEFT JOIN
// column with the type's ZERO VALUE and a zero UUID is not NULL.
//
// This is the ONLY place the sentinel is pinned. The catalog-membership
// check rejects the zero UUID too, so swapping the sentinel for `r.id IS NOT
// NULL` changes no seeded result and every behavioural test stays green --
// see RepoCondition's own doc comment on the overlap.
func TestRepoConditionResolvesANullRepoIDByName(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	if !strings.Contains(condition, "coalesce(toString(o.repo_id), toString(r.id))") {
		t.Errorf("condition does not resolve a NULL repo_id through the name join:\n%s", condition)
	}
	if !strings.Contains(condition, "lower(r.repo) = lower(o.repo_full_name)") {
		t.Errorf("the name join is case-sensitive:\n%s", condition)
	}
	if !strings.Contains(condition, "1 AS matched") || !strings.Contains(condition, "r.matched = 1") {
		t.Errorf("condition does not carry the matched sentinel:\n%s", condition)
	}
	if strings.Contains(condition, "r.id IS NOT NULL") {
		t.Errorf("condition tests r.id IS NOT NULL, which is true for the zero UUID an unmatched join fills in:\n%s", condition)
	}
}

// TestRepoConditionWindowsOnTheBoundInstant pins that both ends of the
// validity window read the one bound instant, never now64() inline: two
// reads inside one response must resolve the same membership. It also pins
// the parameter's zone: valid_from/valid_to are DateTime64(3, 'UTC'), and a
// parameter naming no zone is parsed in the server's, which shifts the whole
// window by that server's offset.
func TestRepoConditionWindowsOnTheBoundInstant(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	instant := fmt.Sprintf("{%s:DateTime64(3, 'UTC')}", BindingAsOf)
	if !strings.Contains(condition, "o.valid_from <= "+instant) {
		t.Errorf("valid_from is not tested against the bound instant:\n%s", condition)
	}
	if !strings.Contains(condition, "o.valid_to IS NULL OR o.valid_to > "+instant) {
		t.Errorf("valid_to is not tested against the bound instant:\n%s", condition)
	}
	if strings.Contains(condition, "now64") || strings.Contains(condition, "now()") {
		t.Errorf("condition resolves its own instant instead of the caller's:\n%s", condition)
	}
	if strings.Contains(condition, fmt.Sprintf("{%s:DateTime64(3)}", BindingAsOf)) {
		t.Errorf("the instant is bound with no timezone, so the server's own zone decides the window:\n%s", condition)
	}
}

// TestRepoConditionRequiresTheRepositoryInTheCatalog pins the guard against
// an ownership row that outlived its repository's removal: ClickHouse
// enforces no foreign key, so without this a team scope alone could still
// read a repository every org-scoped read correctly does not see.
func TestRepoConditionRequiresTheRepositoryInTheCatalog(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	guard := "coalesce(toString(o.repo_id), toString(r.id)) IN ("
	if !strings.Contains(condition, guard) {
		t.Fatalf("condition does not require the resolved id to be in the catalog:\n%s", condition)
	}
	depths := sqlshape.Depths(condition)
	guardIdx := strings.Index(condition, guard)
	ownershipIdx := strings.Index(condition, "FROM team_repo_ownership AS o FINAL")
	if depths[guardIdx] != depths[ownershipIdx] {
		t.Fatalf("the catalog guard sits at depth %d and the ownership read at %d -- it must narrow that read, not a different one:\n%s",
			depths[guardIdx], depths[ownershipIdx], condition)
	}
}

// TestRepoConditionRanksNothing pins the membership-not-precedence decision:
// a repository two teams both work in belongs to both of them here. Ranking
// it down to one owner deletes a team's real signal.
func TestRepoConditionRanksNothing(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	for _, ranking := range []string{"is_primary", "specificity", "priority", "ORDER BY", "LIMIT"} {
		if strings.Contains(condition, ranking) {
			t.Errorf("condition consults %q -- membership never ranks competing claims:\n%s", ranking, condition)
		}
	}
	for _, narrowing := range []string{"match_type", "o.source"} {
		if strings.Contains(condition, narrowing) {
			t.Errorf("condition narrows by %q -- every source and match type counts:\n%s", narrowing, condition)
		}
	}
}

// TestRepoConditionStartsNoStatement pins that this is spliceable into a
// read-only client's statement: the client rejects anything whose first
// token is not SELECT, so the condition must never lead with WITH.
func TestRepoConditionStartsNoStatement(t *testing.T) {
	condition, _ := RepoCondition("org-1", "repo_id", []string{"ABC-123"}, asOf)
	if strings.HasPrefix(strings.TrimSpace(condition), "WITH") || strings.HasPrefix(strings.TrimSpace(condition), "SELECT") {
		t.Fatalf("condition is a statement, not a boolean:\n%s", condition)
	}
}

func indexesOf(haystack, needle string) []int {
	var found []int
	for offset := 0; ; {
		index := strings.Index(haystack[offset:], needle)
		if index < 0 {
			return found
		}
		found = append(found, offset+index)
		offset += index + len(needle)
	}
}
