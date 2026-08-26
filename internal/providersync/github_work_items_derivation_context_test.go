package providersync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

type fakeGitHubWorkItemDerivationContextSource struct {
	facts   githubWorkItemDerivationFacts
	err     error
	called  bool
	claim   Claim
	request githubWorkItemDerivationLoadRequest

	// CHAOS-3978 stored-edge half.
	storedEdges          []githubWorkItemDependencyRow
	storedEdgeErr        error
	storedEdgeCalls      int
	storedEdgeSubjectIDs []string
	storedEdgeClaim      Claim
}

type recordingGitHubWorkItemDerivationConn struct {
	driver.Conn
	queries []string
	args    [][]any
}

func (conn *recordingGitHubWorkItemDerivationConn) Query(
	_ context.Context, query string, args ...any,
) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	conn.args = append(conn.args, append([]any(nil), args...))
	return emptyGitHubWorkItemDerivationRows{}, nil
}

type emptyGitHubWorkItemDerivationRows struct{}

func (emptyGitHubWorkItemDerivationRows) Next() bool                       { return false }
func (emptyGitHubWorkItemDerivationRows) Scan(...any) error                { return nil }
func (emptyGitHubWorkItemDerivationRows) ScanStruct(any) error             { return nil }
func (emptyGitHubWorkItemDerivationRows) ColumnTypes() []driver.ColumnType { return nil }
func (emptyGitHubWorkItemDerivationRows) Totals(...any) error              { return nil }
func (emptyGitHubWorkItemDerivationRows) Columns() []string                { return nil }
func (emptyGitHubWorkItemDerivationRows) Close() error                     { return nil }
func (emptyGitHubWorkItemDerivationRows) Err() error                       { return nil }
func (emptyGitHubWorkItemDerivationRows) HasData() bool                    { return false }

func (source *fakeGitHubWorkItemDerivationContextSource) Load(
	_ context.Context,
	claim Claim,
	request githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	source.called = true
	source.claim = claim
	source.request = request
	return source.facts, source.err
}

func (source *fakeGitHubWorkItemDerivationContextSource) LoadStoredInheritableEdges(
	_ context.Context,
	claim Claim,
	sourceWorkItemIDs []string,
) ([]githubWorkItemDependencyRow, error) {
	source.storedEdgeCalls++
	source.storedEdgeClaim = claim
	source.storedEdgeSubjectIDs = append([]string(nil), sourceWorkItemIDs...)
	if source.storedEdgeErr != nil {
		return nil, source.storedEdgeErr
	}
	return append([]githubWorkItemDependencyRow(nil), source.storedEdges...), nil
}

func TestLoadGitHubWorkItemDerivationContextBoundsDonorQueryToLiveInheritableEdges(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim := githubWorkItemOracleClaim()
	source := &fakeGitHubWorkItemDerivationContextSource{}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: "gh:acme/api#1", Provider: "github", OrgID: claim.OrgID,
		}},
		Dependencies: []githubWorkItemDependencyRow{
			{SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: "extkey:stale-1", RelationshipType: "external_issue_key", LastSynced: now.Add(-time.Minute), OrgID: claim.OrgID},
			{SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: "extkey:stale-1", RelationshipType: "blocks", LastSynced: now, OrgID: claim.OrgID},
			{SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: " extkey:proj-2 ", RelationshipType: "duplicates", LastSynced: now, OrgID: claim.OrgID},
			{SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: "linear:TEAM-8", RelationshipType: "relates_to", LastSynced: now, OrgID: claim.OrgID},
			{SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: "jira:IGNORED-3", RelationshipType: "blocked_by", LastSynced: now, OrgID: claim.OrgID},
		},
	}

	_, err := loadGitHubWorkItemDerivationContext(context.Background(), claim, rows, source, now)
	if err != nil {
		t.Fatal(err)
	}
	if !source.called || source.claim.OrgID != claim.OrgID || source.claim.Provider != "github" {
		t.Fatalf("loader did not receive the exact tenant claim: called=%t claim=%+v", source.called, source.claim)
	}
	if !source.request.AsOf.Equal(now) {
		t.Fatalf("as-of = %s, want %s", source.request.AsOf, now)
	}
	if want := []string{"linear:TEAM-8"}; !reflect.DeepEqual(source.request.DonorWorkItemIDs, want) {
		t.Fatalf("donor ids = %#v, want %#v", source.request.DonorWorkItemIDs, want)
	}
	if want := []string{"PROJ-2"}; !reflect.DeepEqual(source.request.DonorIssueKeys, want) {
		t.Fatalf("donor keys = %#v, want %#v", source.request.DonorIssueKeys, want)
	}
}

func TestLoadGitHubWorkItemDerivationContextRejectsForeignTenantBeforeLoading(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	source := &fakeGitHubWorkItemDerivationContextSource{}
	_, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim,
		githubWorkItemRows{Dependencies: []githubWorkItemDependencyRow{{
			SourceWorkItemID: "gh:acme/api#1", TargetWorkItemID: "linear:CHAOS-1",
			RelationshipType: "relates_to", OrgID: "org-other",
		}}},
		source, time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("error = %v, want ErrInvalidScope", err)
	}
	if source.called {
		t.Fatal("foreign-tenant dependency reached the context loader")
	}
}

func TestGitHubWorkItemDerivationPreservesPrecedenceAndProvenance(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim := githubWorkItemOracleClaim()
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	repoIDText := repoID.String()
	donorProject := "linear-project-1"
	source := &fakeGitHubWorkItemDerivationContextSource{facts: githubWorkItemDerivationFacts{
		Projects: []githubWorkItemDerivationProjectFact{{
			Provider: "linear", TeamID: "team-linked", TeamName: "Linked Team",
			ProjectID: donorProject, IsPrimary: 1, Specificity: 80, UpdatedAt: now,
		}},
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoIDText, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-member", TeamName: "Member Team",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
		ManualFallbacks: []githubWorkItemDerivationManualFallback{{
			Provider: "github", ScopeType: "repo", ScopeID: repoIDText,
			TeamID: "team-manual", TeamName: "Manual Team", Priority: 100,
		}},
		DonorItems: []githubWorkItemDerivationSubject{{
			WorkItemID: "linear:CHAOS-42", Provider: "linear", ProjectID: &donorProject,
			OrgID: claim.OrgID,
		}},
	}}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: "gh:acme/api#7", Provider: "github", RepoID: &repoID,
			ProjectID: githubWorkItemDerivationStringPointer("acme/api"),
			Assignees: []string{"DEV@EXAMPLE.COM"}, OrgID: claim.OrgID,
		}},
		Dependencies: []githubWorkItemDependencyRow{{
			SourceWorkItemID: "gh:acme/api#7", TargetWorkItemID: "extkey:CHAOS-42",
			RelationshipType: "external_issue_key", LastSynced: now, OrgID: claim.OrgID,
		}},
	}

	derived, err := loadGitHubWorkItemDerivationContext(context.Background(), claim, rows, source, now)
	if err != nil {
		t.Fatal(err)
	}
	teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(rows.WorkItems[0]))
	if got := githubWorkItemDerivationStringValue(teamID); got != "team-repo" {
		t.Fatalf("primary team id = %q, want team-repo", got)
	}
	if got := githubWorkItemDerivationStringValue(teamName); got != "Repository Team" {
		t.Fatalf("primary team name = %q, want Repository Team", got)
	}

	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	repo := bySource["repo_ownership"]
	if repo.IsPrimary != 1 || repo.Confidence != "high" || repo.Evidence != "repo_ownership="+repoIDText {
		t.Fatalf("repo provenance = %+v", repo)
	}
	for _, lower := range []string{"assignee_membership", "linked_issue", "manual_fallback"} {
		candidate, exists := bySource[lower]
		if !exists || candidate.IsPrimary != 0 {
			t.Fatalf("lower-precedence %s candidate = %+v exists=%t", lower, candidate, exists)
		}
	}
	if linked := bySource["linked_issue"]; githubWorkItemDerivationStringValue(linked.TeamID) != "team-linked" || linked.Confidence != "medium" || linked.Evidence != "linked_issue=gh:acme/api#7" {
		t.Fatalf("linked provenance = %+v", linked)
	}
}

func TestGitHubWorkItemDerivationDoesNotLaunderManualDonor(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim := githubWorkItemOracleClaim()
	source := &fakeGitHubWorkItemDerivationContextSource{facts: githubWorkItemDerivationFacts{
		ManualFallbacks: []githubWorkItemDerivationManualFallback{{
			ScopeType: "issue_key_prefix", ScopeID: "CHAOS", TeamID: "team-manual", TeamName: "Manual Team",
		}},
		DonorItems: []githubWorkItemDerivationSubject{{
			WorkItemID: "linear:CHAOS-77", Provider: "linear", OrgID: claim.OrgID,
		}},
	}}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{WorkItemID: "gh:acme/api#8", Provider: "github", OrgID: claim.OrgID}},
		Dependencies: []githubWorkItemDependencyRow{{
			SourceWorkItemID: "gh:acme/api#8", TargetWorkItemID: "extkey:CHAOS-77",
			RelationshipType: "external_issue_key", LastSynced: now, OrgID: claim.OrgID,
		}},
	}
	derived, err := loadGitHubWorkItemDerivationContext(context.Background(), claim, rows, source, now)
	if err != nil {
		t.Fatal(err)
	}
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(rows.WorkItems[0]))
	if teamID != nil || len(candidates) != 1 || candidates[0].Source != "unassigned" || candidates[0].IsPrimary != 1 {
		t.Fatalf("manual donor was inherited: team=%v candidates=%+v", teamID, candidates)
	}
}

func TestGitHubWorkItemDerivationTrimsManualScopeAndPreservesZeroPriority(t *testing.T) {
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		ManualFallbacks: []githubWorkItemDerivationManualFallback{{
			Provider: "github", ScopeType: "repo", ScopeID: "  " + repoID + "  ",
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "explicit", Priority: 0,
		}},
	})
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#manual", Provider: "github", RepoID: &repoID, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-manual" || len(candidates) != 1 {
		t.Fatalf("manual attribution = team %v candidates %+v", teamID, candidates)
	}
	candidate := candidates[0]
	if candidate.Source != "manual_fallback" || candidate.Priority != 0 ||
		candidate.Evidence != "manual_fallback:repo=  "+repoID+"   (explicit)" {
		t.Fatalf("manual provenance = %+v", candidate)
	}
}

func TestGitHubWorkItemDerivationManualFallbackTieBreakIsStable(t *testing.T) {
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	projectID := "acme/api"
	subject := githubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#manual-tie", Provider: "github",
		RepoID: &repoID, ProjectID: &projectID, OrgID: "org-acme",
	}
	rules := []githubWorkItemDerivationManualFallback{
		{
			Provider: "github", ScopeType: "repo", ScopeID: repoID,
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "repo rule", Priority: 0,
		},
		{
			Provider: "github", ScopeType: "project", ScopeID: projectID,
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "project rule", Priority: 0,
		},
	}
	resolve := func(input []githubWorkItemDerivationManualFallback) []githubWorkItemDerivationCandidate {
		t.Helper()
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ManualFallbacks: input,
		})
		_, _, candidates := derived.resolve(subject)
		return candidates
	}

	forward := resolve(rules)
	reversed := resolve([]githubWorkItemDerivationManualFallback{rules[1], rules[0]})
	if !reflect.DeepEqual(forward, reversed) {
		t.Fatalf("manual fallback order changed attribution:\nforward=%+v\nreversed=%+v", forward, reversed)
	}
	if len(forward) != 2 || forward[0].IsPrimary != 1 ||
		forward[0].Evidence != "manual_fallback:project=acme/api (project rule)" ||
		forward[1].IsPrimary != 0 {
		t.Fatalf("manual fallback deterministic ranking=%+v", forward)
	}
}

func TestGitHubWorkItemDerivationRequiresAnUnambiguousActualEdge(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	projectLinear := "linear-project"
	projectJira := "jira-project"
	source := githubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#9", Provider: "github", OrgID: "org-acme",
	}
	linearDonor := githubWorkItemDerivationSubject{
		WorkItemID: "linear:CHAOS-9", Provider: "linear", ProjectID: &projectLinear, OrgID: "org-acme",
	}
	jiraDonor := githubWorkItemDerivationSubject{
		WorkItemID: "jira:CHAOS-9", Provider: "jira", ProjectID: &projectJira, OrgID: "org-acme",
	}
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{Projects: []githubWorkItemDerivationProjectFact{
		{Provider: "linear", ProjectID: projectLinear, TeamID: "team-linear", TeamName: "Linear Team", IsPrimary: 1, UpdatedAt: now},
		{Provider: "jira", ProjectID: projectJira, TeamID: "team-jira", TeamName: "Jira Team", IsPrimary: 1, UpdatedAt: now},
	}})
	subjects := map[string]githubWorkItemDerivationSubject{
		source.WorkItemID: source, linearDonor.WorkItemID: linearDonor, jiraDonor.WorkItemID: jiraDonor,
	}

	t.Run("ambiguous provider-neutral key", func(t *testing.T) {
		candidateContext := derived
		candidateContext.linkedIssue, _, _ = candidateContext.buildLinkedIssueIndex(
			"github", subjects, []githubWorkItemDependencyRow{{
				SourceWorkItemID: source.WorkItemID, TargetWorkItemID: "extkey:CHAOS-9",
				RelationshipType: "external_issue_key", LastSynced: now, OrgID: "org-acme",
			}}, nil,
		)
		teamID, _, candidates := candidateContext.resolve(source)
		if teamID != nil || len(candidates) != 1 || candidates[0].Source != "unassigned" {
			t.Fatalf("ambiguous key resolved: team=%v candidates=%+v", teamID, candidates)
		}
	})

	t.Run("key-shaped donor without edge", func(t *testing.T) {
		candidateContext := derived
		candidateContext.linkedIssue, _, _ = candidateContext.buildLinkedIssueIndex(
			"github", subjects, []githubWorkItemDependencyRow{{
				SourceWorkItemID: "gh:acme/api#other", TargetWorkItemID: linearDonor.WorkItemID,
				RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
			}}, nil,
		)
		teamID, _, candidates := candidateContext.resolve(source)
		if teamID != nil || len(candidates) != 1 || candidates[0].Source != "unassigned" {
			t.Fatalf("unlinked donor resolved: team=%v candidates=%+v", teamID, candidates)
		}
	})
}

func TestGitHubWorkItemDerivationRanksProjectRepoAndMemberFacts(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	projectID := "acme/api"
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Projects: []githubWorkItemDerivationProjectFact{{
			Provider: "github", ProjectID: projectID, TeamID: "team-project", TeamName: "Project Team",
			IsPrimary: 1, Specificity: 80, UpdatedAt: now,
		}},
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", RepoID: &repoID, RepoFullName: projectID,
			TeamID: "team-repo", TeamName: "Repo Team", IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", MemberID: "dev@example.com", TeamID: "team-member", TeamName: "Member Team",
			IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#10", Provider: "github", RepoID: &repoID,
		ProjectID: &projectID, Assignees: []string{"DEV@EXAMPLE.COM"}, OrgID: "org-acme",
	})
	if got := githubWorkItemDerivationStringValue(teamID); got != "team-project" {
		t.Fatalf("primary team = %q, want team-project", got)
	}
	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if candidate := bySource["project_ownership"]; candidate.IsPrimary != 1 || candidate.Confidence != "high" || candidate.Evidence != "project_ownership=acme/api" {
		t.Fatalf("project provenance = %+v", candidate)
	}
	for _, source := range []string{"repo_ownership", "assignee_membership"} {
		if candidate := bySource[source]; candidate.IsPrimary != 0 {
			t.Fatalf("%s unexpectedly primary: %+v", source, candidate)
		}
	}
}

func TestLoadGitHubWorkItemDerivationContextCapsDonorTargets(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	dependencies := make([]githubWorkItemDependencyRow, githubWorkItemDerivationContextLimit+1)
	for index := range dependencies {
		dependencies[index] = githubWorkItemDependencyRow{
			SourceWorkItemID: "gh:acme/api#11", TargetWorkItemID: "linear:CHAOS-" + strconv.Itoa(index),
			RelationshipType: "relates_to", LastSynced: now, OrgID: claim.OrgID,
		}
	}
	source := &fakeGitHubWorkItemDerivationContextSource{}
	_, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim,
		githubWorkItemRows{Dependencies: dependencies}, source, now,
	)
	if !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("error = %v, want ErrEffectRecoveryUnsafe", err)
	}
	if source.called {
		t.Fatal("over-limit donor target set reached loader")
	}
}

func TestGitHubWorkItemDerivationQueriesCollapseTeamVersionsAndOrderStably(t *testing.T) {
	conn := &recordingGitHubWorkItemDerivationConn{}
	source := githubWorkItemClickHouseDerivationContextSource{Conn: conn}
	asOf := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	const orgID = "org-acme"

	if _, err := source.loadProjects(context.Background(), orgID, asOf); err != nil {
		t.Fatal(err)
	}
	if _, err := source.loadRepos(context.Background(), orgID, asOf); err != nil {
		t.Fatal(err)
	}
	if _, err := source.loadMembers(context.Background(), orgID, asOf); err != nil {
		t.Fatal(err)
	}
	if _, err := source.loadManualFallbacks(context.Background(), orgID, asOf); err != nil {
		t.Fatal(err)
	}
	if _, err := source.loadDonors(context.Background(), orgID, githubWorkItemDerivationLoadRequest{
		AsOf: asOf, DonorWorkItemIDs: []string{"linear:CHAOS-1"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(conn.queries) != 5 {
		t.Fatalf("queries = %d, want 5", len(conn.queries))
	}

	orders := []string{
		"ORDER BY g.provider, g.project_id, g.team_id",
		"ORDER BY g.provider, g.repo_full_name, g.team_id",
		"ORDER BY g.provider, g.member_id, g.team_id",
	}
	for index, order := range orders {
		query := conn.queries[index]
		if !strings.Contains(query, "argMax(name, (updated_at, last_synced, name))") ||
			!strings.Contains(query, "GROUP BY org_id, id") || !strings.Contains(query, order) {
			t.Fatalf("query %d lacks deterministic latest-team join/order:\n%s", index, query)
		}
		args := conn.args[index]
		if len(args) != 4 || args[0] != orgID || args[1] != asOf || args[2] != asOf || args[3] != githubWorkItemDerivationContextLimit+1 {
			t.Fatalf("query %d args = %#v", index, args)
		}
	}
	manualOrder := "ORDER BY provider, scope_type, scope_id, priority, team_id, team_name, reason"
	if manualQuery := conn.queries[3]; !strings.Contains(manualQuery, manualOrder) {
		t.Fatalf("manual fallback query lacks complete deterministic order:\n%s", manualQuery)
	}
	if args := conn.args[3]; len(args) != 4 || args[0] != orgID ||
		args[1] != asOf || args[2] != asOf ||
		args[3] != githubWorkItemDerivationContextLimit+1 {
		t.Fatalf("manual query args = %#v", args)
	}
	donorQuery := conn.queries[4]
	if !strings.Contains(donorQuery, "WHERE org_id = ?") || !strings.Contains(donorQuery, "LIMIT ?") {
		t.Fatalf("donor query lost tenant/limit fence:\n%s", donorQuery)
	}
	if args := conn.args[4]; len(args) != 4 || args[0] != orgID || args[3] != 2 {
		t.Fatalf("donor query args = %#v", args)
	}
}

// fakeDonorRow holds one donor row's column values, in the EXACT order
// loadDonors's SELECT list produces them.
type fakeDonorRow struct {
	values []any
}

// fakeDonorRows is a driver.Rows that actually returns data through Scan --
// codex round-6 finding (2026-08-25, BLOCK): every other Rows double in this
// package (emptyGitHubWorkItemDerivationRows included) has Next() return
// false immediately, so loadDonors's Scan() destination list -- which must
// stay in the same order as its SELECT column list -- has never actually
// been exercised. A column reordering that shifted "type" into the wrong
// destination would compile and pass every existing test while silently
// corrupting production donor Type propagation.
type fakeDonorRows struct {
	rows []fakeDonorRow
	idx  int
}

func (r *fakeDonorRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeDonorRows) Scan(dest ...any) error {
	row := r.rows[r.idx-1]
	if len(dest) != len(row.values) {
		return fmt.Errorf("scan destination count %d != row value count %d", len(dest), len(row.values))
	}
	for index, destination := range dest {
		value := row.values[index]
		switch target := destination.(type) {
		case *string:
			*target = value.(string)
		case **string:
			if pointer, ok := value.(*string); ok {
				*target = pointer
			} else {
				*target = nil
			}
		case *[]string:
			*target = value.([]string)
		default:
			return fmt.Errorf("fakeDonorRows: unsupported Scan destination %T at index %d", destination, index)
		}
	}
	return nil
}

func (fakeDonorRows) ScanStruct(any) error             { return nil }
func (fakeDonorRows) ColumnTypes() []driver.ColumnType { return nil }
func (fakeDonorRows) Totals(...any) error              { return nil }
func (fakeDonorRows) Columns() []string                { return nil }
func (fakeDonorRows) Close() error                     { return nil }
func (fakeDonorRows) Err() error                       { return nil }
func (fakeDonorRows) HasData() bool                    { return true }

type fakeDonorRowsConn struct {
	driver.Conn
	rows          *fakeDonorRows
	capturedQuery string
}

func (conn *fakeDonorRowsConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.capturedQuery = query
	return conn.rows, nil
}

func TestLoadGitHubWorkItemDerivationContextDonorScanPropagatesTypeInCorrectColumnOrder(t *testing.T) {
	// Codex round-7 finding (2026-08-25, HIGH, reproduced by hand: swapping
	// "provider, type" to "type, provider" in the SELECT list while leaving
	// Scan() untouched still passed this test before this assertion existed,
	// because fakeDonorRowsConn ignores the query text entirely and always
	// hands back the same hand-ordered values). The Scan-order assertions
	// below only catch a Scan-destination reorder; they say nothing about
	// whether the SELECT list itself still matches. Pin the exact column
	// list/order as text so a SELECT-only reorder fails THIS test too.
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	nativeTeamKey := "native-key"
	projectKey := "proj-key"
	projectID := "proj-id"
	projectName := "proj-name"
	conn := &fakeDonorRowsConn{rows: &fakeDonorRows{rows: []fakeDonorRow{{values: []any{
		"ghpr:acme/api#9", "github", "pr", &repoID, &nativeTeamKey,
		&projectKey, &projectID, &projectName, []string{"alice"}, "org-acme",
	}}}}}
	source := githubWorkItemClickHouseDerivationContextSource{Conn: conn}
	subjects, err := source.loadDonors(context.Background(), "org-acme", githubWorkItemDerivationLoadRequest{
		DonorWorkItemIDs: []string{"ghpr:acme/api#9"},
	})
	if err != nil {
		t.Fatalf("loadDonors error = %v", err)
	}
	// Codex round-8 finding (2026-08-25, MEDIUM): strings.Contains alone only
	// proves the expected columns appear IN ORDER somewhere in the query --
	// it would still pass if a future column were appended after org_id
	// (production Scan has exactly ten destinations, so a silently widened
	// SELECT list is a real arity regression this test exists to catch).
	// Anchoring the projection to end immediately before "FROM work_items
	// FINAL" (no columns in between) makes the ten-column list exact, not
	// just a matching prefix.
	const expectedProjection = "SELECT work_item_id, provider, type, toString(repo_id), native_team_key, project_key,\n       project_id, project_name, assignees, org_id\nFROM work_items FINAL"
	if !strings.Contains(conn.capturedQuery, expectedProjection) {
		t.Fatalf("donor query SELECT projection changed shape (order, columns, or an appended column before FROM):\n%s", conn.capturedQuery)
	}
	if len(subjects) != 1 {
		t.Fatalf("subjects = %+v, want exactly 1", subjects)
	}
	subject := subjects[0]
	if subject.WorkItemID != "ghpr:acme/api#9" || subject.Provider != "github" || subject.Type != "pr" {
		t.Fatalf("WorkItemID/Provider/Type = %q/%q/%q, want ghpr:acme/api#9/github/pr",
			subject.WorkItemID, subject.Provider, subject.Type)
	}
	if subject.RepoID == nil || *subject.RepoID != repoID {
		t.Fatalf("RepoID = %v, want %v", subject.RepoID, repoID)
	}
	if subject.NativeTeamKey == nil || *subject.NativeTeamKey != nativeTeamKey {
		t.Fatalf("NativeTeamKey = %v, want %v", subject.NativeTeamKey, nativeTeamKey)
	}
	if subject.ProjectKey == nil || *subject.ProjectKey != projectKey {
		t.Fatalf("ProjectKey = %v, want %v", subject.ProjectKey, projectKey)
	}
	if subject.ProjectID == nil || *subject.ProjectID != projectID {
		t.Fatalf("ProjectID = %v, want %v", subject.ProjectID, projectID)
	}
	if subject.ProjectName == nil || *subject.ProjectName != projectName {
		t.Fatalf("ProjectName = %v, want %v", subject.ProjectName, projectName)
	}
	if len(subject.Assignees) != 1 || subject.Assignees[0] != "alice" {
		t.Fatalf("Assignees = %+v, want [alice]", subject.Assignees)
	}
	if subject.OrgID != "org-acme" {
		t.Fatalf("OrgID = %q, want org-acme", subject.OrgID)
	}
}

var _ githubWorkItemDerivationContextSource = (*fakeGitHubWorkItemDerivationContextSource)(nil)

// CHAOS-4321 (chris's ruling, 2026-08-26): only the CHAOS-4244 author_membership
// path (a PR/MR's reporter walked through team_memberships) is removed here.
// assignee_membership -- the pre-4244, rank-4 mechanism -- is UNCHANGED and
// stays legitimate under the manual-override basis chris confirmed (see
// docs/contribute/architecture/team-attribution.md §0); its own coverage
// (TestGitHubWorkItemDerivationPreservesPrecedenceAndProvenance,
// TestGitHubWorkItemDerivationRanksProjectRepoAndMemberFacts, both above,
// untouched) is not repeated here. These replace the CHAOS-4244 author-path
// test suite that asserted the now-removed behavior with red-first coverage
// of the ruling's author-only scenarios: every case below failed against the
// pre-CHAOS-4321 resolver, which stamped a primary author_membership
// candidate from team_memberships alone.
func TestGitHubWorkItemDerivationAuthorNeverInfersTeamFromMembership(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	for _, tt := range []struct {
		name    string
		facts   githubWorkItemDerivationFacts
		subject func() githubWorkItemDerivationSubject
	}{
		{
			name: "author in a team that does not own the repo",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#1", Provider: "github", Type: "pr",
					RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
				}
			},
		},
		{
			name: "author on two teams (ambiguous) -- no arbitrary pick",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{
					{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now},
					{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now},
				},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#2", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
		},
		{
			name: "bot author with a matching membership row -- still unassigned",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "dependabot[bot]", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "github:dependabot[bot]"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#5", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
		},
		{
			name: "plain GitHub issue, mapped reporter, no ownership fact anywhere",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#9", Provider: "github", Type: "issue",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
		},
		{
			name: "GitLab MR, mapped reporter -- author path removed for every provider/type",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "gitlab", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "gitlab:acme/api!9", Provider: "gitlab", Type: "merge_request",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			derived := newGitHubWorkItemDerivationContext(tt.facts)
			subject := tt.subject()
			teamID, teamName, candidates := derived.resolve(subject)
			if teamID != nil || teamName != nil {
				t.Fatalf("team = (%v, %v), want (nil, nil)",
					githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
			}
			// Evidence stays plain "no_candidate" -- byte-identical to
			// Python and to main -- not "no_candidate:no_owning_team": that
			// telemetry improvement (ticket step 4) broke the live-Python-
			// oracle parity gate (Python's evidence is untouched, held
			// pending team-lead) and is deferred to land with the Python fix.
			if len(candidates) != 1 || candidates[0].Source != "unassigned" ||
				candidates[0].Evidence != "no_candidate" || candidates[0].IsPrimary != 1 {
				t.Fatalf("candidates = %+v, want exactly one unassigned/no_candidate primary row", candidates)
			}
			if got := githubWorkItemTeamAttributionMetricSource(githubWorkItemTeamAttributionRow{
				Source: candidates[0].Source, Evidence: candidates[0].Evidence,
			}); got != "unassigned" {
				t.Fatalf("metric source = %q, want unassigned", got)
			}
		})
	}
}

func TestGitHubWorkItemDerivationOwnershipWinsOverAuthorMembership(t *testing.T) {
	// CHAOS-4321: repo ownership resolves the team even when the author is
	// mapped to a DIFFERENT team via team_memberships -- the author signal
	// must contribute NOTHING to the candidate list at all (not merely lose
	// a precedence fight, and unlike assignee_membership which DOES still
	// appear as a non-primary candidate in this situation --
	// TestGitHubWorkItemDerivationPreservesPrecedenceAndProvenance above).
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-other", TeamName: "Other Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#6", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-repo" || githubWorkItemDerivationStringValue(teamName) != "Repository Team" {
		t.Fatalf("team = (%v, %v), want team-repo/Repository Team",
			githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
	}
	// No assignee set, so no assignee_membership candidate either -- exactly
	// one row (repo_ownership); the author signal is structurally absent.
	if len(candidates) != 1 || candidates[0].Source != "repo_ownership" {
		t.Fatalf("candidates = %+v, want exactly one repo_ownership row (author must not appear at all)", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorOnlyDonorNeverPropagatesATeam(t *testing.T) {
	// CHAOS-4321: since the author signal no longer resolves ANY team, an
	// author-only "donor" cannot inherit-donate one either. author_membership
	// was never in allowedDonorSources even before this ruling (see main's
	// buildLinkedIssueIndex) -- this confirms the stronger post-fix property:
	// there is no author candidate to consider donor-eligible in the first
	// place.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	reporter := "alice"
	donor := githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#100", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	}
	dependent := githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#101", Provider: "github", Type: "pr", OrgID: "org-acme",
	}
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-donor-only", TeamName: "Donor Only Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	donorTeamID, _, _ := derived.resolve(donor)
	if donorTeamID != nil {
		t.Fatalf("donor team id = %v, want nil (author-only membership must not resolve a team)",
			githubWorkItemDerivationStringValue(donorTeamID))
	}
	subjects := map[string]githubWorkItemDerivationSubject{
		donor.WorkItemID: donor, dependent.WorkItemID: dependent,
	}
	linkedIssue, _, _ := derived.buildLinkedIssueIndex(
		"github", subjects, []githubWorkItemDependencyRow{{
			SourceWorkItemID: dependent.WorkItemID, TargetWorkItemID: donor.WorkItemID,
			RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
		}}, nil,
	)
	if _, ok := linkedIssue[dependent.WorkItemID]; ok {
		t.Fatalf("dependent inherited a team from an author-only donor: %+v", linkedIssue[dependent.WorkItemID])
	}
}
