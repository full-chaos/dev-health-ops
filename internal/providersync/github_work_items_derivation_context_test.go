package providersync

import (
	"context"
	"errors"
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

var _ githubWorkItemDerivationContextSource = (*fakeGitHubWorkItemDerivationContextSource)(nil)

// CHAOS-4244: a GitHub PR's author (Reporter) is a membership signal the
// "assignee" field never carries -- GitHub distinguishes the two, and most
// PRs are opened with no assignee set. This mirrors
// compute_work_items.py's resolve_team_attribution, which now feeds
// item.reporter into the same assignee_membership candidate list.

func TestGitHubWorkItemDerivationAuthorWithNoAssigneeResolvesViaAssigneeMembership(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:full-chaos/dev-health-ops#4244", Provider: "github",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	if githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
		t.Fatalf("team name = %v, want Ops Team", teamName)
	}
	if len(candidates) != 1 || candidates[0].Source != "assignee_membership" || candidates[0].IsPrimary != 1 {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationNoAssigneeNoReporterStaysUnassigned(t *testing.T) {
	// RED CONTROL: same member facts, but subject.Reporter is nil (the
	// pre-fix shape). Must stay unassigned -- proves the positive test above
	// is not passing for some other, accidental reason.
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:full-chaos/dev-health-ops#4244", Provider: "github",
		OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil", teamID)
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationAmbiguousReporterMembershipContributesNothing(t *testing.T) {
	// CHAOS-4110 ambiguity gate (chris, 2026-08-23): a person-shaped signal is
	// only usable "where the reporter's membership is unambiguous (exactly
	// one team)". Two DIFFERENT teams for the same identity must contribute
	// nothing, not an arbitrary tie-break winner.
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			},
			{
				Provider: "github", TeamID: "team-platform", TeamName: "Platform Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			},
		},
	})
	reporter := "alice"
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:full-chaos/dev-health-ops#4244", Provider: "github",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (ambiguous reporter membership)", teamID)
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationUnambiguousReporterMembershipStillResolves(t *testing.T) {
	// Positive control: multiple candidate ROWS for the same identity that
	// all name the SAME team_id (e.g. matched via member_id and an email
	// facet separately) must still resolve -- the gate counts DISTINCT
	// team_ids, not candidate rows.
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", RawEmail: githubWorkItemDerivationStringPointer("alice"),
				IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			},
		},
	})
	reporter := "alice"
	teamID, teamName, _ := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:full-chaos/dev-health-ops#4244", Provider: "github",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	if githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
		t.Fatalf("team name = %v, want Ops Team", teamName)
	}
}

func TestGitHubWorkItemDerivationReporterNeverOutranksAHigherSource(t *testing.T) {
	// The author candidate is still rank 4 (assignee_membership): a
	// repo_ownership fact must keep winning even when the reporter also
	// resolves to a DIFFERENT team.
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:full-chaos/dev-health-ops#9", Provider: "github",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-repo" {
		t.Fatalf("team id = %v, want team-repo (repo_ownership must outrank the author)", teamID)
	}
	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if author := bySource["assignee_membership"]; author.IsPrimary != 0 || githubWorkItemDerivationStringValue(author.TeamID) != "team-ops" {
		t.Fatalf("author candidate = %+v, want present, non-primary, team-ops", author)
	}
}
