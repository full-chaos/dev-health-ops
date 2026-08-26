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
	if _, _, _, _, err := source.loadMembers(context.Background(), orgID, asOf); err != nil {
		t.Fatal(err)
	}
	if _, err := source.loadProviderMembers(context.Background(), orgID, asOf); err != nil {
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
	// CHAOS-4321: loadMembers issues TWO queries (identities, teams admin
	// roster) and loadProviderMembers restores the pre-CHAOS-4321
	// team_memberships query as the fallback layer (chris, 08:30 PT) -- 7
	// total, not 5.
	if len(conn.queries) != 7 {
		t.Fatalf("queries = %d, want 7", len(conn.queries))
	}

	orders := []string{
		"ORDER BY g.provider, g.project_id, g.team_id",
		"ORDER BY g.provider, g.repo_full_name, g.team_id",
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

	// loadMembers: identities (index 2) then admin teams (index 3) -- both
	// admin-authored, org-scoped, active-only, and neither takes an as-of
	// arg (membership isn't bitemporal like ownership).
	identitiesQuery := conn.queries[2]
	if !strings.Contains(identitiesQuery, "FROM identities FINAL") ||
		!strings.Contains(identitiesQuery, "is_active = 1") {
		t.Fatalf("identities query lacks admin-authored/active fence:\n%s", identitiesQuery)
	}
	if args := conn.args[2]; len(args) != 2 || args[0] != orgID || args[1] != githubWorkItemDerivationContextLimit+1 {
		t.Fatalf("identities query args = %#v", args)
	}
	adminTeamsQuery := conn.queries[3]
	if !strings.Contains(adminTeamsQuery, "FROM teams FINAL") ||
		!strings.Contains(adminTeamsQuery, "is_active = 1") {
		t.Fatalf("admin teams query lacks admin-authored/active fence:\n%s", adminTeamsQuery)
	}
	if args := conn.args[3]; len(args) != 2 || args[0] != orgID || args[1] != githubWorkItemDerivationContextLimit+1 {
		t.Fatalf("admin teams query args = %#v", args)
	}

	// loadProviderMembers (index 4): the restored pre-CHAOS-4321
	// team_memberships query, fallback layer, bitemporal (as_of + org_id).
	providerMembersQuery := conn.queries[4]
	if !strings.Contains(providerMembersQuery, "FROM team_memberships") ||
		!strings.Contains(providerMembersQuery, "ORDER BY g.provider, g.member_id, g.team_id") {
		t.Fatalf("provider members query lacks expected shape:\n%s", providerMembersQuery)
	}
	if args := conn.args[4]; len(args) != 4 || args[0] != orgID || args[1] != asOf || args[2] != asOf || args[3] != githubWorkItemDerivationContextLimit+1 {
		t.Fatalf("provider members query args = %#v", args)
	}

	manualOrder := "ORDER BY provider, scope_type, scope_id, priority, team_id, team_name, reason"
	if manualQuery := conn.queries[5]; !strings.Contains(manualQuery, manualOrder) {
		t.Fatalf("manual fallback query lacks complete deterministic order:\n%s", manualQuery)
	}
	if args := conn.args[5]; len(args) != 4 || args[0] != orgID ||
		args[1] != asOf || args[2] != asOf ||
		args[3] != githubWorkItemDerivationContextLimit+1 {
		t.Fatalf("manual query args = %#v", args)
	}
	donorQuery := conn.queries[6]
	if !strings.Contains(donorQuery, "WHERE org_id = ?") || !strings.Contains(donorQuery, "LIMIT ?") {
		t.Fatalf("donor query lost tenant/limit fence:\n%s", donorQuery)
	}
	if args := conn.args[6]; len(args) != 4 || args[0] != orgID || args[3] != 2 {
		t.Fatalf("donor query args = %#v", args)
	}
}

// fakeMembersSplitConn feeds loadMembers one identities row and one teams
// row so the CHAOS-4321 round-3 provider-tag split can be tested against
// the real ClickHouse-scan path, not just the pure resolve() consumer.
type fakeMembersSplitConn struct {
	driver.Conn
}

func (fakeMembersSplitConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM identities FINAL"):
		return &fakeMembersSplitRows{
			rows: [][]any{
				{"alice-id", (*string)(nil), `{"github":["lead"]}`, []string{}, time.Time{}},
			},
		}, nil
	case strings.Contains(query, "FROM teams FINAL"):
		return &fakeMembersSplitRows{
			rows: [][]any{
				{"team-eng", "Engineering", []string{"lead", "alice@example.com"}, []string{}},
			},
		}, nil
	default:
		return emptyGitHubWorkItemDerivationRows{}, nil
	}
}

type fakeMembersSplitRows struct {
	rows [][]any
	idx  int
}

func (r *fakeMembersSplitRows) Next() bool { return r.idx < len(r.rows) }
func (r *fakeMembersSplitRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i, d := range dest {
		switch target := d.(type) {
		case *string:
			*target, _ = row[i].(string)
		case **string:
			*target, _ = row[i].(*string)
		case *[]string:
			*target, _ = row[i].([]string)
		case *time.Time:
			*target, _ = row[i].(time.Time)
		default:
			return fmt.Errorf("fakeMembersSplitRows: unsupported scan dest %T", d)
		}
	}
	return nil
}
func (r *fakeMembersSplitRows) ScanStruct(any) error             { return nil }
func (r *fakeMembersSplitRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fakeMembersSplitRows) Totals(...any) error              { return nil }
func (r *fakeMembersSplitRows) Columns() []string                { return nil }
func (r *fakeMembersSplitRows) Close() error                     { return nil }
func (r *fakeMembersSplitRows) Err() error                       { return nil }
func (r *fakeMembersSplitRows) HasData() bool                    { return len(r.rows) > 0 }

// TestGitHubWorkItemLoadMembersScopesTeamsMembersFallbackByProvider pins the
// loadMembers half of the CHAOS-4321 round-3 fix directly (resolve()-level
// coverage lives in TestGitHubWorkItemDerivationTwoLayerMembershipResolution
// (k)/(l)/(m)): a `teams.members` roster containing a bare, non-email login
// ("lead") that identities.provider_identities confirms belongs to GitHub
// must be split into a github-provider-tagged fact, NOT the untyped pool --
// while the email-shaped facet in the SAME roster stays untyped.
func TestGitHubWorkItemLoadMembersScopesTeamsMembersFallbackByProvider(t *testing.T) {
	source := githubWorkItemClickHouseDerivationContextSource{Conn: fakeMembersSplitConn{}}
	_, _, providerUntyped, providerTagged, err := source.loadMembers(context.Background(), "org-acme", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(providerTagged) != 1 ||
		providerTagged[0].Provider != "github" || providerTagged[0].TeamID != "team-eng" ||
		providerTagged[0].MemberID != "lead" || providerTagged[0].Specificity != 50 || providerTagged[0].Priority != 10 {
		t.Fatalf("providerTagged = %+v, want exactly one github-tagged \"lead\" fact at specificity 50/priority 10", providerTagged)
	}
	if len(providerUntyped) != 1 || providerUntyped[0].Facet != "alice@example.com" {
		t.Fatalf("providerUntyped = %+v, want exactly the email-shaped facet (the login must NOT stay untyped)", providerUntyped)
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
// TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped
// covers CHAOS-4321 (chris's ruling, 2026-08-26 07:09 PT): membership-based
// attribution (assignee AND author alike) is legitimate ONLY when it
// resolves to EXACTLY one admin-authored team -- `derived.memberByID` is
// itself sourced only from `identities`/`teams` now (see loadMembers), never
// provider auto-import, so every candidate reaching resolve() is already
// admin-mapped; "no membership" here means no ADMIN membership, and
// "ambiguous" means mapped to 2+ teams, never "auto-imported vs curated".
func TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name       string
		facts      githubWorkItemDerivationFacts
		subject    func() githubWorkItemDerivationSubject
		wantSource string
		wantTeam   string
		wantEvid   string
	}{
		{
			name:  "author with no admin mapping is unassigned",
			facts: githubWorkItemDerivationFacts{},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#1", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:no_membership",
		},
		{
			name: "author admin-mapped to one team is attributed",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#2", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "author_membership", wantTeam: "team-ops",
		},
		{
			name: "author admin-mapped to two teams is unassigned, no arbitrary pick",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{
					{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
					{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#3", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:ambiguous_admin_membership:team-ops,team-platform",
		},
		{
			name: "bot author never attributed even when admin-mapped",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "dependabot[bot]", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "github:dependabot[bot]"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#5", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:bot_author",
		},
		{
			name: "plain GitHub issue, admin-mapped reporter -- author path stays PR/MR-only",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#9", Provider: "github", Type: "issue",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate",
		},
		{
			name: "GitLab MR, admin-mapped reporter -- author path is provider-neutral",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "gitlab", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "gitlab:acme/api!9", Provider: "gitlab", Type: "merge_request",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "author_membership", wantTeam: "team-ops",
		},
		{
			name:  "assignee with no admin mapping is unassigned",
			facts: githubWorkItemDerivationFacts{},
			subject: func() githubWorkItemDerivationSubject {
				return githubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#20", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:no_membership",
		},
		{
			name: "assignee admin-mapped to one team is attributed",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() githubWorkItemDerivationSubject {
				return githubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#21", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "assignee_membership", wantTeam: "team-ops",
		},
		{
			name: "assignee admin-mapped to two teams is unassigned, no arbitrary pick",
			facts: githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{
					{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
					{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				},
			},
			subject: func() githubWorkItemDerivationSubject {
				return githubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#22", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:ambiguous_admin_membership:team-ops,team-platform",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			derived := newGitHubWorkItemDerivationContext(tt.facts)
			subject := tt.subject()
			teamID, teamName, candidates := derived.resolve(subject)
			if tt.wantTeam == "" {
				if teamID != nil || teamName != nil {
					t.Fatalf("team = (%v, %v), want (nil, nil)",
						githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
				}
			} else if githubWorkItemDerivationStringValue(teamID) != tt.wantTeam {
				t.Fatalf("team = %v, want %v", githubWorkItemDerivationStringValue(teamID), tt.wantTeam)
			}
			var primary *githubWorkItemDerivationCandidate
			for index := range candidates {
				if candidates[index].IsPrimary == 1 {
					primary = &candidates[index]
					break
				}
			}
			if primary == nil || primary.Source != tt.wantSource {
				t.Fatalf("candidates = %+v, want primary source %q", candidates, tt.wantSource)
			}
			if tt.wantEvid != "" && primary.Evidence != tt.wantEvid {
				t.Fatalf("evidence = %q, want %q", primary.Evidence, tt.wantEvid)
			}
			if tt.wantSource == "unassigned" {
				wantMetric := "unassigned"
				if reason, ok := strings.CutPrefix(tt.wantEvid, "no_candidate:"); ok && reason != "" {
					// CHAOS-4321: the metric label drops any ":<team ids>"
					// suffix (cardinality guard) -- only the persisted
					// evidence text keeps it.
					if name, _, found := strings.Cut(reason, ":"); found {
						wantMetric = name
					} else {
						wantMetric = reason
					}
				}
				if got := githubWorkItemTeamAttributionMetricSource(githubWorkItemTeamAttributionRow{
					Source: primary.Source, Evidence: primary.Evidence,
				}); got != wantMetric {
					t.Fatalf("metric source = %q, want %q", got, wantMetric)
				}
			}
		})
	}
}

func TestGitHubWorkItemDerivationOwnershipWinsOverAssigneeAndAuthorMembership(t *testing.T) {
	// CHAOS-4321: ownership always wins, even when BOTH assignee and author
	// are admin-mapped to a DIFFERENT team -- membership candidates still
	// appear as non-primary provenance rows, they just never outrank a real
	// repo_ownership fact.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{
			{Provider: "github", TeamID: "team-other", TeamName: "Other Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
			{Provider: "github", TeamID: "team-other", TeamName: "Other Team", MemberID: "bob", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
		},
	})
	reporter := "alice"
	teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#6", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, Assignees: []string{"bob"}, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-repo" || githubWorkItemDerivationStringValue(teamName) != "Repository Team" {
		t.Fatalf("team = (%v, %v), want team-repo/Repository Team",
			githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
	}
	sources := map[string]bool{}
	var primarySource string
	for _, candidate := range candidates {
		sources[candidate.Source] = true
		if candidate.IsPrimary == 1 {
			primarySource = candidate.Source
		}
	}
	if primarySource != "repo_ownership" {
		t.Fatalf("primary source = %q, want repo_ownership", primarySource)
	}
	if !sources["assignee_membership"] || !sources["author_membership"] {
		t.Fatalf("candidates = %+v, want assignee_membership AND author_membership present as non-primary rows", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorOnlyDonorNeverPropagatesATeam(t *testing.T) {
	// CHAOS-4321: author_membership resolves the donor's OWN attribution
	// (when unambiguous, restored by the 07:09 PT ruling) but stays OUT of
	// allowedDonorSources (see buildLinkedIssueIndex) -- a person-shaped
	// signal must never be laundered into a rank-5 linked_issue donor fact
	// for a DIFFERENT item, ranked or not.
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
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	donorTeamID, _, donorCandidates := derived.resolve(donor)
	if githubWorkItemDerivationStringValue(donorTeamID) != "team-donor-only" {
		t.Fatalf("donor team id = %v, want team-donor-only (its OWN attribution resolves via author_membership)",
			githubWorkItemDerivationStringValue(donorTeamID))
	}
	for _, candidate := range donorCandidates {
		if candidate.IsPrimary == 1 && candidate.Source != "author_membership" {
			t.Fatalf("donor primary source = %q, want author_membership", candidate.Source)
		}
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

// TestGitHubWorkItemDerivationTwoLayerMembershipResolution covers CHAOS-4321
// (chris, 08:30 PT: "manual is override -- if the override exists, use it,
// else use attribution from providers"): (e) a bare teams.members facet with
// no backing identities row still resolves via memberByUntypedFacet, (f) no
// admin mapping at all falls through to the provider auto-import layer, (g)
// an admin mapping wins outright over a conflicting provider membership for
// the SAME identity, and (h) an AMBIGUOUS admin mapping does not fall
// through to a clean provider answer.
func TestGitHubWorkItemDerivationTwoLayerMembershipResolution(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	t.Run("(e) teams.members-only mapping is attributed, no identities row", func(t *testing.T) {
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			UntypedMembers: []githubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-ops", TeamName: "Ops Team", Facet: "alice@example.com", UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#30", Provider: "github", Type: "issue",
			Assignees: []string{"alice@example.com"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-ops" || githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})

	t.Run("(f) provider-only single team is attributed via fallback layer", func(t *testing.T) {
		reporter := "alice"
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "ghpr:acme/api#31", Provider: "github", Type: "pr",
			Reporter: &reporter, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-ops" || githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "author_membership" || candidates[0].Evidence != "reporter=alice" {
			t.Fatalf("candidates = %+v, want exactly one author_membership row", candidates)
		}
	})

	t.Run("(g) admin mapping overrides a conflicting provider membership", func(t *testing.T) {
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			Members: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
			}},
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-other", TeamName: "Other Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#32", Provider: "github", Type: "issue",
			Assignees: []string{"alice"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-ops" || githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row (provider layer must not even appear)", candidates)
		}
	})

	t.Run("(h) ambiguous admin mapping does not fall through to provider", func(t *testing.T) {
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			Members: []githubWorkItemDerivationMemberFact{
				{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
			},
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-clean", TeamName: "Clean Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#33", Provider: "github", Type: "issue",
			Assignees: []string{"alice"}, OrgID: "org-acme",
		})
		if teamID != nil || teamName != nil {
			t.Fatalf("team = (%v, %v), want (nil, nil)", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "unassigned" ||
			candidates[0].Evidence != "no_candidate:ambiguous_admin_membership:team-ops,team-platform" {
			t.Fatalf("candidates = %+v, want exactly one unassigned row naming the colliding admin teams", candidates)
		}
	})

	t.Run("(i) a bare UntypedMembers-shaped members facet with no ManualMembers entry resolves via the fallback tier, not the override", func(t *testing.T) {
		// CHAOS-4321 fix (chris, 2026-08-26 10:39 PT, after a codex
		// adversarial review HIGH finding: "the new membership layer can
		// turn provider-imported rosters into authoritative,
		// provider-neutral admin overrides"): ProviderUntypedMembers (from
		// teams.members) must resolve, but as the fallback tier -- lower
		// specificity than the admin layer's UntypedMembers
		// (teams.manual_members).
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderUntypedMembers: []githubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-fallback", TeamName: "Fallback Team", Facet: "bob@example.com", UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#34", Provider: "github", Type: "issue",
			Assignees: []string{"bob@example.com"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-fallback" || githubWorkItemDerivationStringValue(teamName) != "Fallback Team" {
			t.Fatalf("team = (%v, %v), want team-fallback/Fallback Team", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" || candidates[0].Specificity != 50 {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row at specificity 50", candidates)
		}
	})

	t.Run("(j) an UntypedMembers (manual_members) entry overrides a conflicting ProviderUntypedMembers (members) entry for the same identity", func(t *testing.T) {
		// The other half of (i): a genuinely admin-exclusive
		// teams.manual_members facet wins outright even when the SAME
		// identity also appears in a DIFFERENT team's bare teams.members
		// roster (the shape a provider auto-import row takes) -- the admin
		// layer short-circuits before the fallback pool is ever consulted.
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			UntypedMembers: []githubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-override", TeamName: "Override Team", Facet: "carol@example.com", UpdatedAt: now,
			}},
			ProviderUntypedMembers: []githubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-fallback", TeamName: "Fallback Team", Facet: "carol@example.com", UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#35", Provider: "github", Type: "issue",
			Assignees: []string{"carol@example.com"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-override" || githubWorkItemDerivationStringValue(teamName) != "Override Team" {
			t.Fatalf("team = (%v, %v), want team-override/Override Team", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" || candidates[0].Specificity != 60 {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row at specificity 60 (admin layer, not fallback)", candidates)
		}
	})

	// (k)/(l)/(m) below pin the CHAOS-4321 round-3 codex adversarial review
	// HIGH finding fix (team-lead ruling, 2026-08-26): a teams.members
	// fallback facet must not cross providers unless it is email-shaped.
	// loadMembers is what actually splits a raw teams.members roster by
	// provider tag (identities.provider_identities) -- these three cases
	// fix the SPLIT'S OUTPUT shape directly, i.e. exactly what loadMembers
	// hands to ProviderMembers (provider-tagged) and ProviderUntypedMembers
	// (email-shaped, still untyped) after the split.
	t.Run("(k) a provider-tagged roster login never attributes a DIFFERENT provider's item sharing the same string", func(t *testing.T) {
		// A GitHub team's roster contains bare login "lead" -- loadMembers
		// confirmed via identities.provider_identities that "lead" is a
		// GitHub identity, so it lands in ProviderMembers keyed to
		// Provider: "github", not in the untyped pool.
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "lead", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "jira:PROJ-1", Provider: "jira", Type: "issue",
			Assignees: []string{"lead"}, OrgID: "org-acme",
		})
		if teamID != nil || teamName != nil {
			t.Fatalf("team = (%v, %v), want (nil, nil): a github-tagged roster login must not attribute a jira item sharing the same raw string", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "unassigned" || candidates[0].Evidence != "no_candidate:no_membership" {
			t.Fatalf("candidates = %+v, want exactly one unassigned/no_membership row", candidates)
		}
	})

	t.Run("(l) the SAME provider-tagged roster login still attributes ITS OWN provider's item (positive control for (k))", func(t *testing.T) {
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "lead", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#40", Provider: "github", Type: "issue",
			Assignees: []string{"lead"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-eng" || githubWorkItemDerivationStringValue(teamName) != "Engineering" {
			t.Fatalf("team = (%v, %v), want team-eng/Engineering", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})

	t.Run("(m) an email-shaped roster facet still attributes ACROSS providers (CHAOS-2609 stays)", func(t *testing.T) {
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderUntypedMembers: []githubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-eng", TeamName: "Engineering", Facet: "alice@example.com", UpdatedAt: now,
			}},
		})
		for _, provider := range []string{"github", "jira"} {
			teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
				WorkItemID: provider + ":same-email#1", Provider: provider, Type: "issue",
				Assignees: []string{"alice@example.com"}, OrgID: "org-acme",
			})
			if githubWorkItemDerivationStringValue(teamID) != "team-eng" || githubWorkItemDerivationStringValue(teamName) != "Engineering" {
				t.Fatalf("%s: team = (%v, %v), want team-eng/Engineering", provider, githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
			}
			if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
				t.Fatalf("%s: candidates = %+v, want exactly one assignee_membership row", provider, candidates)
			}
		}
	})

	t.Run("(n) a provider-tagged roster facet with irregular internal whitespace still matches a work item assignee with DIFFERENT irregular whitespace", func(t *testing.T) {
		// Python/Go parity companion (team-lead ruling, 2026-08-26): Go's
		// resolve() already collapses internal whitespace correctly here --
		// normalizeDerivationIdentity (via strings.Fields+Join) runs on
		// every ProviderMembers identity at the SAME point real
		// team_memberships rows go through, so this pool's MemberID does
		// not need to be pre-normalized by loadMembers's split logic to
		// resolve correctly. This test is the Go half of the pair pinning
		// that fact -- the Python half
		// (test_loader_scopes_teams_members_fallback_facet_normalizes_internal_whitespace)
		// needed an actual code fix; this one is a regression pin, not a
		// fix, proving both languages now agree.
		derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
			ProviderMembers: []githubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "john   doe", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#41", Provider: "github", Type: "issue",
			Assignees: []string{"john  doe"}, OrgID: "org-acme",
		})
		if githubWorkItemDerivationStringValue(teamID) != "team-eng" || githubWorkItemDerivationStringValue(teamName) != "Engineering" {
			t.Fatalf("team = (%v, %v), want team-eng/Engineering", githubWorkItemDerivationStringValue(teamID), githubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})
}

// The tests below restore CHAOS-4244 author-path coverage that lane-4321's
// Round-1 commit (cbe8f65fe, "remove person-membership as a team source")
// deleted under that round's original wider scope, and which was never
// brought back when the scope narrowed to "membership stays, but only
// through admin-authored data, else provider fallback" (chris, 08:30 PT).
// Confirmed via `git worktree add ... origin/main` that origin/main still
// carries all 19 of these as of f26cf55e0 -- this is a genuine coverage
// gap on this branch, not a stale golden-count pin. Each is adapted to the
// two-layer resolveMembership design: admin-authored member facts now live
// in Facts.Members at Specificity 60 (the CHAOS-4321 admin-layer
// convention), not the pre-4321 Specificity 50 the deleted originals used.

func TestGitHubWorkItemDerivationSubjectFromRowPropagatesTypeForAuthorMembership(t *testing.T) {
	// Codex round-7 finding (2026-08-25, HIGH, reproduced by hand: removing
	// "Type: row.Type" from githubWorkItemDerivationSubjectFromRow left every
	// R6 Jira/Linear/mismatched-provider negative test green, because all
	// three use Provider "jira" -- the provider+type gate is already closed
	// on Provider alone, so those tests never actually exercise whether Type
	// propagated). This is the positive case codex asked for: a real
	// githubWorkItemRow with Provider "github" and Type "pr", converted
	// through the actual production githubWorkItemDerivationSubjectFromRow,
	// must still resolve via author_membership -- if Type propagation broke,
	// the gate would close and this would resolve unassigned instead. Table
	// includes the GitLab MR case too, since the gate is provider-paired.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name     string
		provider string
		itemType string
	}{
		{name: "github_pr", provider: "github", itemType: "pr"},
		{name: "gitlab_merge_request", provider: "gitlab", itemType: "merge_request"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: testCase.provider, TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			})
			reporter := "alice"
			row := githubWorkItemRow{
				WorkItemID: "acme/api#9", Provider: testCase.provider, Title: "t", Type: testCase.itemType,
				Status: "open", Reporter: &reporter, OrgID: "org-acme",
			}
			subject := githubWorkItemDerivationSubjectFromRow(row)
			if subject.Type != row.Type {
				t.Fatalf("githubWorkItemDerivationSubjectFromRow(row).Type = %q, want %q (row.Type must propagate)", subject.Type, row.Type)
			}
			teamID, teamName, candidates := derived.resolve(subject)
			if githubWorkItemDerivationStringValue(teamID) != "team-ops" {
				t.Fatalf("team id = %v, want team-ops (Type propagation must keep the author_membership gate open)", githubWorkItemDerivationStringValue(teamID))
			}
			if githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
				t.Fatalf("team name = %v, want Ops Team", githubWorkItemDerivationStringValue(teamName))
			}
			if len(candidates) != 1 || candidates[0].Source != "author_membership" || candidates[0].IsPrimary != 1 {
				t.Fatalf("candidates = %+v", candidates)
			}
		})
	}
}

func TestGitHubWorkItemDerivationNoAssigneeNoReporterStaysUnassigned(t *testing.T) {
	// RED CONTROL: same member facts as a positive author_membership case,
	// but subject.Reporter is nil. Must stay unassigned -- proves the
	// positive cases elsewhere in this file are not passing for some other,
	// accidental reason (e.g. an assignee fallback silently covering for a
	// broken reporter path).
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#4244", Provider: "github", Type: "pr",
		OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil", teamID)
	}
	// No Reporter and no Assignees means neither membership path is even
	// attempted (resolveMembership is never called), so no membership skip
	// reason exists to report -- evidence stays the bare "no_candidate", not
	// "no_candidate:no_membership" (that suffix is only for a path that WAS
	// attempted and found zero candidates).
	if len(candidates) != 1 || candidates[0].Source != "unassigned" ||
		candidates[0].Evidence != "no_candidate" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationUnambiguousReporterMembershipStillResolves(t *testing.T) {
	// Positive control: multiple candidate ROWS for the same identity that
	// all name the SAME team_id (e.g. matched via member_id and an email
	// facet separately) must still resolve -- the two-layer admin gate
	// counts DISTINCT team_ids, not candidate rows.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", RawEmail: githubWorkItemDerivationStringPointer("alice"),
				IsPrimary: 1, Specificity: 60, UpdatedAt: now,
			},
		},
	})
	reporter := "alice"
	teamID, teamName, _ := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#4244", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	if githubWorkItemDerivationStringValue(teamName) != "Ops Team" {
		t.Fatalf("team name = %v, want Ops Team", teamName)
	}
}

func TestGitHubWorkItemDerivationAuthorNeverOutranksALinkedIssueDonor(t *testing.T) {
	// CHAOS-4244 precedence ruling (chris, 2026-08-24): a PR with an
	// admin-mapped author AND a linked_issue donor for a DIFFERENT team must
	// resolve to the linked issue's team -- author_membership (rank 6) sits
	// BELOW linked_issue (rank 5).
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	derived.linkedIssue = map[string][2]string{
		"ghpr:acme/api#9": {"team-platform", "Platform Team"},
	}
	reporter := "alice"
	teamID, teamName, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#9", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-platform" {
		t.Fatalf("team id = %v, want team-platform (linked_issue must outrank the author)", teamID)
	}
	if githubWorkItemDerivationStringValue(teamName) != "Platform Team" {
		t.Fatalf("team name = %v, want Platform Team", teamName)
	}
	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if author := bySource["author_membership"]; author.IsPrimary != 0 || githubWorkItemDerivationStringValue(author.TeamID) != "team-ops" {
		t.Fatalf("author candidate = %+v, want present, non-primary, team-ops", author)
	}
	if linked := bySource["linked_issue"]; linked.IsPrimary != 1 || githubWorkItemDerivationStringValue(linked.TeamID) != "team-platform" {
		t.Fatalf("linked_issue candidate = %+v, want present, primary, team-platform", linked)
	}
}

func TestGitHubWorkItemDerivationCausalAuthorNeverOutranksARealLinkedIssueDonor(t *testing.T) {
	// The other half of the test above: rather than injecting the
	// linked_issue candidate directly, this drives the REAL production
	// builder end to end -- a Linear issue that resolves to team CHAOS via
	// its OWN project ownership fact (a first-class, donor-eligible source)
	// is linked to a GitHub PR whose author resolves to a DIFFERENT team via
	// the admin member layer. The PR must inherit the donor's team, not the
	// author's.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	donorProjectID := "linear-project-chaos"
	donor := githubWorkItemDerivationSubject{
		WorkItemID: "linear:CHAOS-2400", Provider: "linear",
		ProjectID: &donorProjectID, OrgID: "org-acme",
	}
	pr := githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#100", Provider: "github", Type: "pr",
		OrgID: "org-acme",
	}
	reporter := "alice"
	pr.Reporter = &reporter
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Projects: []githubWorkItemDerivationProjectFact{{
			Provider: "linear", ProjectID: donorProjectID, TeamID: "CHAOS", TeamName: "Chaos Team",
			IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	subjects := map[string]githubWorkItemDerivationSubject{
		donor.WorkItemID: donor, pr.WorkItemID: pr,
	}
	derived.linkedIssue, _, _ = derived.buildLinkedIssueIndex(
		"github", subjects, []githubWorkItemDependencyRow{{
			SourceWorkItemID: pr.WorkItemID, TargetWorkItemID: donor.WorkItemID,
			RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
		}}, nil,
	)
	teamID, teamName, candidates := derived.resolve(pr)
	if githubWorkItemDerivationStringValue(teamID) != "CHAOS" {
		t.Fatalf("team id = %v, want CHAOS (the real donor must outrank the author)", githubWorkItemDerivationStringValue(teamID))
	}
	if githubWorkItemDerivationStringValue(teamName) != "Chaos Team" {
		t.Fatalf("team name = %v, want Chaos Team", githubWorkItemDerivationStringValue(teamName))
	}
	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if linked := bySource["linked_issue"]; linked.IsPrimary != 1 || githubWorkItemDerivationStringValue(linked.TeamID) != "CHAOS" {
		t.Fatalf("linked_issue candidate = %+v, want present, primary, CHAOS", linked)
	}
	if author := bySource["author_membership"]; author.IsPrimary != 0 || githubWorkItemDerivationStringValue(author.TeamID) != "team-ops" {
		t.Fatalf("author candidate = %+v, want present, non-primary, team-ops", author)
	}
}

func TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAGitLabIssue(t *testing.T) {
	// Negative control paired with the GitLab MR case in
	// TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped:
	// a GitLab ISSUE (WorkItemID "gitlab:acme/api#9", the "#" convention, not
	// "!") must stay unassigned on this signal alone, exactly like a GitHub
	// issue does -- the PR/MR gate is Provider+Type, not just Provider.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "gitlab", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "gitlab:acme/api#9", Provider: "gitlab", Type: "issue",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (GitLab issue must not gain author_membership)", githubWorkItemDerivationStringValue(teamID))
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAJiraIssue(t *testing.T) {
	// Codex round-5 finding (2026-08-25, BLOCK): Jira has no PR-equivalent
	// Type at all, so this proves the gate stays closed for it even when its
	// author IS an admin-mapped, unambiguous single-team member -- the same
	// shape that would resolve for a real GitHub PR or GitLab MR author.
	// Drives the real githubWorkItemRow -> githubWorkItemDerivationSubjectFromRow
	// conversion (codex round-6: a hand-built subject literal would miss a
	// Type-propagation regression in that conversion).
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "jira", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	row := githubWorkItemRow{
		WorkItemID: "jira:OPS-101", Provider: "jira", Title: "t", Type: "bug",
		Status: "todo", Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(row))
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (Jira has no PR-equivalent type)", githubWorkItemDerivationStringValue(teamID))
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToALinearIssue(t *testing.T) {
	// The Linear half of the codex round-5 finding above: Linear also has no
	// PR-equivalent Type, so an admin-mapped, unambiguous single-team Linear
	// author must never gain author_membership either. Drives the real
	// githubWorkItemRow -> githubWorkItemDerivationSubjectFromRow conversion.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "linear", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	row := githubWorkItemRow{
		WorkItemID: "linear:CHAOS-5", Provider: "linear", Title: "t", Type: "story",
		Status: "todo", Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(row))
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (Linear has no PR-equivalent type)", githubWorkItemDerivationStringValue(teamID))
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorMembershipGatesOnProviderNotIDShape(t *testing.T) {
	// RED-FIRST on a since-fixed gate (codex round-5, 2026-08-25, BLOCK): an
	// earlier gate matched on WorkItemID STRING SHAPE alone (a "gitlab:"
	// prefix containing "!") with no check that Provider actually said
	// "gitlab". A legacy or mismatched row -- a Jira item whose WorkItemID
	// happens to look like a GitLab MR -- would therefore have incorrectly
	// opened the gate. The current gate,
	// githubWorkItemDerivationIsPullOrMergeRequestType, closes this because
	// Provider "jira" never matches either case of its switch. Drives the
	// real githubWorkItemRow -> githubWorkItemDerivationSubjectFromRow
	// conversion (codex round-6) so the proof covers actual production Type
	// propagation, not just the resolve()-level gate logic.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "jira", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	row := githubWorkItemRow{
		WorkItemID: "gitlab:legacy-import!42", Provider: "jira", Title: "t", Type: "bug",
		Status: "todo", Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(row))
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (Provider jira must gate closed despite a GitLab-MR-shaped WorkItemID)", githubWorkItemDerivationStringValue(teamID))
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationReporterAndAssigneeSamePersonSameTeamStayDistinctProvenance(t *testing.T) {
	// When the author IS the assignee and both resolve to the SAME team,
	// resolve() keeps BOTH candidates as provenance -- one assignee_membership
	// row (rank 4, evidence "assignee=...") and one author_membership row
	// (rank 6, evidence "reporter=..."). Splitting the source (CHAOS-4244's
	// precedence ruling) makes them structurally distinct at the ClickHouse
	// storage key (source differs, not just evidence). The assignee_membership
	// row must win primary: it outranks author_membership even for the
	// identical team.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, _, candidates := derived.resolve(githubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#77", Provider: "github", Type: "pr",
		Assignees: []string{"alice"}, Reporter: &reporter, OrgID: "org-acme",
	})
	if githubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	bySource := map[string]githubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	assignee, ok := bySource["assignee_membership"]
	if !ok || assignee.IsPrimary != 1 || githubWorkItemDerivationStringValue(assignee.TeamID) != "team-ops" {
		t.Fatalf("assignee candidate = %+v (present=%v), want present, primary, team-ops", assignee, ok)
	}
	author, ok := bySource["author_membership"]
	if !ok || author.IsPrimary != 0 || githubWorkItemDerivationStringValue(author.TeamID) != "team-ops" {
		t.Fatalf("author candidate = %+v (present=%v), want present, non-primary, team-ops", author, ok)
	}
	if assignee.Evidence == author.Evidence {
		t.Fatalf("expected distinct evidence (assignee= vs reporter=), got identical: %q", assignee.Evidence)
	}
}

func TestGitHubWorkItemTeamAttributionMetricSourceSplitsRealReporterFromRealAssigneeRows(t *testing.T) {
	// Metric-vocabulary regression (codex, 2026-08-24): a real
	// resolver-produced reporter candidate's evidence must classify as
	// "author", and a real assignee candidate's evidence must classify as
	// "assignee" -- proving it end to end through resolve(), not a
	// handcrafted "reporter="/"assignee=" literal.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name       string
		subject    githubWorkItemDerivationSubject
		wantMetric string
	}{
		{
			name: "reporter",
			subject: func() githubWorkItemDerivationSubject {
				reporter := "alice"
				return githubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#4244", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			}(),
			wantMetric: "author",
		},
		{
			name: "assignee",
			subject: githubWorkItemDerivationSubject{
				WorkItemID: "gh:acme/api#4245", Provider: "github", Type: "issue",
				Assignees: []string{"alice"}, OrgID: "org-acme",
			},
			wantMetric: "assignee",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
				Members: []githubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			})
			_, _, candidates := derived.resolve(testCase.subject)
			primary := candidates[0]
			row := githubWorkItemTeamAttributionRow{Source: primary.Source, Evidence: primary.Evidence}
			if got := githubWorkItemTeamAttributionMetricSource(row); got != testCase.wantMetric {
				t.Fatalf("metric source = %q, want %q (evidence=%q)", got, testCase.wantMetric, primary.Evidence)
			}
		})
	}
}
