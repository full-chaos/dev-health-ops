//go:build integration

package remaining

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// A project key string is not a link across providers. Through the real fact
// loaders, the real cascade and the real writer into a store built from the
// migration chain, for every provider of the item:
//   - a key held by teams of every other provider and by a team with no
//     provider (all of them sort first in the catalog) and by two teams of
//     the item's provider: the first team of the item's provider is the one
//     issue_project primary row, the second its co-owner, the others no row;
//   - a key held only by teams of other providers and a team with no
//     provider: no issue_project row; the item's project ownership by a team
//     of its provider is the primary;
//   - a native team key held by a team of another provider that sorts first
//     and by a team of the item's provider: the native_team primary is the
//     team of the item's provider.
func TestProjectAndNativeKeysAttributeOnlyToTeamsOfTheItemsProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	orgID := "org-provider-scope-" + uuid.NewString()
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertTeam := func(provider, teamID string, keys []string) {
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], 1, ?, ?, ?, ?, ?)`,
			teamID, uuid.New(), "Team "+teamID, keys, opened, opened, orgID, provider, teamID); err != nil {
			t.Fatalf("insert team %s/%s: %v", provider, teamID, err)
		}
	}
	insertOwnership := func(provider, teamID, projectKey string) {
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, NULL, ?)`,
			orgID, provider, teamID, "proj-"+strings.ToLower(projectKey), projectKey, opened, opened); err != nil {
			t.Fatalf("insert ownership %s/%s: %v", provider, teamID, err)
		}
	}
	others := func(provider string) []string {
		var result []string
		for _, other := range coOwnerProviders {
			if other != provider {
				result = append(result, other)
			}
		}
		return result
	}
	repoIDText := uuid.New().String()
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	affected := map[string]struct{}{}
	addSubject := func(subject teamattribution.GithubWorkItemDerivationSubject) {
		subject.OrgID, subject.RepoID = orgID, &repoIDText
		subjects[subject.WorkItemID] = subject
		affected[subject.WorkItemID] = struct{}{}
	}
	for _, provider := range coOwnerProviders {
		upper := strings.ToUpper(provider)
		mixed, only, native := "MIX"+upper, "ONLY"+upper, "NAT"+upper
		insertTeam("", "0-admin-"+provider, []string{mixed, only, native})
		for _, other := range others(provider) {
			insertTeam(other, "0-"+other+"-holds-"+provider, []string{mixed, only, native})
		}
		insertTeam(provider, "m-team-a-"+provider, []string{mixed})
		insertTeam(provider, "m-team-b-"+provider, []string{mixed})
		insertTeam(provider, "own-"+provider, nil)
		insertOwnership(provider, "own-"+provider, only)
		insertTeam(provider, "nat-"+provider, []string{native})

		mixedKey, onlyKey, nativeKey := mixed, only, native
		addSubject(teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":" + mixed + "-1", Provider: provider, ProjectKey: &mixedKey})
		onlyProjectID := "proj-" + strings.ToLower(only)
		addSubject(teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":" + only + "-1", Provider: provider, ProjectKey: &onlyKey, ProjectID: &onlyProjectID})
		addSubject(teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":" + native + "-1", Provider: provider, NativeTeamKey: &nativeKey})
	}

	computedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	facts, err := LoadWorkItemDerivationFacts(ctx, conn, orgID, computedAt)
	if err != nil {
		t.Fatalf("load facts: %v", err)
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
	rows := BuildWorkItemAttributionRows(orgID, computedAt, affected, subjects, derived)
	if _, err := writer.WriteAttributions(ctx, WorkItemAttributionProducer{
		Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString(),
	}, rows); err != nil {
		t.Fatalf("write attributions: %v", err)
	}

	ownTeam := func(provider, teamID string) bool {
		return strings.HasSuffix(teamID, "-"+provider) && !strings.HasPrefix(teamID, "0-")
	}
	for _, provider := range coOwnerProviders {
		upper := strings.ToUpper(provider)
		check := func(id, source, wantPrimary, wantCoOwner string) {
			t.Helper()
			stored := latestAttributions(t, ctx, conn, orgID, id)
			var primaries []storedAttribution
			for _, row := range stored {
				if row.isPrimary == 1 {
					primaries = append(primaries, row)
				}
				if row.teamID != "" && !ownTeam(provider, row.teamID) {
					t.Errorf("%s: row of a team that is not of provider %s: %+v", id, provider, row)
				}
				if strings.TrimSpace(row.evidence) == "" || row.source == "" {
					t.Errorf("%s: row without provenance: %+v", id, row)
				}
			}
			if len(primaries) != 1 || primaries[0].teamID != wantPrimary || primaries[0].source != source {
				t.Errorf("%s: primary rows = %+v, want one %s row of %s", id, primaries, source, wantPrimary)
			}
			if got := strings.Join(teamsWith(stored, source, 2), ","); got != wantCoOwner {
				t.Errorf("%s: co-owner teams = %q, want %q", id, got, wantCoOwner)
			}
		}
		check(provider+":MIX"+upper+"-1", "issue_project", "m-team-a-"+provider, "m-team-b-"+provider)
		check(provider+":ONLY"+upper+"-1", "project_ownership", "own-"+provider, "")
		if got := teamsWith(latestAttributions(t, ctx, conn, orgID, provider+":ONLY"+upper+"-1"), "issue_project", 1); len(got) != 0 {
			t.Errorf("%s: issue_project primary rows of %v, want none", provider, got)
		}
		check(provider+":NAT"+upper+"-1", "native_team", "nat-"+provider, "")
	}
}
