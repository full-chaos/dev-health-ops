//go:build integration

package remaining

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

type coOwnerStore struct {
	t      *testing.T
	ctx    context.Context
	conn   driver.Conn
	orgID  string
	writer *WorkItemAttributionClickHouseWriter
	repoID string
}

func newCoOwnerStore(t *testing.T, ctx context.Context) coOwnerStore {
	t.Helper()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES work_item_team_attributions"); err != nil {
		t.Fatalf("stop merges: %v", err)
	}
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	return coOwnerStore{t: t, ctx: ctx, conn: conn, orgID: "org-coowner-" + uuid.NewString(), writer: writer, repoID: uuid.NewString()}
}

func (s coOwnerStore) team(provider, teamID string, keys []string, at time.Time) {
	s.t.Helper()
	if err := s.conn.Exec(s.ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], 1, ?, ?, ?, ?, ?)`,
		teamID, uuid.New(), "Team "+teamID, keys, at, at, s.orgID, provider, teamID); err != nil {
		s.t.Fatalf("insert team %s: %v", teamID, err)
	}
}

func (s coOwnerStore) owns(provider, teamID, projectID, key string, from time.Time, to *time.Time, at time.Time) {
	s.t.Helper()
	if err := s.conn.Exec(s.ctx, `INSERT INTO team_project_ownership
		(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, ?, ?)`,
		s.orgID, provider, teamID, projectID, key, from, to, at); err != nil {
		s.t.Fatalf("insert ownership %s: %v", teamID, err)
	}
}

func (s coOwnerStore) subject(provider, key, projectID string) teamattribution.GithubWorkItemDerivationSubject {
	repoID, projectKey, project := s.repoID, key, projectID
	return teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: provider + ":" + key + "-1", Provider: provider, RepoID: &repoID,
		ProjectKey: &projectKey, ProjectID: &project, OrgID: s.orgID,
	}
}

func (s coOwnerStore) write(computedAt time.Time, subjects ...teamattribution.GithubWorkItemDerivationSubject) {
	s.t.Helper()
	facts, err := LoadWorkItemDerivationFacts(s.ctx, s.conn, s.orgID, computedAt)
	if err != nil {
		s.t.Fatalf("load facts: %v", err)
	}
	bySubject := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	affected := map[string]struct{}{}
	for _, subject := range subjects {
		bySubject[subject.WorkItemID] = subject
		affected[subject.WorkItemID] = struct{}{}
	}
	rows := BuildWorkItemAttributionRows(s.orgID, computedAt, affected, bySubject, teamattribution.NewGitHubWorkItemDerivationContext(facts))
	if _, err := s.writer.WriteAttributions(s.ctx, WorkItemAttributionProducer{
		Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString(),
	}, rows); err != nil {
		s.t.Fatalf("write attributions: %v", err)
	}
}

// Team B owns the item's project through two ownership facts at the top rank
// (project p1 by id, p2 by the same key); team A owns p1. No team holds the
// key in the catalog, so project_ownership decides. Through the real loaders,
// cascade and writer, team B is stored as a co-owner (2) and team A as the
// one primary (1).
func TestACoOwnerWithTwoOwnershipFactsIsStoredAsACoOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newCoOwnerStore(t, ctx)
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var subjects []teamattribution.GithubWorkItemDerivationSubject
	for _, provider := range coOwnerProviders {
		key := "PK" + strings.ToUpper(provider)
		s.owns(provider, "team-a-"+provider, "p1-"+provider, key, opened, nil, opened)
		s.owns(provider, "team-b-"+provider, "p1-"+provider, key, opened, nil, opened)
		s.owns(provider, "team-b-"+provider, "p2-"+provider, key, opened, nil, opened)
		subjects = append(subjects, s.subject(provider, key, "p1-"+provider))
	}
	s.write(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), subjects...)
	for _, subject := range subjects {
		provider := subject.Provider
		stored := latestAttributions(t, ctx, s.conn, s.orgID, subject.WorkItemID)
		if got := teamsWith(stored, "project_ownership", 1); strings.Join(got, ",") != "team-a-"+provider {
			t.Errorf("%s: primary teams = %v, want [team-a-%s]", provider, got, provider)
		}
		if got := teamsWith(stored, "project_ownership", 2); strings.Join(got, ",") != "team-b-"+provider {
			t.Errorf("%s: co-owner teams = %v, want [team-b-%s] (stored: %+v)", provider, got, provider, stored)
		}
	}
}

// A team of another provider that holds the same key string is not an owner
// of the item's project: it is never stored as a co-owner. The teams are read
// in catalog order (provider, id). When the first holder is of the item's
// provider, it is the primary and the other holder of the item's provider is
// a co-owner; when the first holder is of another provider (a Linear item
// whose key a Jira team also holds), it stays the primary as before this
// change and no team is stored as a co-owner.
func TestAKeyOfAnotherProvidersTeamIsNeverStoredAsACoOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newCoOwnerStore(t, ctx)
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	type holder struct{ provider, teamID string }
	var subjects []teamattribution.GithubWorkItemDerivationSubject
	holders := map[string][]holder{}
	for _, provider := range coOwnerProviders {
		other := "linear"
		if provider == "linear" {
			other = "jira"
		}
		key := "XK" + strings.ToUpper(provider)
		for _, h := range []holder{{provider, "x-team-a-" + provider}, {provider, "x-team-b-" + provider}, {other, "x-team-z-" + other + "-for-" + provider}} {
			s.team(h.provider, h.teamID, []string{key}, opened)
			s.owns(h.provider, h.teamID, "xp-"+h.provider+"-"+provider, key, opened, nil, opened)
			holders[provider] = append(holders[provider], h)
		}
		sort.Slice(holders[provider], func(left, right int) bool {
			a, b := holders[provider][left], holders[provider][right]
			if a.provider != b.provider {
				return a.provider < b.provider
			}
			return a.teamID < b.teamID
		})
		subjects = append(subjects, s.subject(provider, key, "xp-"+provider+"-"+provider))
	}
	s.write(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), subjects...)
	for _, subject := range subjects {
		provider := subject.Provider
		stored := latestAttributions(t, ctx, s.conn, s.orgID, subject.WorkItemID)
		first := holders[provider][0]
		if got := teamsWith(stored, "issue_project", 1); strings.Join(got, ",") != first.teamID {
			t.Errorf("%s: primary teams = %v, want the first holder [%s] (unchanged)", provider, got, first.teamID)
		}
		wantCoOwners := ""
		if first.provider == provider {
			wantCoOwners = "x-team-b-" + provider
		}
		if got := strings.Join(teamsWith(stored, "issue_project", 2), ","); got != wantCoOwners {
			t.Errorf("%s: co-owner teams = %q, want %q (stored: %+v)", provider, got, wantCoOwners, stored)
		}
		for _, row := range stored {
			if row.isPrimary == 2 && !strings.HasPrefix(row.teamID, "x-team-a-"+provider) && !strings.HasPrefix(row.teamID, "x-team-b-"+provider) {
				t.Errorf("%s: team of another provider stored as a co-owner: %+v", provider, row)
			}
		}
	}
}

// Team B co-owns the project, then leaves it (its key and its ownership fact
// are closed). The next attribution run writes the item with a newer
// computed_at and without team B. Team B's old co-owner row keeps its own sort
// key, so it stays stored (merges stopped) until a merge; the newest
// computed_at fence of the item, which the team-scoped readers apply, does not
// read it.
func TestATeamThatLeavesTheProjectHasNoCoOwnerRowAfterTheNextRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newCoOwnerStore(t, ctx)
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	left := time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)
	var subjects []teamattribution.GithubWorkItemDerivationSubject
	for _, provider := range coOwnerProviders {
		key := "LK" + strings.ToUpper(provider)
		s.team(provider, "l-team-a-"+provider, []string{key}, opened)
		s.team(provider, "l-team-b-"+provider, []string{key}, opened)
		s.owns(provider, "l-team-a-"+provider, "lp-"+provider, key, opened, nil, opened)
		s.owns(provider, "l-team-b-"+provider, "lp-"+provider, key, opened, nil, opened)
		subjects = append(subjects, s.subject(provider, key, "lp-"+provider))
	}
	s.write(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), subjects...)
	for _, subject := range subjects {
		if got := teamsWith(latestAttributions(t, ctx, s.conn, s.orgID, subject.WorkItemID), "issue_project", 2); strings.Join(got, ",") != "l-team-b-"+subject.Provider {
			t.Fatalf("%s run 1: co-owner teams = %v, want [l-team-b-%s]", subject.Provider, got, subject.Provider)
		}
	}
	for _, provider := range coOwnerProviders {
		key := "LK" + strings.ToUpper(provider)
		s.team(provider, "l-team-b-"+provider, []string{}, left)
		s.owns(provider, "l-team-b-"+provider, "lp-"+provider, key, opened, &left, left)
	}
	s.write(time.Date(2026, 2, 11, 0, 0, 0, 0, time.UTC), subjects...)
	for _, subject := range subjects {
		provider := subject.Provider
		b := "l-team-b-" + provider
		for _, row := range latestAttributions(t, ctx, s.conn, s.orgID, subject.WorkItemID) {
			if row.teamID == b {
				t.Errorf("%s run 2: team B left the project but has a row: %+v", provider, row)
			}
		}
		var stale uint64
		if err := s.conn.QueryRow(ctx, `SELECT count() FROM work_item_team_attributions
			WHERE org_id = ? AND work_item_id = ? AND team_id = ? AND is_primary = 2`,
			s.orgID, subject.WorkItemID, b).Scan(&stale); err != nil {
			t.Fatalf("count stale rows: %v", err)
		}
		if stale != 1 {
			t.Fatalf("%s: %d stored co-owner rows of team B, want its old row still stored (merges stopped), so the fence is what hides it", provider, stale)
		}
	}
}
