//go:build integration

package teamattribution

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Only an ACTIVE team takes part in attribution, and the rule is the same for
// every provider: a team whose newest `teams` row is inactive takes no work
// item by a project key, by its own id or by a native team key. The newest row
// decides, so a team set inactive and then active again takes its items.
//
// The store is built from the real migration chain. Each team is written as an
// active row and, where the case says so, a newer row under the same sort key.
// Merges are stopped so that the rows stay physical: a loader that reads one
// of the older rows is seen. The rows are read through the real loader and
// resolved by the real context.
func TestAnInactiveTeamTakesNoWorkItemForEveryProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES teams"); err != nil {
		t.Fatalf("stop merges on teams: %v", err)
	}
	const org = "org-active-teams"
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// states is the is_active value of each row of a team, oldest first.
	teams := []struct {
		name   string
		states []uint8
		taken  bool
	}{
		{"active", []uint8{1}, true},
		{"inactive", []uint8{1, 0}, false},
		{"neveractive", []uint8{0}, false},
		{"activeagain", []uint8{1, 0, 1}, true},
	}
	physical := 0
	for _, provider := range retractionProviders {
		for _, team := range teams {
			id := team.name + "-" + provider
			teamUUID := uuid.New()
			for version, isActive := range team.states {
				at := first.Add(time.Duration(version) * time.Hour)
				if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
					id, teamUUID, "Team "+id, []string{"KEY" + id}, isActive, at, at, org, provider, id); err != nil {
					t.Fatalf("insert team %s: %v", id, err)
				}
				physical++
			}
		}
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams WHERE org_id = ?`, org).Scan(&stored); err != nil {
		t.Fatalf("count teams: %v", err)
	}
	if stored != uint64(physical) {
		t.Fatalf("teams holds %d physical rows, want %d (every version unmerged)", stored, physical)
	}

	facts, err := ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
	if err != nil {
		t.Fatalf("LoadTeams: %v", err)
	}
	loaded := map[string]bool{}
	for _, fact := range facts {
		loaded[fact.Provider+"/"+fact.TeamID] = true
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: facts})

	for _, provider := range retractionProviders {
		for _, team := range teams {
			id := team.name + "-" + provider
			if loaded[provider+"/"+id] != team.taken {
				t.Errorf("%s: LoadTeams loaded = %v, want %v", id, loaded[provider+"/"+id], team.taken)
			}
			projectKey, teamKey := "KEY"+id, id
			for name, subject := range map[string]GithubWorkItemDerivationSubject{
				"by the project key":     {WorkItemID: provider + ":1", Provider: provider, Type: "issue", ProjectKey: &projectKey},
				"by the team id as key":  {WorkItemID: provider + ":2", Provider: provider, Type: "issue", ProjectKey: &teamKey},
				"by the native team key": {WorkItemID: provider + ":3", Provider: provider, Type: "issue", NativeTeamKey: &teamKey},
			} {
				gotTeam, _, candidates := derived.Resolve(subject)
				// A subject no team takes still gets the one candidate that
				// says so; it names no team.
				named := 0
				for _, candidate := range candidates {
					if candidate.TeamID != nil && *candidate.TeamID != "" {
						named++
					}
				}
				switch {
				case team.taken && (gotTeam == nil || *gotTeam != id):
					t.Errorf("%s %s: resolved team = %v, want %s (candidates %+v)", id, name, gotTeam, id, candidates)
				case !team.taken && (gotTeam != nil || named != 0):
					t.Errorf("%s %s: resolved team = %v with %d candidates that name a team, want no team: the team is inactive (candidates %+v)",
						id, name, gotTeam, named, candidates)
				}
			}
		}
	}
}
