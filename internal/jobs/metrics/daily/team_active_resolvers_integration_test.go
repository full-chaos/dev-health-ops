//go:build integration

package daily

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// The acceptance case of the active-team rule for the three team resolvers of
// the daily metric families, on the schema of the migration chain:
//
//   - LoadWellbeingTeams (team_wellbeing, ic_finalize, team_cognitive_load,
//     team_complexity, compounding_risk_team);
//   - LoadAIImpactTeams (ai_impact);
//   - teamownership.AuthoritativeOwnerByRepo (testops, ai_impact and the three
//     finalize families above).
//
// A team is replaced by a provider-keyed id: the row of the bare id goes
// inactive and the keyed id is active. A resolver must then give no pattern,
// no member and no repository to the bare id. The newest row decides, so a
// team that is set active again takes part again. With no inactive team the
// resolvers return every team, as they did before the rule.
//
// The file uses no symbol of the rule, so the same file runs on a tree
// without it.

const teamActiveResolverOrg = "00000000-0000-4000-8000-00000000ac71"

func seedTeamActiveResolverTeam(
	t *testing.T, ctx context.Context, conn driver.Conn, id string, active bool, at time.Time,
) {
	t.Helper()
	flag := uint8(0)
	if active {
		flag = 1
	}
	if err := conn.Exec(ctx, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), "Team "+id,
		[]string{"dev@example.com"}, []string{"acme/*"}, at, at, teamActiveResolverOrg, "github", flag,
	); err != nil {
		t.Fatalf("insert team %s: %v", id, err)
	}
}

func seedTeamActiveResolverOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn, teamID string, repoID uuid.UUID, repoName string,
	primary uint8, at time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', ?, ?, ?, ?)`,
		teamActiveResolverOrg, "github", teamID, repoID, repoName, primary, uint16(10), at, at,
	); err != nil {
		t.Fatalf("insert ownership of %s by %s: %v", repoName, teamID, err)
	}
}

// teamActiveResolverState is what the three resolvers return.
type teamActiveResolverState struct {
	wellbeingTeams []string
	aiImpactTeams  []string
	owners         map[string]string
}

func readTeamActiveResolverState(
	t *testing.T, ctx context.Context, conn driver.Conn, asOf time.Time,
) teamActiveResolverState {
	t.Helper()
	var state teamActiveResolverState
	wellbeing, err := LoadWellbeingTeams(ctx, conn, teamActiveResolverOrg)
	if err != nil {
		t.Fatalf("LoadWellbeingTeams: %v", err)
	}
	for _, team := range wellbeing {
		state.wellbeingTeams = append(state.wellbeingTeams, team.ID)
	}
	sort.Strings(state.wellbeingTeams)
	aiImpact, err := LoadAIImpactTeams(ctx, conn, teamActiveResolverOrg)
	if err != nil {
		t.Fatalf("LoadAIImpactTeams: %v", err)
	}
	for _, team := range aiImpact {
		state.aiImpactTeams = append(state.aiImpactTeams, team.ID)
	}
	sort.Strings(state.aiImpactTeams)
	state.owners, err = teamownership.AuthoritativeOwnerByRepo(ctx, conn, teamActiveResolverOrg, asOf)
	if err != nil {
		t.Fatalf("AuthoritativeOwnerByRepo: %v", err)
	}
	return state
}

func (state teamActiveResolverState) check(t *testing.T, phase string, teams []string, owners map[string]string) {
	t.Helper()
	if !reflect.DeepEqual(state.wellbeingTeams, teams) {
		t.Errorf("%s: LoadWellbeingTeams returns %v, want %v", phase, state.wellbeingTeams, teams)
	}
	if !reflect.DeepEqual(state.aiImpactTeams, teams) {
		t.Errorf("%s: LoadAIImpactTeams returns %v, want %v", phase, state.aiImpactTeams, teams)
	}
	if !reflect.DeepEqual(state.owners, owners) {
		t.Errorf("%s: AuthoritativeOwnerByRepo returns %v, want %v", phase, fmt.Sprint(state.owners), fmt.Sprint(owners))
	}
}

func TestNoTeamResolverResolvesToAnInactiveTeam(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	api := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	web := uuid.MustParse("00000000-0000-4000-8000-0000000000a2")
	for id, name := range map[uuid.UUID]string{api: "acme/api", web: "acme/web"} {
		if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)",
			id, name, teamActiveResolverOrg, "github", t0); err != nil {
			t.Fatalf("insert repo %s: %v", name, err)
		}
	}
	seedTeamActiveResolverTeam(t, ctx, conn, "platform", true, t0)
	seedTeamActiveResolverTeam(t, ctx, conn, "github:platform", true, t0)
	// The bare id is the primary owner of acme/api and the one owner of
	// acme/web; the keyed id owns acme/api with a lower rank.
	seedTeamActiveResolverOwnership(t, ctx, conn, "platform", api, "acme/api", 1, t0)
	seedTeamActiveResolverOwnership(t, ctx, conn, "github:platform", api, "acme/api", 0, t0)
	seedTeamActiveResolverOwnership(t, ctx, conn, "platform", web, "acme/web", 1, t0)
	asOf := t0.Add(240 * time.Hour)

	// No inactive team: every team takes part, and the ranking of the
	// ownership rows decides.
	readTeamActiveResolverState(t, ctx, conn, asOf).check(t, "no inactive team",
		[]string{"github:platform", "platform"},
		map[string]string{api.String(): "platform", web.String(): "platform"})

	// The bare id is replaced: its newest row is inactive.
	seedTeamActiveResolverTeam(t, ctx, conn, "platform", false, t0.Add(24*time.Hour))
	readTeamActiveResolverState(t, ctx, conn, asOf).check(t, "the bare id is inactive",
		[]string{"github:platform"},
		// acme/api goes to the lower-ranked row of the active team; acme/web
		// has no active owner and is left to the caller's pattern fallback.
		map[string]string{api.String(): "github:platform"})

	// The newest row decides: the bare id is set active again.
	seedTeamActiveResolverTeam(t, ctx, conn, "platform", true, t0.Add(48*time.Hour))
	readTeamActiveResolverState(t, ctx, conn, asOf).check(t, "the bare id is active again",
		[]string{"github:platform", "platform"},
		map[string]string{api.String(): "platform", web.String(): "platform"})
}
