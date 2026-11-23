//go:build integration

package testopsrisk

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13814 on a migrated ClickHouse. Three repositories of one org, each with a different value in every table
// the answer reads (release confidence, quality drag, stability), so one statement that lost its scope clause moves
// the answer of its own site: team-1 owns web and api, team-2 owns api and lib (api is co-owned and counts whole
// for each owner), and a second org holds a repository with the same id.
func TestRealClickHouse_TestopsRiskScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	options, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-1"
	synced := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, r := range []struct{ id, name, org string }{
		{rA, "acme/web", org}, {rB, "acme/api", org}, {rC, "acme/lib", org}, {rA, "other/web", "org-2"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, r.id, r.name, "github", r.org, synced, synced); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range []struct{ team, repo, name string }{
		{"team-1", rA, "acme/web"}, {"team-1", rB, "acme/api"}, {"team-2", rB, "acme/api"}, {"team-2", rC, "acme/lib"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			org, "github", o.team, o.repo, o.name, "exact", "inferred", uint8(0), uint16(0), int32(0), synced, nil, synced); err != nil {
			t.Fatal(err)
		}
	}
	const f = `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`
	for _, v := range []struct {
		repo       string
		conf, drag float64
		stability  float64
		computed   string
	}{
		{rA, 0.8, 2, 0.9, "2026-01-01 02:00:00"},
		{rB, 0.6, 3, 0.7, "2026-01-01 02:00:00"},
		{rC, 0.2, 10, 0.1, "2026-01-01 02:00:00"},
	} {
		conf(t, ctx, conn, org, v.repo, "2026-01-01", v.conf, f, v.computed)
		drag(t, ctx, conn, org, v.repo, "2026-01-01", v.drag, 1, 1, 0.5, 0.5, v.computed)
		stab(t, ctx, conn, org, v.repo, "2026-01-01", v.stability, v.computed)
	}
	// another org, same repository id: never read under org-1
	conf(t, ctx, conn, "org-2", rA, "2026-01-01", 0.01, f, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-2", rA, "2026-01-01", 99, 1, 1, 0.5, 0.5, "2026-01-01 02:00:00")
	stab(t, ctx, conn, "org-2", rA, "2026-01-01", 0.01, "2026-01-01 02:00:00")

	window := func(repoIDs, teamIDs []string) model.TestOpsRiskInput {
		return model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31"), RepoIds: repoIDs, TeamIds: teamIDs}
	}
	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-9 }
	cases := []struct {
		name            string
		repos, teams    []string
		conf, drag, stb float64 // 0 with none = the answer is empty
		quadrant        []string
		none            bool
	}{
		{name: "no scope", conf: (0.8 + 0.6 + 0.2) / 3, drag: 15, stb: (0.9 + 0.7 + 0.1) / 3, quadrant: []string{"acme/lib", "acme/api", "acme/web"}},
		{name: "empty lists are no scope", repos: []string{}, teams: []string{}, conf: (0.8 + 0.6 + 0.2) / 3, drag: 15, stb: (0.9 + 0.7 + 0.1) / 3, quadrant: []string{"acme/lib", "acme/api", "acme/web"}},
		{name: "blank team id is the organization", teams: []string{""}, conf: (0.8 + 0.6 + 0.2) / 3, drag: 15, stb: (0.9 + 0.7 + 0.1) / 3, quadrant: []string{"acme/lib", "acme/api", "acme/web"}},
		{name: "repository by name", repos: []string{"acme/web"}, conf: 0.8, drag: 2, stb: 0.9, quadrant: []string{"acme/web"}},
		{name: "repository by id", repos: []string{rB}, conf: 0.6, drag: 3, stb: 0.7, quadrant: []string{"acme/api"}},
		{name: "blank repository id matches nothing", repos: []string{""}, none: true},
		{name: "team-1 owns web and api", teams: []string{"team-1"}, conf: 0.7, drag: 5, stb: 0.8, quadrant: []string{"acme/api", "acme/web"}},
		{name: "team-2 owns api and lib: api counts whole", teams: []string{"team-2"}, conf: 0.4, drag: 13, stb: 0.4, quadrant: []string{"acme/lib", "acme/api"}},
		{name: "both teams: the union, each repository once", teams: []string{"team-1", "team-2"}, conf: (0.8 + 0.6 + 0.2) / 3, drag: 15, stb: (0.9 + 0.7 + 0.1) / 3, quadrant: []string{"acme/lib", "acme/api", "acme/web"}},
		{name: "team and repository are both applied", teams: []string{"team-1"}, repos: []string{"acme/web", "acme/lib"}, conf: 0.8, drag: 2, stb: 0.9, quadrant: []string{"acme/web"}},
		{name: "a team that owns nothing", teams: []string{"team-none"}, none: true},
		{name: "a team and a repository it does not own", teams: []string{"team-1"}, repos: []string{"acme/lib"}, none: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(ctx, client, org, window(tc.repos, tc.teams))
			if err != nil {
				t.Fatal(err)
			}
			if tc.none {
				if got.ReleaseConfidence != nil || got.QualityDragHours != nil || got.PipelineStability != nil || len(got.Timeseries) != 0 || len(got.QuadrantData) != 0 {
					t.Fatalf("want an empty answer, got %#v", got)
				}
				return
			}
			if !near(got.ReleaseConfidence, tc.conf) {
				t.Errorf("release confidence %v, want %v", got.ReleaseConfidence, tc.conf)
			}
			if !near(got.QualityDragHours, tc.drag) {
				t.Errorf("quality drag %v, want %v", got.QualityDragHours, tc.drag)
			}
			if !near(got.PipelineStability, tc.stb) {
				t.Errorf("stability %v, want %v", got.PipelineStability, tc.stb)
			}
			var names []string
			for _, q := range got.QuadrantData {
				if q.Name != nil {
					names = append(names, *q.Name)
				}
			}
			if len(names) != len(tc.quadrant) {
				t.Fatalf("quadrant %v, want %v", names, tc.quadrant)
			}
			for i := range names {
				if names[i] != tc.quadrant[i] {
					t.Errorf("quadrant %v, want %v", names, tc.quadrant)
					break
				}
			}
		})
	}
}
