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

// CHAOS-13814, repoIds on a migrated ClickHouse: how a repository named in repoIds is matched against the catalog.
// Each case has an organization of its own, so one store serves the three.
func TestRealClickHouse_TestopsRiskRepoIdsMatch(t *testing.T) {
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
	// A merge would collapse the stored versions of a catalog row before the read and hide a missing latest-row pick.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES repos"); err != nil {
		t.Fatal(err)
	}

	repo := func(org, id, name string, synced time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, id, name, "github", org, synced, synced); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(org, id string, c, d, s float64) {
		t.Helper()
		const f = `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`
		conf(t, ctx, conn, org, id, "2026-01-01", c, f, "2026-01-01 02:00:00")
		drag(t, ctx, conn, org, id, "2026-01-01", d, 1, 1, 0.5, 0.5, "2026-01-01 02:00:00")
		stab(t, ctx, conn, org, id, "2026-01-01", s, "2026-01-01 02:00:00")
	}
	window := func(repoIDs ...string) model.TestOpsRiskInput {
		return model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31"), RepoIds: repoIDs}
	}
	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-9 }
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	t.Run("a name reused after a rename admits only the repository that holds it now", func(t *testing.T) {
		const org = "org-versions"
		// A was acme/api and is acme/api-v2 now (two stored versions); B took the name acme/api.
		repo(org, rA, "acme/api", t1)
		repo(org, rA, "acme/api-v2", t2)
		repo(org, rB, "acme/api", t2)
		seed(org, rA, 0.8, 2, 0.9)
		seed(org, rB, 0.4, 7, 0.3)
		for name, want := range map[string]struct {
			conf, drag, stab float64
			quadrant         string
		}{"acme/api": {0.4, 7, 0.3, "acme/api"}, "acme/api-v2": {0.8, 2, 0.9, "acme/api-v2"}} {
			got, err := Resolve(ctx, client, org, window(name))
			if err != nil {
				t.Fatal(err)
			}
			if !near(got.ReleaseConfidence, want.conf) || !near(got.QualityDragHours, want.drag) || !near(got.PipelineStability, want.stab) {
				t.Errorf("%s: confidence %v drag %v stability %v, want %v %v %v", name, got.ReleaseConfidence, got.QualityDragHours, got.PipelineStability, want.conf, want.drag, want.stab)
			}
			if len(got.QuadrantData) != 1 || got.QuadrantData[0].Name == nil || *got.QuadrantData[0].Name != want.quadrant {
				t.Errorf("%s: quadrant %#v, want only %s", name, got.QuadrantData, want.quadrant)
			}
		}
	})

	t.Run("a blank repoIds entry is no name: it matches no repository, not the unnamed one", func(t *testing.T) {
		const org = "org-blank"
		repo(org, rA, "", t1)
		repo(org, rB, "acme/web", t1)
		seed(org, rA, 0.8, 2, 0.9)
		seed(org, rB, 0.4, 7, 0.3)
		for name, ids := range map[string][]string{"blank only": {""}, "blank and a name": {"", "acme/web"}} {
			got, err := Resolve(ctx, client, org, window(ids...))
			if err != nil {
				t.Fatal(err)
			}
			if name == "blank only" {
				if got.ReleaseConfidence != nil || got.QualityDragHours != nil || len(got.QuadrantData) != 0 {
					t.Errorf("%s: want an empty answer, got confidence %v drag %v quadrant %#v", name, got.ReleaseConfidence, got.QualityDragHours, got.QuadrantData)
				}
				continue
			}
			if !near(got.ReleaseConfidence, 0.4) || !near(got.QualityDragHours, 7) || len(got.QuadrantData) != 1 {
				t.Errorf("%s: want only acme/web, got confidence %v drag %v quadrant %#v", name, got.ReleaseConfidence, got.QualityDragHours, got.QuadrantData)
			}
		}
	})

	t.Run("a repository id with no catalog row still selects its risk rows", func(t *testing.T) {
		const org = "org-missing"
		repo(org, rB, "acme/web", t1)
		// rC has risk rows and no catalog row: the quadrant names it by its id.
		seed(org, rB, 0.4, 7, 0.3)
		seed(org, rC, 0.9, 1, 0.8)
		all, err := Resolve(ctx, client, org, window())
		if err != nil {
			t.Fatal(err)
		}
		listed := false
		for _, q := range all.QuadrantData {
			listed = listed || q.ID == rC
		}
		if !listed {
			t.Fatalf("the quadrant does not list %s: %#v", rC, all.QuadrantData)
		}
		got, err := Resolve(ctx, client, org, window(rC))
		if err != nil {
			t.Fatal(err)
		}
		if !near(got.ReleaseConfidence, 0.9) || !near(got.QualityDragHours, 1) || !near(got.PipelineStability, 0.8) || len(got.QuadrantData) != 1 || got.QuadrantData[0].ID != rC {
			t.Errorf("an id the quadrant returned selects nothing: confidence %v drag %v stability %v quadrant %#v", got.ReleaseConfidence, got.QualityDragHours, got.PipelineStability, got.QuadrantData)
		}
	})
}
