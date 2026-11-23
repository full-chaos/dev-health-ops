//go:build integration

package testopswindow

import (
	"context"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func caseTotal(r *model.TestOpsTestWindowResult) int {
	if r.Cases == nil {
		return 0
	}
	return r.Cases.Total
}

// CHAOS-14517 on a migrated ClickHouse: how a repository named in repoIds is matched against the catalog. Each case
// has an organization of its own, so one store serves the three.
func TestRealClickHouse_TestWindowRepoIdsMatch(t *testing.T) {
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
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	// A merge would collapse the stored versions of a catalog row before the read and hide a missing latest-row pick.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES repos"); err != nil {
		t.Fatal(err)
	}

	const (
		repoA = "14517000-0000-4000-8000-00000000000a"
		repoB = "14517000-0000-4000-8000-00000000000b"
		repoC = "14517000-0000-4000-8000-00000000000c"
	)
	t1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	inWindow := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	repo := func(org, id, name string, synced time.Time) {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, id, name, "github", org, synced, synced)
	}
	// seed: n completed CI runs and one stored day of cases (total cases = cases) for the repository.
	seed := func(org, id string, runs int, cases uint32) {
		for i := 0; i < runs; i++ {
			exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, pipeline_name, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				org, id, id+"-run-"+string(rune('a'+i)), "CI", "github", "success", inWindow, t2)
		}
		exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			org, id, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC), cases, cases, uint32(0), t2)
	}
	since := graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))

	t.Run("a name reused after a rename admits only the repository that holds it now", func(t *testing.T) {
		const org = "org-versions"
		// A was acme/api and is acme/api-v2 now (two stored versions); B took the name acme/api.
		repo(org, repoA, "acme/api", t1)
		repo(org, repoA, "acme/api-v2", t2)
		repo(org, repoB, "acme/api", t2)
		seed(org, repoA, 2, 100)
		seed(org, repoB, 3, 7)
		got, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/api"}})
		if err != nil {
			t.Fatal(err)
		}
		if got.Coverage.CompletedRuns != 3 || caseTotal(got) != 7 {
			t.Errorf("acme/api: %d completed runs, %d cases, want only its holder B: 3 and 7", got.Coverage.CompletedRuns, caseTotal(got))
		}
		renamed, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/api-v2"}})
		if err != nil {
			t.Fatal(err)
		}
		if renamed.Coverage.CompletedRuns != 2 || caseTotal(renamed) != 100 {
			t.Errorf("acme/api-v2: %d completed runs, %d cases, want A: 2 and 100", renamed.Coverage.CompletedRuns, caseTotal(renamed))
		}
	})

	t.Run("a blank repoIds entry is no name: it matches no repository, not the unnamed one", func(t *testing.T) {
		const org = "org-blank"
		repo(org, repoA, "", t1)
		repo(org, repoB, "acme/web", t1)
		seed(org, repoA, 2, 100)
		seed(org, repoB, 3, 7)
		blank, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{""}})
		if err != nil {
			t.Fatal(err)
		}
		if blank.Coverage.CompletedRuns != 0 || caseTotal(blank) != 0 {
			t.Errorf("blank only: %d completed runs, %d cases, want an empty answer", blank.Coverage.CompletedRuns, caseTotal(blank))
		}
		mixed, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"", "acme/web"}})
		if err != nil {
			t.Fatal(err)
		}
		if mixed.Coverage.CompletedRuns != 3 || caseTotal(mixed) != 7 {
			t.Errorf("blank and a name: %d completed runs, %d cases, want only acme/web: 3 and 7", mixed.Coverage.CompletedRuns, caseTotal(mixed))
		}
	})

	t.Run("a repository id with no catalog row still selects its rows", func(t *testing.T) {
		const org = "org-missing"
		repo(org, repoB, "acme/web", t1)
		seed(org, repoB, 3, 7)
		seed(org, repoC, 4, 50)
		got, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{repoC}})
		if err != nil {
			t.Fatal(err)
		}
		if got.Coverage.CompletedRuns != 4 || caseTotal(got) != 50 {
			t.Errorf("an id with no catalog row: %d completed runs, %d cases, want its own: 4 and 50", got.Coverage.CompletedRuns, caseTotal(got))
		}
	})

	t.Run("an id or a name of another organization never selects from this one", func(t *testing.T) {
		const org, other = "org-own", "org-other"
		repo(org, repoA, "acme/a", t1)
		repo(other, repoB, "acme/b", t1)
		repo(other, repoC, "acme/a", t1) // the same name in the other organization
		seed(org, repoA, 2, 100)
		seed(other, repoB, 3, 7)
		seed(other, repoC, 4, 50)
		for name, ids := range map[string][]string{"name of the other org": {"acme/b"}, "id of the other org": {repoB, repoC}} {
			got, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: ids})
			if err != nil {
				t.Fatal(err)
			}
			if got.Coverage.CompletedRuns != 0 || caseTotal(got) != 0 {
				t.Errorf("%s: %d completed runs, %d cases, want an empty answer", name, got.Coverage.CompletedRuns, caseTotal(got))
			}
		}
		same, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/a"}})
		if err != nil {
			t.Fatal(err)
		}
		if same.Coverage.CompletedRuns != 2 || caseTotal(same) != 100 {
			t.Errorf("a name both organizations hold: %d completed runs, %d cases, want this organization's own: 2 and 100", same.Coverage.CompletedRuns, caseTotal(same))
		}
	})

	t.Run("a blank entry beside an id drops out; an id is matched as sent, so an upper-case UUID selects nothing", func(t *testing.T) {
		const org = "org-mixed"
		repo(org, repoA, "acme/a", t1)
		repo(org, repoB, "acme/b", t1)
		seed(org, repoA, 2, 100)
		seed(org, repoB, 3, 7)
		mixed, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"", repoB}})
		if err != nil {
			t.Fatal(err)
		}
		if mixed.Coverage.CompletedRuns != 3 || caseTotal(mixed) != 7 {
			t.Errorf("blank and an id: %d completed runs, %d cases, want only that repository: 3 and 7", mixed.Coverage.CompletedRuns, caseTotal(mixed))
		}
		upper, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{strings.ToUpper(repoB)}})
		if err != nil {
			t.Fatal(err)
		}
		if upper.Coverage.CompletedRuns != 0 || caseTotal(upper) != 0 {
			t.Errorf("an upper-case id: %d completed runs, %d cases; ids are matched as sent (exact text), so want an empty answer", upper.Coverage.CompletedRuns, caseTotal(upper))
		}
	})
}
