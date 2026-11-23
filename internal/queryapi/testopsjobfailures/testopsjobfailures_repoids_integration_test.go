//go:build integration

package testopsjobfailures

import (
	"context"
	"sort"
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

func jobNames(groups []model.TestOpsJobFailureGroup) string {
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.JobName)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// CHAOS-14517 on a migrated ClickHouse: how a repository named in repoIds is matched against the catalog. Each case
// has an organization of its own, so one store serves the three. Each repository has one failing job named for it.
func TestRealClickHouse_JobFailuresRepoIdsMatch(t *testing.T) {
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
		repoA = "14517000-0000-4000-8000-0000000000a1"
		repoB = "14517000-0000-4000-8000-0000000000b1"
		repoC = "14517000-0000-4000-8000-0000000000c1"
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
	// seed: the repository has one failing job named jobName.
	seed := func(org, id, jobName string) {
		exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, pipeline_name, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			org, id, id+"-run", "CI", "github", "completed", inWindow, t2)
		exec(`INSERT INTO ci_job_runs (org_id, repo_id, run_id, job_id, job_name, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			org, id, id+"-run", id+"-job", jobName, "failure", inWindow, t2)
	}
	since := graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))

	t.Run("a name reused after a rename admits only the repository that holds it now", func(t *testing.T) {
		const org = "org-versions"
		// A was acme/api and is acme/api-v2 now (two stored versions); B took the name acme/api.
		repo(org, repoA, "acme/api", t1)
		repo(org, repoA, "acme/api-v2", t2)
		repo(org, repoB, "acme/api", t2)
		seed(org, repoA, "job-of-a")
		seed(org, repoB, "job-of-b")
		got, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/api"}}, 20)
		if err != nil {
			t.Fatal(err)
		}
		if names := jobNames(got.Groups); names != "job-of-b" {
			t.Errorf("acme/api: groups %q, want only its holder B: job-of-b", names)
		}
		renamed, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/api-v2"}}, 20)
		if err != nil {
			t.Fatal(err)
		}
		if names := jobNames(renamed.Groups); names != "job-of-a" {
			t.Errorf("acme/api-v2: groups %q, want A: job-of-a", names)
		}
	})

	t.Run("a blank repoIds entry is no name: it matches no repository, not the unnamed one", func(t *testing.T) {
		const org = "org-blank"
		repo(org, repoA, "", t1)
		repo(org, repoB, "acme/web", t1)
		seed(org, repoA, "job-of-a")
		seed(org, repoB, "job-of-b")
		blank, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{""}}, 20)
		if err != nil {
			t.Fatal(err)
		}
		if names := jobNames(blank.Groups); names != "" {
			t.Errorf("blank only: groups %q, want an empty answer", names)
		}
		mixed, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"", "acme/web"}}, 20)
		if err != nil {
			t.Fatal(err)
		}
		if names := jobNames(mixed.Groups); names != "job-of-b" {
			t.Errorf("blank and a name: groups %q, want only acme/web: job-of-b", names)
		}
	})

	t.Run("a repository id with no catalog row still selects its rows", func(t *testing.T) {
		const org = "org-missing"
		repo(org, repoB, "acme/web", t1)
		seed(org, repoB, "job-of-b")
		seed(org, repoC, "job-of-c")
		got, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{repoC}}, 20)
		if err != nil {
			t.Fatal(err)
		}
		if names := jobNames(got.Groups); names != "job-of-c" {
			t.Errorf("an id with no catalog row: groups %q, want its own: job-of-c", names)
		}
	})
}
