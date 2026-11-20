//go:build integration

package issuepredges_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTextParseReadsTheCurrentTextOfAResyncedPullRequest: B8 of CHAOS-14285. Org A's PR 9 named API-1 in its old
// title and does not in its current title (two git_pull_requests versions, merges stopped): the text parse reads one
// row per pull request and makes no link to API-1. Org B, on the same repository id and number, names API-1 in its
// one version and keeps its link.
func TestTextParseReadsTheCurrentTextOfAResyncedPullRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES git_pull_requests`); err != nil {
		t.Fatal(err)
	}
	const (
		orgA = "00000000-0000-4000-8000-0000001428a0"
		orgB = "00000000-0000-4000-8000-0000001428b0"
		repo = "00000000-0000-4000-8000-000000142854"
	)
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	pr := func(org, title, synced string) {
		exec(`INSERT INTO git_pull_requests (repo_id, number, title, created_at, org_id, last_synced)
VALUES (toUUID('` + repo + `'), 9, '` + title + `', toDateTime64('2026-08-01 10:00:00', 3, 'UTC'), '` + org + `', toDateTime64('` + synced + `', 3, 'UTC'))`)
	}
	for _, org := range []string{orgA, orgB} {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, org_id, last_synced)
VALUES (toUUID('` + repo + `'), 'jira:API-1', 'jira', '` + org + `', toDateTime64('2026-08-01 10:00:00', 3, 'UTC'))`)
	}
	pr(orgA, "API-1 fix login", "2026-08-24 13:00:00")
	pr(orgA, "Fix login", "2026-08-25 13:00:00")
	pr(orgB, "API-1 fix login", "2026-08-24 13:00:00")

	loader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	check := func(phase string) {
		for _, c := range []struct {
			org   string
			links int
		}{{orgA, 0}, {orgB, 1}} {
			prs, items, err := loader.LoadTextParseInputs(ctx, c.org, issuepredges.Window{})
			if err != nil {
				t.Fatal(err)
			}
			var titles []string
			for _, p := range prs {
				titles = append(titles, p.Title)
			}
			result := issuepredges.DeriveTextParseEdges(prs, items, issueprlinks.NewNativeLinks(nil), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC))
			t.Logf("%s: org %s pr rows %q links %d", phase, c.org[len(c.org)-4:], titles, len(result.Links))
			if len(prs) != 1 || len(result.Links) != c.links {
				t.Errorf("%s: org %s %d pr rows %q and %d links, want 1 row and %d links", phase, c.org[len(c.org)-4:], len(prs), titles, len(result.Links), c.links)
			}
		}
	}
	check("separate parts")
	exec(`SYSTEM START MERGES git_pull_requests`)
	exec(`OPTIMIZE TABLE git_pull_requests FINAL`)
	check("after OPTIMIZE FINAL")
}
