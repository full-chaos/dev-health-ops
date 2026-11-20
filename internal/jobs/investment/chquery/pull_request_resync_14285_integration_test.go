//go:build integration

package chquery

import (
	"testing"
)

// TestFetchPullRequestsReadsTheCurrentVersionOfAResyncedPullRequest: B7 of CHAOS-14285. A pull request retitled on
// its second sync keeps both git_pull_requests versions until a merge (merges stopped). The fetch gives one row per
// pull request, with the current title; org Beta, on the same repository id and number, keeps its own row.
func TestFetchPullRequestsReadsTheCurrentVersionOfAResyncedPullRequest(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES git_pull_requests`); err != nil {
		t.Fatal(err)
	}
	const repo = "00000000-0000-4000-8000-000000142853"
	insert := func(org, title, synced string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, title, created_at, org_id, last_synced)
VALUES (toUUID('`+repo+`'), 7, '`+title+`', toDateTime64('2026-08-01 10:00:00', 3, 'UTC'), '`+org+`', toDateTime64('`+synced+`', 3, 'UTC'))`); err != nil {
			t.Fatal(err)
		}
	}
	// The current version first, the stale one in a later part.
	insert(orgAlpha, "current title", "2026-08-25 13:00:00")
	insert(orgAlpha, "stale title", "2026-08-24 13:00:00")
	insert(orgBeta, "beta title", "2026-08-24 13:00:00")

	check := func(phase string) {
		for _, c := range []struct{ org, want string }{{orgAlpha, "current title"}, {orgBeta, "beta title"}} {
			prs, err := reader.FetchPullRequests(ctx, map[string][]uint32{repo: {7}}, c.org)
			if err != nil {
				t.Fatal(err)
			}
			var titles []string
			for _, pr := range prs {
				titles = append(titles, pr.Title)
			}
			t.Logf("%s: org %s titles %q", phase, c.org[:4], titles)
			if len(prs) != 1 || prs[0].Title != c.want {
				t.Errorf("%s: org %s rows %q, want one row %q", phase, c.org[:4], titles, c.want)
			}
		}
	}
	check("separate parts")
	if err := conn.Exec(ctx, `SYSTEM START MERGES git_pull_requests`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `OPTIMIZE TABLE git_pull_requests FINAL`); err != nil {
		t.Fatal(err)
	}
	check("after OPTIMIZE FINAL")
}
