//go:build integration

package drilldown

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// CHAOS-13085 C1-C3, C7-C9 (acp): the REAL link subquery on a real ClickHouse. Allowed repository k1, hidden k2.
// Items: one linked to k1 per provenance tier, one linked to k2 only, one linked to both, one with no link row, and one
// of another organization with the same id linked to k1.
func TestIssueDrilldownIsNarrowedToItemsLinkedToAnAllowedRepository(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	const org, otherOrg = "org-link-13085", "org-link-13085-other"
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	k1, k2 := uuid.New(), uuid.New()
	for name, id := range map[string]uuid.UUID{"acme/k1": k1, "acme/k2": k2} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, name, org, at, at); err != nil {
			t.Fatal(err)
		}
	}
	item := func(orgID, id string) {
		t.Helper()
		if err := admin.Exec(ctx, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, 'github', ?, 'KEY', 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`, id, at, at, at, at, at.Add(time.Hour), orgID); err != nil {
			t.Fatal(err)
		}
	}
	link := func(orgID string, repo uuid.UUID, id, tier string, pr uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `
INSERT INTO work_graph_issue_pr (org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced)
VALUES (?, ?, ?, ?, 1.0, ?, 'test', now64(3))`, orgID, repo, id, pr, tier); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"gh:native", "gh:text", "gh:heuristic", "gh:hidden", "gh:both", "gh:unlinked"} {
		item(org, id)
	}
	link(org, k1, "gh:native", "native", 1)
	link(org, k1, "gh:text", "explicit_text", 2)
	link(org, k1, "gh:heuristic", "heuristic", 3)
	link(org, k2, "gh:hidden", "native", 4)
	link(org, k1, "gh:both", "native", 5)
	link(org, k2, "gh:both", "native", 6)
	item(otherOrg, "gh:other-org")
	link(otherOrg, k1, "gh:other-org", "native", 7)

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	read := func(allowed []string, blocked bool) string {
		t.Helper()
		ceiling, err := reader.ResolveCeiling(ctx, pepgate.Allowlist(org, allowed))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := BuildIssuesResponse(ctx, reader, org, IssueParams{
			StartDay: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), EndDay: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
			Limit: 50, Ceiling: ceiling, BlockedOnly: blocked,
		})
		if err != nil {
			t.Fatalf("real ClickHouse refused the link statement: %v", err)
		}
		var got []string
		for _, it := range resp.Items {
			got = append(got, it.WorkItemID)
		}
		sort.Strings(got)
		return strings.Join(got, ", ")
	}
	for _, tc := range []struct {
		name    string
		allowed []string
		want    string
	}{
		{"k1: every tier, the shared item whole; not the hidden, the unlinked or the other organization's", []string{"acme/k1"}, "gh:both, gh:heuristic, gh:native, gh:text"},
		{"k2: the hidden and the shared item only", []string{"acme/k2"}, "gh:both, gh:hidden"},
		{"a key that names nothing", []string{"acme/none"}, ""},
		{"an empty allowlist", nil, ""},
	} {
		if got := read(tc.allowed, false); got != tc.want {
			t.Errorf("%s: items = [%s], want [%s]", tc.name, got, tc.want)
		}
	}
}
