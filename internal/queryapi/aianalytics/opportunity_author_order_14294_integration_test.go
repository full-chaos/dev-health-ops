//go:build integration

package aianalytics

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-14294: two repetitive clusters tie on count, repository and title prefix. Swapping only the author emails
// must not change the served order: the author is no input to the order.
func TestRealClickHouse_RepetitiveTieOrderIgnoresAuthorEmail(t *testing.T) {
	ctx, conn, client := startStore(t)
	const repo = "00000000-0000-0000-0000-000000142940"
	worlds := []struct{ org, emailLow, emailHigh string }{
		{"00000000-0000-0000-0000-000000142941", "aaa@example.invalid", "zzz@example.invalid"},
		{"00000000-0000-0000-0000-000000142942", "zzz@example.invalid", "aaa@example.invalid"},
	}
	var orders []string
	for _, w := range worlds {
		var prs, attrs []string
		for c, email := range []string{w.emailLow, w.emailHigh} {
			for k := 1; k <= 5; k++ {
				n := 100*c + k
				prs = append(prs, fmt.Sprintf("('%s', %d, 'Rename widget helper', 'name', '%s', now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, email, w.org))
				attrs = append(attrs, fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", w.org, n, repo))
			}
		}
		cutInsert(t, ctx, conn, "git_pull_requests (repo_id, number, title, author_name, author_email, created_at, org_id, last_synced)", prs)
		cutInsert(t, ctx, conn, "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)", attrs)
		got, err := AiOpportunities(ctx, client, w.org, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		var order []string
		for _, r := range got.Recommendations {
			first := false
			for _, ref := range r.EvidenceRefs {
				first = first || strings.HasSuffix(ref, ":"+repo+":1")
			}
			if first {
				order = append(order, "anchor-1")
			} else {
				order = append(order, "anchor-101")
			}
		}
		orders = append(orders, strings.Join(order, ","))
	}
	if len(strings.Split(orders[0], ",")) != 2 {
		t.Fatalf("want two clusters served, got %q", orders[0])
	}
	if orders[0] != orders[1] {
		t.Errorf("the served order follows the author emails: %q with one assignment, %q with the swapped one", orders[0], orders[1])
	}
}

// Two clusters whose anchors are 9 and 10 serve 9 first (number, not text), in the read and in the cut at N = 1.
func TestRealClickHouse_RepetitiveTieOrderIsNumericAnchor(t *testing.T) {
	ctx, conn, client := startStore(t)
	const repo, org = "00000000-0000-0000-0000-000000142940", "00000000-0000-0000-0000-000000142943"
	var prs, attrs []string
	for c, numbers := range [][]int{{10, 11, 12, 13, 14}, {9, 21, 22, 23, 24}} {
		for _, n := range numbers {
			prs = append(prs, fmt.Sprintf("('%s', %d, 'Rename widget helper', 'name', 'e%d@example.invalid', now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, c, org))
			attrs = append(attrs, fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", org, n, repo))
		}
	}
	cutInsert(t, ctx, conn, "git_pull_requests (repo_id, number, title, author_name, author_email, created_at, org_id, last_synced)", prs)
	cutInsert(t, ctx, conn, "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)", attrs)
	for _, limit := range []int{1, 100} {
		got, err := AiOpportunities(ctx, client, org, nil, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Recommendations) == 0 {
			t.Fatalf("limit %d: nothing served", limit)
		}
		has9 := false
		for _, ref := range got.Recommendations[0].EvidenceRefs {
			has9 = has9 || strings.HasSuffix(ref, ":"+repo+":9")
		}
		if !has9 {
			t.Errorf("limit %d: the first served cluster is not the one anchored at 9: %v", limit, got.Recommendations[0].EvidenceRefs)
		}
	}
}
