package providersync

import (
	"testing"
	"time"
)

// TestJiraRelationshipCanonicalizesRelatesTo is the red->green proof for
// CHAOS-5316. CF's corpus lane found 7 rows quarantined by the graph
// projector: Jira issue links whose Jira-reported link-type text is
// "relates to" (the REAL raw value observed in production, last_synced
// 2026-08-31 15:06-15:07Z) were normalized to relationship_type "relates" --
// one underscore short of the canonical "relates_to" every OTHER provider
// (GitHub, GitLab, Linear) emits for the same semantic relationship (see
// github_work_items_rows.go:938, gitlab_work_items_rows.go:715/742/764).
// The projector upper-cases relationship_type for Cypher and has no
// "RELATES" mapping (only "RELATES_TO"), so these rows were silently
// dropped from the graph. Before this fix, jiraRelationship("relates to")
// returned "relates" -- this test would have failed against that tree.
func TestJiraRelationshipCanonicalizesRelatesTo(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		// The exact raw string CF found in production.
		{name: "exact production raw value", raw: "relates to", want: "relates_to"},
		{name: "case-insensitive", raw: "Relates To", want: "relates_to"},
		{name: "outward phrasing variant", raw: "is related to", want: "relates_to"},
		// Un-related link types must not regress.
		{name: "blocks", raw: "blocks", want: "blocks"},
		{name: "is blocked by", raw: "is blocked by", want: "blocked_by"},
		{name: "duplicates", raw: "duplicates", want: "duplicates"},
		{name: "unrecognised", raw: "clones", want: "other"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := jiraRelationship(testCase.raw)
			if got != testCase.want {
				t.Fatalf("jiraRelationship(%q) = %q, want %q", testCase.raw, got, testCase.want)
			}
		})
	}
}

// TestNormalizeJiraDependenciesRelatesTo exercises the full issuelinks ->
// dependency-row path (not just the pure jiraRelationship mapper) with the
// exact production shape: a Jira issue link whose type carries "relates to"
// as both the outward and inward phrasing (Jira's own convention for its
// symmetric "Relates" link type -- both directions read the same).
func TestNormalizeJiraDependenciesRelatesTo(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	normalizedAt, err := time.Parse(time.RFC3339, "2026-08-31T15:06:00Z")
	if err != nil {
		t.Fatal(err)
	}
	issue := map[string]any{
		"fields": map[string]any{
			"issuelinks": []any{
				map[string]any{
					"type":         map[string]any{"outward": "relates to", "inward": "relates to"},
					"outwardIssue": map[string]any{"key": "OPS-200"},
				},
			},
		},
	}

	rows := normalizeJiraDependencies(claim, "jira:OPS-100", issue, normalizedAt)
	if len(rows) != 1 {
		t.Fatalf("got %d dependency rows, want 1: %+v", len(rows), rows)
	}
	row := rows[0]
	if row.RelationshipType != "relates_to" {
		t.Fatalf("RelationshipType = %q, want %q (raw=%q)", row.RelationshipType, "relates_to", row.RelationshipTypeRaw)
	}
	if row.RelationshipTypeRaw != "relates to" {
		t.Fatalf("RelationshipTypeRaw = %q, want %q", row.RelationshipTypeRaw, "relates to")
	}
	if row.SourceWorkItemID != "jira:OPS-100" || row.TargetWorkItemID != "jira:OPS-200" {
		t.Fatalf("unexpected edge direction: %+v", row)
	}
}
