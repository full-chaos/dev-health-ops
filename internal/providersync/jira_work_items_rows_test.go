package providersync

import (
	"testing"
	"time"
)

// TestJiraRelationshipCanonicalizesRelatesTo is the red->green proof for
// CHAOS-5316. CF's corpus lane found 7 rows quarantined: Jira issue links
// whose Jira-reported link-type text is "relates to" (the REAL raw value
// observed in production, last_synced 2026-08-31 15:06-15:07Z) were
// normalized to relationship_type "relates" -- one underscore short of the
// canonical "relates_to" every OTHER provider (GitHub, GitLab, Linear) emits
// for the same semantic relationship (see github_work_items_rows.go:938,
// gitlab_work_items_rows.go:715/742/764). Codex round chaos-5316-2322-r1
// verified this is NOT a silent-drop bug: both internal/jobs/workgraph/edges'
// dependencyTypeMap (canonical.go:50, with an unrecognised-type fallback to
// EdgeTypeRelates at :107) and cmd/query-api/internal/workgraph.go's
// dependencyRelationshipTypeMap (:139-146, which has explicit entries for
// BOTH "relates" and "relates_to") already handle Jira's pre-fix "relates"
// value without dropping it. The fix's real value is CONSISTENCY: Jira's raw
// value now canonicalizes to the same "relates_to" string every other
// provider writes, instead of a provider-specific "relates" spelling that
// happened to still resolve correctly downstream. Before this fix,
// jiraRelationship("relates to") returned "relates" -- this test would have
// failed to assert the canonical value against that tree.
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
// symmetric "Relates" link type -- both directions read the same). Covers
// BOTH the outwardIssue and inwardIssue branches of normalizeJiraDependencies
// (jira_work_items_rows.go:459/469) in one issuelinks entry each, since
// production processes them independently and a fix proven only against
// outwardIssue leaves the inward branch unverified.
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
				map[string]any{
					"type":        map[string]any{"outward": "relates to", "inward": "relates to"},
					"inwardIssue": map[string]any{"key": "OPS-300"},
				},
			},
		},
	}

	rows := normalizeJiraDependencies(claim, "jira:OPS-100", issue, normalizedAt)
	if len(rows) != 2 {
		t.Fatalf("got %d dependency rows, want 2: %+v", len(rows), rows)
	}

	outward := rows[0]
	if outward.RelationshipType != "relates_to" {
		t.Fatalf("outward RelationshipType = %q, want %q (raw=%q)", outward.RelationshipType, "relates_to", outward.RelationshipTypeRaw)
	}
	if outward.RelationshipTypeRaw != "relates to" {
		t.Fatalf("outward RelationshipTypeRaw = %q, want %q", outward.RelationshipTypeRaw, "relates to")
	}
	if outward.SourceWorkItemID != "jira:OPS-100" || outward.TargetWorkItemID != "jira:OPS-200" {
		t.Fatalf("unexpected outward edge direction: %+v", outward)
	}

	inward := rows[1]
	if inward.RelationshipType != "relates_to" {
		t.Fatalf("inward RelationshipType = %q, want %q (raw=%q)", inward.RelationshipType, "relates_to", inward.RelationshipTypeRaw)
	}
	if inward.RelationshipTypeRaw != "relates to" {
		t.Fatalf("inward RelationshipTypeRaw = %q, want %q", inward.RelationshipTypeRaw, "relates to")
	}
	// jiraRelationship("relates to") is symmetric (not "blocked_by"/"blocks"),
	// so the inward branch does NOT swap source/target: source is the linking
	// issue (OPS-300), target is the issue under sync (OPS-100) --
	// jira_work_items_rows.go:469-471.
	if inward.SourceWorkItemID != "jira:OPS-300" || inward.TargetWorkItemID != "jira:OPS-100" {
		t.Fatalf("unexpected inward edge direction: %+v", inward)
	}
}
