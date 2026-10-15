package providersync

import (
	"testing"
	"time"
)

// The provider matrix of relation_started_at (AGENTS.md: provider-agnostic coverage). A provider that
// reports the time of a link gives that time; a link whose provider reports none keeps a NULL start and
// no time is invented. jira: the changelog "Link" entry; gitlab: link_created_at; linear: the relation's
// createdAt; github: its relations are read from text (body, branch, comments), which carries no link
// time, so the start is NULL.

func matrixTime(t *testing.T, text string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, text)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func jiraBlocksIssue(histories ...any) map[string]any {
	issue := map[string]any{
		"fields": map[string]any{
			"issuelinks": []any{map[string]any{
				"type":         map[string]any{"name": "Blocks", "outward": "blocks", "inward": "is blocked by"},
				"outwardIssue": map[string]any{"key": "OPS-2"},
			}},
		},
	}
	if histories != nil {
		issue["changelog"] = map[string]any{"histories": histories}
	}
	return issue
}

func jiraLinkHistory(created string, from, to string) map[string]any {
	item := map[string]any{"field": "Link", "fieldtype": "jira"}
	if to != "" {
		item["to"], item["toString"] = to, "This issue blocks "+to
	}
	if from != "" {
		item["from"], item["fromString"] = from, "This issue blocks "+from
	}
	return map[string]any{"id": created, "created": created, "items": []any{item}}
}

func TestRelationStartedAtJira(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	synced := matrixTime(t, "2026-10-12T12:00:00Z")
	cases := []struct {
		name  string
		issue map[string]any
		want  string // "" = NULL
	}{
		{"dated link", jiraBlocksIssue(jiraLinkHistory("2026-09-27T09:40:00.000+0000", "", "OPS-2")), "2026-09-27T09:40:00Z"},
		{"no changelog", jiraBlocksIssue(), ""},
		{"changelog without the link", jiraBlocksIssue(jiraLinkHistory("2026-09-27T09:40:00.000+0000", "", "OPS-9")), ""},
		{"removed link", jiraBlocksIssue(
			jiraLinkHistory("2026-09-20T09:00:00.000+0000", "", "OPS-2"), jiraLinkHistory("2026-09-21T09:00:00.000+0000", "OPS-2", "")), ""},
		{"removed and added again starts at the last addition", jiraBlocksIssue(
			jiraLinkHistory("2026-09-20T09:00:00.000+0000", "", "OPS-2"), jiraLinkHistory("2026-09-21T09:00:00.000+0000", "OPS-2", ""),
			jiraLinkHistory("2026-09-25T09:00:00.000+0000", "", "OPS-2")), "2026-09-25T09:00:00Z"},
		{"a time after the sync clock is not a start", jiraBlocksIssue(jiraLinkHistory("2026-10-13T09:00:00.000+0000", "", "OPS-2")), ""},
	}
	for _, c := range cases {
		rows := normalizeJiraDependencies(claim, "jira:OPS-1", c.issue, synced)
		if len(rows) != 1 {
			t.Fatalf("%s: %d rows", c.name, len(rows))
		}
		got := rows[0].RelationStartedAt
		switch {
		case c.want == "" && got != nil:
			t.Errorf("%s: start %v, want NULL", c.name, got)
		case c.want != "" && (got == nil || !got.Equal(matrixTime(t, c.want))):
			t.Errorf("%s: start %v, want %s", c.name, got, c.want)
		}
	}
}

// Two links of different types to the same issue each keep their own time, and removing one leaves the
// other's: the changelog entry names the link type in its description ("This issue blocks OPS-2").
func TestRelationStartedAtJiraLinksOfTwoTypesToOneIssue(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	synced := matrixTime(t, "2026-10-12T12:00:00Z")
	typed := func(created, from, to, phrase string) map[string]any {
		item := map[string]any{"field": "Link", "fieldtype": "jira"}
		if to != "" {
			item["to"], item["toString"] = to, "This issue "+phrase+" "+to
		}
		if from != "" {
			item["from"], item["fromString"] = from, "This issue "+phrase+" "+from
		}
		return map[string]any{"id": created + phrase, "created": created, "items": []any{item}}
	}
	issue := func(histories ...any) map[string]any {
		return map[string]any{
			"fields": map[string]any{"issuelinks": []any{
				map[string]any{"type": map[string]any{"name": "Blocks", "outward": "blocks", "inward": "is blocked by"}, "outwardIssue": map[string]any{"key": "OPS-2"}},
				map[string]any{"type": map[string]any{"name": "Relates", "outward": "relates to", "inward": "relates to"}, "outwardIssue": map[string]any{"key": "OPS-2"}},
			}},
			"changelog": map[string]any{"histories": histories},
		}
	}
	startOf := func(rows []jiraWorkItemDependencyRow, relationship string) *time.Time {
		for _, row := range rows {
			if row.RelationshipType == relationship {
				return row.RelationStartedAt
			}
		}
		t.Fatalf("no %s row in %+v", relationship, rows)
		return nil
	}
	both := issue(
		typed("2026-09-01T09:00:00.000+0000", "", "OPS-2", "blocks"),
		typed("2026-09-20T09:00:00.000+0000", "", "OPS-2", "relates to"))
	rows := normalizeJiraDependencies(claim, "jira:OPS-1", both, synced)
	if got := startOf(rows, "blocks"); got == nil || !got.Equal(matrixTime(t, "2026-09-01T09:00:00Z")) {
		t.Errorf("blocks link start %v, want 2026-09-01T09:00:00Z (the relates-to link's later time was used)", got)
	}
	if got := startOf(rows, "relates_to"); got == nil || !got.Equal(matrixTime(t, "2026-09-20T09:00:00Z")) {
		t.Errorf("relates-to link start %v, want 2026-09-20T09:00:00Z", got)
	}
	removed := issue(
		typed("2026-09-01T09:00:00.000+0000", "", "OPS-2", "blocks"),
		typed("2026-09-20T09:00:00.000+0000", "", "OPS-2", "relates to"),
		typed("2026-09-22T09:00:00.000+0000", "OPS-2", "", "relates to"))
	rows = normalizeJiraDependencies(claim, "jira:OPS-1", removed, synced)
	if got := startOf(rows, "blocks"); got == nil || !got.Equal(matrixTime(t, "2026-09-01T09:00:00Z")) {
		t.Errorf("after the relates-to link was removed the blocks start is %v, want 2026-09-01T09:00:00Z", got)
	}
}

func TestRelationStartedAtGitLab(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	synced := matrixTime(t, "2026-10-12T12:00:00Z")
	dated := "2026-09-27T09:40:00Z"
	link := func(createdAt *string) gitlabIssueLinkPayload {
		payload := gitlabIssueLinkPayload{LinkType: "blocks", LinkCreatedAt: createdAt}
		payload.References.Full = "grp/proj#2"
		payload.IID = "2"
		return payload
	}
	rows := normalizeGitLabDependencies(claim, "gitlab:grp/proj#1", "grp/proj", "", []gitlabIssueLinkPayload{link(&dated)}, synced)
	if len(rows) != 1 || rows[0].RelationStartedAt == nil || !rows[0].RelationStartedAt.Equal(matrixTime(t, dated)) {
		t.Fatalf("dated link: %+v", rows)
	}
	rows = normalizeGitLabDependencies(claim, "gitlab:grp/proj#1", "grp/proj", "", []gitlabIssueLinkPayload{link(nil)}, synced)
	if len(rows) != 1 || rows[0].RelationStartedAt != nil {
		t.Fatalf("link without a time: %+v", rows)
	}
}

func TestRelationStartedAtLinear(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	synced := matrixTime(t, "2026-10-12T12:00:00Z")
	dated := "2026-09-27T09:40:00Z"
	payloadOf := func(createdAt *string) linearWorkItemPayload {
		var payload linearWorkItemPayload
		payload.Relations.Nodes = []linearRelationPayload{{
			Type: "blocks", CreatedAt: createdAt,
			Issue: &linearRelationIssuePayload{Identifier: "ENG-1"}, RelatedIssue: &linearRelationIssuePayload{Identifier: "ENG-2"},
		}}
		return payload
	}
	rows := normalizeLinearDependencies(claim, payloadOf(&dated), "linear:ENG-1", synced)
	if len(rows) != 1 || rows[0].RelationStartedAt == nil || !rows[0].RelationStartedAt.Equal(matrixTime(t, dated)) {
		t.Fatalf("dated relation: %+v", rows)
	}
	rows = normalizeLinearDependencies(claim, payloadOf(nil), "linear:ENG-1", synced)
	if len(rows) != 1 || rows[0].RelationStartedAt != nil {
		t.Fatalf("relation without a time: %+v", rows)
	}
}

func TestRelationStartedAtGitHubIsNullBecauseItsRelationsAreText(t *testing.T) {
	claim := Claim{Unit: Unit{OrgID: "org-1"}}
	synced := matrixTime(t, "2026-10-12T12:00:00Z")
	rows, err := extractGitHubWorkItemDependencies(claim, "ghpr:o/r#1", "o/r", "This is blocked by #2", "", nil, synced)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("no relation read from the text")
	}
	for _, row := range rows {
		if row.RelationStartedAt != nil {
			t.Errorf("a text relation got a start time %v", row.RelationStartedAt)
		}
	}
}
