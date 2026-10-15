package providersync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// A Jira Cloud v3 issue as the REST client decodes it: description is an
// Atlassian Document Format tree.
const jiraADFDescription = `{
 "type": "doc", "version": 1,
 "content": [
  {"type": "heading", "attrs": {"level": 2}, "content": [{"type": "text", "text": "Card tokenization"}]},
  {"type": "paragraph", "content": [
    {"type": "text", "text": "Customers asked for "},
    {"type": "text", "text": "saved cards", "marks": [{"type": "strong"}]},
    {"type": "hardBreak"},
    {"type": "text", "text": "ping "},
    {"type": "mention", "attrs": {"id": "5b10", "text": "@alice"}},
    {"type": "text", "text": " and see "},
    {"type": "inlineCard", "attrs": {"url": "https://example.atlassian.net/browse/PAY-9"}}
  ]},
  {"type": "bulletList", "content": [
    {"type": "listItem", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "store a token"}]}]},
    {"type": "listItem", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "never the number"}]}]}
  ]},
  {"type": "mediaSingle", "content": [{"type": "media", "attrs": {"id": "f1", "type": "file"}}]},
  {"type": "codeBlock", "content": [{"type": "text", "text": "tokenize(card)"}]}
 ]
}`

func jiraDescriptionOf(t *testing.T, description any) *string {
	t.Helper()
	input := map[string]any{}
	for key, value := range jiraWorkItemOraclePrepCases()[0].Input {
		input[key] = value
	}
	raw := input["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	fields["description"] = description
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["org_id"].(string)
	row, _, err := normalizeJiraWorkItem(
		claim, jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}},
		loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row.Description
}

func TestJiraDescriptionIsThePlainTextOfAnAtlassianDocument(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal([]byte(jiraADFDescription), &document); err != nil {
		t.Fatal(err)
	}
	want := "Card tokenization\n" +
		"Customers asked for saved cards\nping @alice and see https://example.atlassian.net/browse/PAY-9\n" +
		"store a token\nnever the number\n" +
		"tokenize(card)"
	got := jiraDescriptionOf(t, document)
	if got == nil || *got != want {
		t.Fatalf("description=%q want %q", valueOrNil(got), want)
	}
	if strings.Contains(*got, "map[") {
		t.Fatalf("description holds a Go-formatted map: %q", *got)
	}
}

func valueOrNil(text *string) string {
	if text == nil {
		return "<nil>"
	}
	return *text
}

func TestJiraDescriptionKeepsAStringAndStatesNothingForOtherShapes(t *testing.T) {
	if got := jiraDescriptionOf(t, "Customer-visible repair"); got == nil || *got != "Customer-visible repair" {
		t.Errorf("a v2 string description=%q", valueOrNil(got))
	}
	for name, value := range map[string]any{
		"null":              nil,
		"empty string":      "",
		"empty document":    map[string]any{"type": "doc", "version": 1, "content": []any{}},
		"media only":        map[string]any{"type": "doc", "content": []any{map[string]any{"type": "mediaSingle", "content": []any{map[string]any{"type": "media"}}}}},
		"a list":            []any{"x"},
		"an integer":        42,
		"a float":           42.5,
		"a json number":     json.Number("42"),
		"a number node":     map[string]any{"type": "doc", "content": []any{map[string]any{"type": "paragraph", "content": []any{map[string]any{"type": "text", "text": 42}}}}},
		"true":              true,
		"false":             false,
		"a map, no content": map[string]any{"unexpected": "shape"},
	} {
		if got := jiraDescriptionOf(t, value); got != nil {
			t.Errorf("%s: description=%q, want none", name, *got)
		}
	}
}

// A provider-sized or hostile tree costs a bounded amount of text and depth.
func TestJiraDescriptionFlatteningIsBounded(t *testing.T) {
	paragraphs := make([]any, 0, 5000)
	for range 5000 {
		paragraphs = append(paragraphs, map[string]any{"type": "paragraph", "content": []any{
			map[string]any{"type": "text", "text": strings.Repeat("😀", 40)},
		}})
	}
	got := jiraDescriptionOf(t, map[string]any{"type": "doc", "content": paragraphs})
	if got == nil || len(*got) < workItemDescriptionMaxRunes || len(*got) > 4*workItemDescriptionMaxRunes+len("😀")*40+2 {
		t.Fatalf("flattened %d bytes, want between the description bound and four bytes a rune plus one paragraph", len(valueOrNil(got)))
	}
	if cut := boundedWorkItemDescription(got); utf8.RuneCountInString(*cut) != workItemDescriptionMaxRunes {
		t.Fatalf("cut description holds %d runes", utf8.RuneCountInString(*cut))
	}

	deep := map[string]any{"type": "text", "text": "buried"}
	for range 10000 {
		deep = map[string]any{"type": "blockquote", "content": []any{deep}}
	}
	if got := jiraDescriptionOf(t, map[string]any{"type": "doc", "content": []any{deep}}); got != nil {
		t.Fatalf("a tree nested past the depth bound stated %q", *got)
	}
}

// A card node reads as its URL whether it is inline or a block of its own; a
// description that is only a smart link is still a description.
func TestJiraDescriptionReadsEveryCardNodeAsItsURL(t *testing.T) {
	for _, kind := range []string{"inlineCard", "blockCard", "embedCard"} {
		document := map[string]any{"type": "doc", "version": 1, "content": []any{
			map[string]any{"type": kind, "attrs": map[string]any{"url": "https://example.atlassian.net/browse/PAY-9"}},
		}}
		if got := jiraDescriptionOf(t, document); got == nil || *got != "https://example.atlassian.net/browse/PAY-9" {
			t.Errorf("%s: description=%q", kind, valueOrNil(got))
		}
	}
	data := map[string]any{"type": "doc", "content": []any{
		map[string]any{"type": "blockCard", "attrs": map[string]any{"data": map[string]any{"url": "https://example.com/d"}}},
	}}
	if got := jiraDescriptionOf(t, data); got == nil || *got != "https://example.com/d" {
		t.Errorf("blockCard with data.url: description=%q", valueOrNil(got))
	}
}

// One node larger than the whole bound costs no more than the bound, on a rune
// boundary, for every node kind that writes text.
func TestJiraDescriptionFlatteningBoundsASingleLargeNode(t *testing.T) {
	huge := strings.Repeat("😀", 262144)
	for name, node := range map[string]map[string]any{
		"text":       {"type": "text", "text": huge},
		"mention":    {"type": "mention", "attrs": map[string]any{"text": huge}},
		"inlineCard": {"type": "inlineCard", "attrs": map[string]any{"url": huge}},
	} {
		got := jiraDescriptionOf(t, map[string]any{"type": "doc", "content": []any{
			map[string]any{"type": "paragraph", "content": []any{node}},
		}})
		if got == nil || len(*got) > 4*workItemDescriptionMaxRunes || !utf8.ValidString(*got) ||
			utf8.RuneCountInString(*got) < workItemDescriptionMaxRunes {
			t.Errorf("%s: flattened %d bytes (valid utf8 %v), want between %d runes and %d bytes on a rune boundary",
				name, len(valueOrNil(got)), got != nil && utf8.ValidString(*got), workItemDescriptionMaxRunes, 4*workItemDescriptionMaxRunes)
		}
	}
}
