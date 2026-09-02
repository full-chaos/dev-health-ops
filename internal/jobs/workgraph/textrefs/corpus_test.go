package textrefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type corpusCase struct {
	ID         string                     `json:"id"`
	Axis       string                     `json:"axis"`
	Text       string                     `json:"text"`
	Extractors map[string]json.RawMessage `json:"extractors"`
}

type corpusDocument struct {
	Schema string       `json:"schema"`
	Cases  []corpusCase `json:"cases"`
}

func loadCorpus(t *testing.T) corpusDocument {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "text_reference_parity.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var doc corpusDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if doc.Schema != "text_reference_parity.v1" {
		t.Fatalf("corpus schema is %q, want text_reference_parity.v1", doc.Schema)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("corpus has no cases; every assertion below would be vacuous")
	}
	return doc
}

// TestExtractorsMatchFrozenPython drives every extractor over every corpus case
// and compares against what the deployed Python produced.
//
// Per-extractor tallies rather than a single pass/fail: the point of the run is
// to say WHICH extractor and WHICH axis diverges, since a port lands one
// extractor at a time and a single boolean would hide progress and regressions
// alike.
func TestExtractorsMatchFrozenPython(t *testing.T) {
	doc := loadCorpus(t)

	type result struct{ pass, fail int }
	tally := map[string]*result{}
	firstFailure := map[string]string{}

	record := func(name, id string, got, want any) {
		if tally[name] == nil {
			tally[name] = &result{}
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) == string(wantJSON) {
			tally[name].pass++
			return
		}
		tally[name].fail++
		if _, seen := firstFailure[name]; !seen {
			firstFailure[name] = id + "\n      got:  " + string(gotJSON) + "\n      want: " + string(wantJSON)
		}
	}

	// Python emits [] as null when empty in some encoders; normalise both sides
	// by round-tripping through the same shape.
	norm := func(raw json.RawMessage, into any) any {
		if len(raw) == 0 || string(raw) == "null" {
			return into
		}
		_ = json.Unmarshal(raw, into)
		return into
	}

	for _, c := range doc.Cases {
		if raw, ok := c.Extractors["extract_pr_refs"]; ok {
			want := norm(raw, &[]int{}).(*[]int)
			got := ExtractPRRefs(c.Text)
			if got == nil {
				got = []int{}
			}
			record("extract_pr_refs", c.ID, got, *want)
		}
		if raw, ok := c.Extractors["extract_squash_pr_refs"]; ok {
			want := norm(raw, &[]int{}).(*[]int)
			got := ExtractSquashPRRefs(c.Text)
			if got == nil {
				got = []int{}
			}
			record("extract_squash_pr_refs", c.ID, got, *want)
		}
		if raw, ok := c.Extractors["extract_jira_keys"]; ok {
			want := norm(raw, &[]pythonRef{}).(*[]pythonRef)
			record("extract_jira_keys", c.ID, toPythonRefs(ExtractJiraKeys(c.Text)), *want)
		}
		if raw, ok := c.Extractors["extract_github_issue_refs"]; ok {
			want := norm(raw, &[]pythonRef{}).(*[]pythonRef)
			record("extract_github_issue_refs", c.ID, toPythonRefs(ExtractGitHubIssueRefs(c.Text)), *want)
		}
		if raw, ok := c.Extractors["extract_gitlab_issue_refs"]; ok {
			want := norm(raw, &[]pythonRef{}).(*[]pythonRef)
			record("extract_gitlab_issue_refs", c.ID, toPythonRefs(ExtractGitLabIssueRefs(c.Text)), *want)
		}
	}

	for _, name := range []string{
		"extract_pr_refs", "extract_squash_pr_refs",
		"extract_jira_keys", "extract_github_issue_refs", "extract_gitlab_issue_refs",
	} {
		r := tally[name]
		if r == nil {
			t.Errorf("%s: no cases exercised it", name)
			continue
		}
		if r.fail == 0 {
			t.Logf("%-26s %d/%d PASS", name, r.pass, r.pass+r.fail)
			continue
		}
		t.Errorf("%-26s %d/%d pass, %d FAIL\n    first: %s",
			name, r.pass, r.pass+r.fail, r.fail, firstFailure[name])
	}
}

// pythonRef is the JSON shape the generator emits for a ParsedIssueRef.
type pythonRef struct {
	RawMatch   string  `json:"raw_match"`
	IssueKey   string  `json:"issue_key"`
	RefType    string  `json:"ref_type"`
	ProjectKey *string `json:"project_key"`
}

func toPythonRefs(refs []ParsedIssueRef) []pythonRef {
	out := []pythonRef{}
	for _, r := range refs {
		var pk *string
		if r.ProjectKey != "" {
			v := r.ProjectKey
			pk = &v
		}
		out = append(out, pythonRef{
			RawMatch: r.RawMatch, IssueKey: r.IssueKey,
			RefType: string(r.RefType), ProjectKey: pk,
		})
	}
	return out
}
