package integrationsadmin

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestStoredDictIsDictOfValueOrEmpty(t *testing.T) {
	for _, tc := range []struct {
		text    string
		want    string
		wantErr bool
	}{
		{`{"a": 1, "b": [1.5]}`, `{"a":1,"b":[1.5]}`, false},
		{`{}`, `{}`, false},
		{`null`, `{}`, false},
		{`[]`, `{}`, false},
		{`""`, `{}`, false},
		{`0`, `{}`, false},
		{`false`, `{}`, false},
		{`[["k", "v"], ["n", 1]]`, `{"k":"v","n":1}`, false},
		{`["ab"]`, `{"a":"b"}`, false},
		{`[[1, "v"]]`, ``, true},
		{`[[["a"], 1]]`, ``, true},
		{`[["only-one"]]`, ``, true},
		{`[1]`, ``, true},
		{`"x"`, ``, true},
		{`5`, ``, true},
	} {
		got, err := storedDict(tc.text)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: error %v, want error %v", tc.text, err, tc.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		text, encodeErr := pyjson.MarshalModel(got)
		if encodeErr != nil || string(text) != tc.want {
			t.Errorf("%s: %s, %v; want %s", tc.text, text, encodeErr, tc.want)
		}
	}
}

func TestJiraKeyStripsAndLowers(t *testing.T) {
	for in, want := range map[string]string{"jira": "jira", "JIRA": "jira", " Jira \t": "jira", "github": "github", "": ""} {
		if got := jiraKey(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

func TestHasMarkerReadsTruthiness(t *testing.T) {
	for text, want := range map[string]bool{
		`{}`: false, `{"capped_by_repo_limit": true}`: true, `{"superseded_by_scope_change": 1}`: true,
		`{"capped_by_repo_limit": false, "superseded_by_scope_change": ""}`: false, `{"other": true}`: false,
		`{"capped_by_repo_limit": null}`: false, `{"capped_by_repo_limit": "x"}`: true,
	} {
		object, err := storedDict(text)
		if err != nil {
			t.Fatal(err)
		}
		if got := hasMarker(object); got != want {
			t.Errorf("%s: %v, want %v", text, got, want)
		}
	}
}

func TestLimitIntTruncatesLikeInt(t *testing.T) {
	if got, err := limitInt(pyjson.IntOf(3)); err != nil || got != 3 {
		t.Errorf("int: %d, %v", got, err)
	}
	if got, err := limitInt(pyjson.Float(2.9)); err != nil || got != 2 {
		t.Errorf("float: %d, %v", got, err)
	}
	if _, err := limitInt("x"); err == nil {
		t.Error("a string limit is refused")
	}
}

func TestJiraKeyMatchesPythonStripAndLower(t *testing.T) {
	// U+0130 lower-cases to "i" + U+0307 in Python, and str.strip() removes
	// U+001C to U+001F.
	if jiraKey("J\u0130RA") == "jira" {
		t.Error("a capital I with dot is not a plain i")
	}
	if got := jiraKey("\x1fJira\x1c"); got != "jira" {
		t.Errorf("separators are stripped: %q", got)
	}
	if got := jiraKey("\u00a0jira\u2003"); got != "jira" {
		t.Errorf("unicode spaces are stripped: %q", got)
	}
}

func TestMarkerMetadataIsGetOfValueOrEmpty(t *testing.T) {
	for text, wantErr := range map[string]bool{`{"a":1}`: false, `{}`: false, `null`: false, `[]`: false, `[["a", 1]]`: true, `"x"`: true, `5`: true} {
		if _, err := markerMetadata(text); (err != nil) != wantErr {
			t.Errorf("%s: error %v, want %v", text, err, wantErr)
		}
	}
}
