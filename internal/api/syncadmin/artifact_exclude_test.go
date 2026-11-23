package syncadmin

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func artifactExcludeOptions(value pyjson.Value) *pyjson.Object {
	options := pyjson.NewObject()
	options.Set("all_repos", true)
	options.Set("ci_artifact_exclude_prefixes", value)
	return options
}

// A create (single and batch) of a configuration whose options carry ci_artifact_exclude_prefixes: github takes a valid
// list as given, every other provider is a 422 whose detail names the field and never the value.
func TestCreateArtifactExcludeRefusesEveryNonGitHubProvider(t *testing.T) {
	list := []pyjson.Value{"ci-zz9-"}
	if err := createArtifactExclude("github", artifactExcludeOptions(list)); err != nil {
		t.Fatalf("github: %v, want accepted", err)
	}
	for _, provider := range []string{"gitlab", "jira", "linear", "launchdarkly", "pagerduty"} {
		err := createArtifactExclude(provider, artifactExcludeOptions(list))
		var raised *answer
		if !errors.As(err, &raised) || raised.status != http.StatusUnprocessableEntity {
			t.Errorf("%s: err=%v, want a 422", provider, err)
			continue
		}
		text, _ := pyjson.Dumps(raised.detail)
		if strings.Contains(text, "zz9") || !strings.Contains(text, `"field": "ci_artifact_exclude_prefixes"`) {
			t.Errorf("%s: detail %s, want the field named and the value absent", provider, text)
		}
	}
	options := artifactExcludeOptions(pyjson.NewObject())
	if err := createArtifactExclude("gitlab", pyjson.NewObject()); err != nil {
		t.Errorf("gitlab without the key: %v, want no refusal", err)
	}
	if err := createArtifactExclude("github", options); err == nil {
		t.Error("github with an object value: accepted, want a 422")
	}
}

func TestCreateArtifactExcludeBoundsAnswerWithTheEntryIndex(t *testing.T) {
	for _, cell := range []struct {
		name  string
		value pyjson.Value
		index string
	}{
		{name: "empty entry", value: []pyjson.Value{"ok", ""}, index: `"index": 1`},
		{name: "charset", value: []pyjson.Value{"ci zz9"}, index: `"index": 0`},
		{name: "duplicate", value: []pyjson.Value{"ci-zz9-", "CI-ZZ9-"}, index: `"index": 1`},
		{name: "too long", value: []pyjson.Value{strings.Repeat("z", 101)}, index: `"index": 0`},
	} {
		err := createArtifactExclude("github", artifactExcludeOptions(cell.value))
		var raised *answer
		if !errors.As(err, &raised) || raised.status != http.StatusUnprocessableEntity {
			t.Errorf("%s: err=%v, want a 422", cell.name, err)
			continue
		}
		text, _ := pyjson.Dumps(raised.detail)
		if !strings.Contains(text, cell.index) || strings.Contains(text, "zz9") || strings.Contains(text, "zzzz") {
			t.Errorf("%s: detail %s, want %s and no value", cell.name, text, cell.index)
		}
	}
}

func TestCreateArtifactExcludeClearDropsTheKey(t *testing.T) {
	for _, value := range []pyjson.Value{nil, []pyjson.Value{}} {
		options := artifactExcludeOptions(value)
		if err := createArtifactExclude("github", options); err != nil {
			t.Fatalf("%#v: %v", value, err)
		}
		if _, present := options.Get("ci_artifact_exclude_prefixes"); present {
			t.Errorf("%#v: the key is still in the options, want it dropped", value)
		}
		if _, present := options.Get("all_repos"); !present {
			t.Errorf("%#v: another key was dropped", value)
		}
	}
}
