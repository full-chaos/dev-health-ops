package syncadmin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

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
	if _, err := takeArtifactExclude("github", artifactExcludeOptions(list)); err != nil {
		t.Fatalf("github: %v, want accepted", err)
	}
	for _, provider := range []string{"gitlab", "jira", "linear", "launchdarkly", "pagerduty"} {
		_, err := takeArtifactExclude(provider, artifactExcludeOptions(list))
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
	if _, err := takeArtifactExclude("gitlab", pyjson.NewObject()); err != nil {
		t.Errorf("gitlab without the key: %v, want no refusal", err)
	}
	if _, err := takeArtifactExclude("github", options); err == nil {
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
		_, err := takeArtifactExclude("github", artifactExcludeOptions(cell.value))
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

func TestTakeArtifactExcludeTakesTheKeyOutOfTheOptions(t *testing.T) {
	for _, cell := range []struct {
		value pyjson.Value
		clear bool
	}{{nil, true}, {[]pyjson.Value{}, true}, {[]pyjson.Value{"ci-zz9-"}, false}} {
		options := artifactExcludeOptions(cell.value)
		change, err := takeArtifactExclude("github", options)
		if err != nil || change == nil || change.Clear != cell.clear {
			t.Fatalf("%#v: change=%+v err=%v", cell.value, change, err)
		}
		if _, present := options.Get("ci_artifact_exclude_prefixes"); present {
			t.Errorf("%#v: the key is still in the options, want it out: the list is the integration's", cell.value)
		}
		if _, present := options.Get("all_repos"); !present {
			t.Errorf("%#v: another key was dropped", cell.value)
		}
	}
}

// Every rendered configuration with an integration shows the integration's list in sync_options; with no list on the
// integration the response is byte-equal to the response without the derivation (today's answer), and a configuration
// without an integration is not derived.
func TestSyncConfigResponseShowsTheIntegrationListAndIsByteEqualWithoutOne(t *testing.T) {
	integrationWith, integrationWithout := uuid.New(), uuid.New()
	options := `{"all_repos": true, "schedule_cron": "0 * * * *"}`
	stale := `{"all_repos": true, "ci_artifact_exclude_prefixes": ["old-"]}`
	targets := `["tests"]`
	config := func(integration *uuid.UUID, optionsText string) *syncConfig {
		text, list := optionsText, targets
		return &syncConfig{ID: uuid.New(), Name: "c", Provider: "github", SyncTargets: &list, SyncOptions: &text, IntegrationID: integration}
	}
	render := func(c *syncConfig) string {
		body, err := syncConfigResponse(c, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		text, err := pyjson.MarshalModel(body)
		if err != nil {
			t.Fatal(err)
		}
		return string(text)
	}
	listed := `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-z-"]}`
	plainConfig := `{"fetch_comments": true}`
	read := func(_ context.Context, _ string, ids []uuid.UUID) (map[uuid.UUID]*string, error) {
		if len(ids) != 2 {
			t.Fatalf("one read for every integration, got ids %v", ids)
		}
		return map[uuid.UUID]*string{integrationWith: &listed, integrationWithout: &plainConfig}, nil
	}
	withA, withB := config(&integrationWith, options), config(&integrationWith, stale)
	without, noIntegration := config(&integrationWithout, options), config(nil, stale)
	before := map[*syncConfig]string{}
	for _, c := range []*syncConfig{withA, withB, without, noIntegration} {
		before[c] = render(c)
	}
	if err := deriveArtifactExclude(context.Background(), read, "org", withA, withB, without, noIntegration); err != nil {
		t.Fatal(err)
	}
	if got := render(without); got != before[without] {
		t.Errorf("an integration without the list: %s, want byte-equal to %s", got, before[without])
	}
	if got := render(noIntegration); got != before[noIntegration] {
		t.Errorf("a configuration without an integration: %s, want byte-equal to %s", got, before[noIntegration])
	}
	for _, c := range []*syncConfig{withA, withB} {
		if got := render(c); !strings.Contains(got, `"ci_artifact_exclude_prefixes":["ci-z-"]`) || strings.Contains(got, "old-") {
			t.Errorf("a sibling of the integration with the list: %s, want the integration's list and no copy of its own", got)
		}
	}
}
