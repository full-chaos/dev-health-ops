package backfillrun

import (
	"context"
	"errors"
	"flag"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const (
	sourceA = "aaaaaaaa-0000-4000-8000-000000000001"
	sourceB = "aaaaaaaa-0000-4000-8000-000000000002"
	sourceC = "aaaaaaaa-0000-4000-8000-000000000003"
	sourceX = "aaaaaaaa-0000-4000-8000-0000000000ff"
	integr  = "11111111-0000-4000-8000-000000000001"
	cfgID   = "cccccccc-0000-4000-8000-000000000001"
)

func parseList(t *testing.T, args ...string) ([]string, error) {
	t.Helper()
	var list listFlag
	set := flag.NewFlagSet("t", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.Var(&list, "x", "")
	err := set.Parse(args)
	return list.values, err
}

func TestListFlagIsRepeatableAndCommaSeparated(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want []string
	}{
		"one":            {[]string{"-x", "a"}, []string{"a"}},
		"comma list":     {[]string{"-x", "a,b,c"}, []string{"a", "b", "c"}},
		"repeated":       {[]string{"-x", "a", "-x", "b"}, []string{"a", "b"}},
		"mixed":          {[]string{"-x", "a,b", "-x", "c"}, []string{"a", "b", "c"}},
		"spaces are cut": {[]string{"-x", " a , b "}, []string{"a", "b"}},
		"absent":         {nil, nil},
	} {
		got, err := parseList(t, tc.args...)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, %v; want %v", name, got, err, tc.want)
		}
	}
	for name, args := range map[string][]string{
		"empty value":    {"-x", ""},
		"trailing comma": {"-x", "a,"},
		"double comma":   {"-x", "a,,b"},
		"blank item":     {"-x", "a, ,b"},
		"empty repeat":   {"-x", "a", "-x", ""},
	} {
		if _, err := parseList(t, args...); err == nil {
			t.Errorf("%s: no error; an empty item must not narrow or widen the run silently", name)
		}
	}
}

func TestVerbRefusesAnEmptySelectionItemBeforeTouchingPostgres(t *testing.T) {
	for name, args := range map[string][]string{
		"empty source ids":   {"--config-id", "x", "--source-ids", ""},
		"blank dataset key":  {"--config-id", "x", "--dataset-keys", "commits,,prs"},
		"unknown flag stays": {"--config-id", "x", "--source-id", sourceA},
	} {
		var stderr strings.Builder
		code := runVerb(context.Background(), cli.Env{Args: args, Stderr: &stderr, Stdout: &stderr})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, stderr %s", name, code, stderr.String())
		}
	}
}

func plannerManaged() *Config {
	integration := integr
	return &Config{ID: cfgID, IntegrationID: &integration, PlannerManaged: true}
}

func pinned(source string) *Config {
	config := plannerManaged()
	config.PlannerManaged = false
	config.SourceID = &source
	return config
}

func refusal(t *testing.T, err error) string {
	t.Helper()
	var refused RefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("error %v is not a RefusedError", err)
	}
	return refused.Message
}

func TestScopeSources(t *testing.T) {
	enabled := []string{sourceA, sourceB, sourceC}
	t.Run("absent flag keeps the whole enabled list", func(t *testing.T) {
		got, err := scopeSources(plannerManaged(), nil, enabled)
		if err != nil || !reflect.DeepEqual(got, enabled) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("absent flag on a pinned configuration writes no list", func(t *testing.T) {
		got, err := scopeSources(pinned(sourceB), nil, enabled)
		if err != nil || got != nil {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("a subset keeps the enabled order, whatever the flag order or spelling", func(t *testing.T) {
		got, err := scopeSources(plannerManaged(), []string{strings.ToUpper(sourceC), strings.ReplaceAll(sourceA, "-", ""), sourceC}, enabled)
		if err != nil || !reflect.DeepEqual(got, []string{sourceA, sourceC}) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("an unknown source is named and refused", func(t *testing.T) {
		_, err := scopeSources(plannerManaged(), []string{sourceA, sourceX}, enabled)
		message := refusal(t, err)
		if !strings.Contains(message, sourceX) || strings.Contains(message, sourceB) || !strings.Contains(message, "nothing was started") {
			t.Fatalf("message %q", message)
		}
	})
	t.Run("a disabled source is the same as an unknown one", func(t *testing.T) {
		if _, err := scopeSources(plannerManaged(), []string{sourceC}, []string{sourceA, sourceB}); err == nil {
			t.Fatal("a source that is not enabled was accepted")
		}
	})
	t.Run("a text that is not a uuid is named and refused", func(t *testing.T) {
		_, err := scopeSources(plannerManaged(), []string{"acme/widgets"}, enabled)
		if message := refusal(t, err); !strings.Contains(message, `"acme/widgets"`) {
			t.Fatalf("message %q", message)
		}
	})
	t.Run("a pinned configuration accepts its own source and writes no list", func(t *testing.T) {
		got, err := scopeSources(pinned(sourceB), []string{sourceB}, enabled)
		if err != nil || got != nil {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("a pinned configuration whose source is not enabled is refused", func(t *testing.T) {
		_, err := scopeSources(pinned(sourceB), []string{sourceB}, []string{sourceA, sourceC})
		if message := refusal(t, err); !strings.Contains(message, sourceB) || !strings.Contains(message, "not an enabled source") {
			t.Fatalf("message %q", message)
		}
	})
	t.Run("a pinned configuration refuses another source", func(t *testing.T) {
		_, err := scopeSources(pinned(sourceB), []string{sourceA}, enabled)
		if message := refusal(t, err); !strings.Contains(message, sourceA) || !strings.Contains(message, sourceB) {
			t.Fatalf("message %q", message)
		}
	})
}

func TestScopeDatasets(t *testing.T) {
	// github's keys in DatasetKey order, of which the integration has these on.
	enabledRows := []string{"repo-metadata", "commits", "files"}
	t.Run("absent flag keeps what Validate resolved", func(t *testing.T) {
		got, err := scopeDatasets(plannerManaged(), "github", nil, enabledRows, nil)
		if err != nil || !reflect.DeepEqual(got, enabledRows) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("a subset of the enabled rows, in provider order", func(t *testing.T) {
		got, err := scopeDatasets(plannerManaged(), "github", []string{"files", "commits", "files"}, enabledRows, nil)
		if err != nil || !reflect.DeepEqual(got, []string{"commits", "files"}) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("a key the provider does not have is unknown", func(t *testing.T) {
		_, err := scopeDatasets(plannerManaged(), "github", []string{"commits", "no-such-dataset"}, enabledRows, nil)
		message := refusal(t, err)
		if !strings.Contains(message, `"no-such-dataset"`) || !strings.Contains(message, "known:") {
			t.Fatalf("message %q", message)
		}
	})
	t.Run("a key is exact: a different case is unknown", func(t *testing.T) {
		if _, err := scopeDatasets(plannerManaged(), "github", []string{"Commits"}, enabledRows, nil); err == nil {
			t.Fatal("the planner matches keys exactly, so Commits is not commits")
		}
	})
	t.Run("a known key whose row is off is refused: a backfill does not switch it on", func(t *testing.T) {
		_, err := scopeDatasets(plannerManaged(), "github", []string{"prs"}, enabledRows, nil)
		message := refusal(t, err)
		if !strings.Contains(message, `"prs"`) || !strings.Contains(message, "does not switch a dataset on") {
			t.Fatalf("message %q", message)
		}
	})
	t.Run("a pinned configuration is held to its sync_targets keys", func(t *testing.T) {
		config := pinned(sourceA)
		if _, err := scopeDatasets(config, "github", []string{"files"}, []string{"commits"}, nil); err == nil {
			t.Fatal("a key the configuration's sync_targets do not select was accepted")
		}
		got, err := scopeDatasets(config, "github", []string{"commits"}, []string{"commits", "files"}, nil)
		if err != nil || !reflect.DeepEqual(got, []string{"commits"}) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("a pinned configuration with no sync_targets is held to the enabled rows", func(t *testing.T) {
		got, err := scopeDatasets(pinned(sourceA), "github", []string{"commits"}, nil, []string{"commits", "files"})
		if err != nil || !reflect.DeepEqual(got, []string{"commits"}) {
			t.Fatalf("got %v, %v", got, err)
		}
		_, err = scopeDatasets(pinned(sourceA), "github", []string{"prs"}, nil, []string{"commits", "files"})
		if message := refusal(t, err); !strings.Contains(message, `"prs"`) || !strings.Contains(message, "does not switch a dataset on") {
			t.Fatalf("message %q", message)
		}
		if _, err := scopeDatasets(pinned(sourceA), "github", []string{"prs"}, nil, nil); err == nil {
			t.Fatal("a key with no enabled row at all was accepted")
		}
	})
}
