package investment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/orgllmconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type fakeOrgConfig struct {
	cfg   orgllmconfig.Config
	err   error
	calls int
}

func (f *fakeOrgConfig) Fetch(context.Context, string) (orgllmconfig.Config, error) {
	f.calls++
	return f.cfg, f.err
}

const orgKeyValue = "placeholder-org-key-value"

func orgExecutor(t *testing.T, fetch *fakeOrgConfig) (*NativeExecutor, *bytes.Buffer, *int) {
	t.Helper()
	var logs bytes.Buffer
	platformCalls := 0
	executor := &NativeExecutor{
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
		newProvider: func(requested, model string) (categorize.Provider, categorize.ProviderKind, error) {
			platformCalls++
			return categorize.MockProvider{}, categorize.ProviderKindMock, nil
		},
	}
	if fetch != nil {
		executor.SetOrgLLMConfig(fetch, nil)
	}
	return executor, &logs, &platformCalls
}

func orgCfg(provider, model, key string) orgllmconfig.Config {
	return orgllmconfig.Config{Provider: provider, Model: model, APIKey: secrets.NewHidden(key), Version: "v1"}
}

func TestOrgProviderUsesTheOrgConfigAndStampsTheModelThatRan(t *testing.T) {
	fetch := &fakeOrgConfig{cfg: orgCfg("openai", "org-small-model", orgKeyValue)}
	executor, logs, platform := orgExecutor(t, fetch)
	provider, kind, stamp, err := executor.providerForRun(context.Background(), "org-1", "auto", "")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if kind != categorize.ProviderKindOpenAI || stamp != "org-small-model" || provider.Model() != stamp {
		t.Fatalf("kind %s stamp %q provider model %q", kind, stamp, provider.Model())
	}
	if *platform != 0 {
		t.Fatal("the platform provider was built for a BYO org")
	}
	if strings.Contains(logs.String(), orgKeyValue) || !strings.Contains(logs.String(), "org-small-model") {
		t.Fatalf("log must name the model and never the key: %s", logs.String())
	}
}

func TestOrgProviderExplicitRequestModelWins(t *testing.T) {
	executor, _, _ := orgExecutor(t, &fakeOrgConfig{cfg: orgCfg("openai", "org-model", orgKeyValue)})
	provider, _, stamp, err := executor.providerForRun(context.Background(), "org-1", "auto", "asked-model")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if stamp != "asked-model" || provider.Model() != "asked-model" {
		t.Fatalf("stamp %q provider %q", stamp, provider.Model())
	}
}

func TestOrgProviderFallsBackToThePlatformOnlyWhenNoConfigApplies(t *testing.T) {
	for name, tc := range map[string]struct {
		fetch     *fakeOrgConfig
		org       string
		requested string
		wantFetch int
	}{
		"feature off":             {fetch: nil, org: "org-1", requested: "auto"},
		"org has none":            {fetch: &fakeOrgConfig{err: orgllmconfig.ErrNotConfigured}, org: "org-1", requested: "auto", wantFetch: 1},
		"unscoped run":            {fetch: &fakeOrgConfig{}, org: "", requested: "auto"},
		"explicit other provider": {fetch: &fakeOrgConfig{cfg: orgCfg("openai", "m", orgKeyValue)}, org: "org-1", requested: "mock", wantFetch: 1},
	} {
		t.Run(name, func(t *testing.T) {
			executor, _, platform := orgExecutor(t, tc.fetch)
			provider, _, stamp, err := executor.providerForRun(context.Background(), tc.org, tc.requested, "ref-model")
			if err != nil {
				t.Fatal(err)
			}
			defer provider.Close()
			if *platform != 1 || stamp != "ref-model" {
				t.Fatalf("platform calls %d stamp %q", *platform, stamp)
			}
			if tc.fetch != nil && tc.fetch.calls != tc.wantFetch {
				t.Fatalf("fetch calls %d, want %d", tc.fetch.calls, tc.wantFetch)
			}
		})
	}
}

// A BYO org is never silently moved to the platform provider.
func TestOrgProviderFailsClosed(t *testing.T) {
	for name, tc := range map[string]struct {
		fetch         *fakeOrgConfig
		deterministic bool
	}{
		"api refused":          {&fakeOrgConfig{err: fmt.Errorf("%w: status 401", orgllmconfig.ErrRefused)}, true},
		"api unavailable":      {&fakeOrgConfig{err: fmt.Errorf("%w: status 503", orgllmconfig.ErrUnavailable)}, false},
		"unknown provider":     {&fakeOrgConfig{cfg: orgCfg("nonesuch", "m", orgKeyValue)}, true},
		"no native client":     {&fakeOrgConfig{cfg: orgCfg("lmstudio", "m", "")}, true},
		"hosted without a key": {&fakeOrgConfig{cfg: orgCfg("openai", "m", "")}, true},
		"ssrf base url": {&fakeOrgConfig{cfg: orgllmconfig.Config{
			Provider: "openai", Model: "m", APIKey: secrets.NewHidden(orgKeyValue), BaseURL: "http://169.254.169.254/v1",
		}}, true},
	} {
		t.Run(name, func(t *testing.T) {
			executor, logs, platform := orgExecutor(t, tc.fetch)
			_, _, _, err := executor.providerForRun(context.Background(), "org-1", "auto", "")
			if err == nil {
				t.Fatal("a BYO org run proceeded")
			}
			var det *workgraph.DeterministicError
			if got := errors.As(err, &det); got != tc.deterministic {
				t.Fatalf("deterministic = %v, want %v (%v)", got, tc.deterministic, err)
			}
			if *platform != 0 {
				t.Fatal("fell back to the platform provider")
			}
			if strings.Contains(logs.String(), orgKeyValue) || strings.Contains(err.Error(), orgKeyValue) {
				t.Fatalf("a secret leaked: %s / %v", logs.String(), err)
			}
			if !strings.Contains(logs.String(), "org_id=org-1") {
				t.Fatalf("the failure is not logged with the org id: %s", logs.String())
			}
		})
	}
}
