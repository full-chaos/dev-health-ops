package investment

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The gate and the build of one categorize run serve from ONE config read: a
// save that lands between the gate's check and the build (here: every read
// after the first answers a base_url that carries a stored key, flagged) never
// reaches a provider.

const flipCarriedKey = "sk-flipped-categorize-" + "0123456789"

type flipConfig struct {
	reads        int
	firstCarries bool
}

func (f *flipConfig) read(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
	f.reads++
	cfg := llmorgsettings.OrgConfig{Provider: "openai", Model: "org-model-1", APIKey: secrets.NewHidden("sk-org-test"), BaseURL: "https://models.example.invalid/v1"}
	if f.reads > 1 || f.firstCarries {
		cfg.BaseURL = "https://models.example.invalid/v1?k=" + flipCarriedKey
		cfg.KeyInBaseURL = true
	}
	return cfg, true, nil
}

// checkingGate is the model gate's config half: it refuses a flagged or
// key-carrying base url and admits any other found config.
func checkingGate(ctx context.Context, orgID string, config llmorgsettings.ConfigSource) llmorgsettings.Decision {
	cfg, found, err := config.OrgConfig(ctx, orgID)
	if err != nil || !found {
		return llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: string(llmorgsettings.ConfigUnreadableRow)}
	}
	if cfg.KeyInBaseURL || llmorgsettings.BaseURLCarriesKey(cfg.BaseURL, cfg.APIKey.Reveal()) {
		return llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: string(llmorgsettings.ConfigInvalidBaseURL)}
	}
	return llmorgsettings.Decision{Kind: llmorgsettings.Own}
}

func flipBYO(flip *flipConfig, urls *[]string) *orgCategorizeBYO {
	return &orgCategorizeBYO{
		logger:  testLogger(),
		optedIn: func(context.Context, string) (bool, error) { return true, nil },
		gate:    checkingGate,
		config:  flip.read,
		newProvider: func(_ categorize.ProviderKind, _, baseURL, model string) (categorize.Provider, error) {
			*urls = append(*urls, baseURL)
			return &stubOrgModel{model: model, reply: answerAllQuestions}, nil
		},
	}
}

func TestBuildServesTheConfigTheGateChecked(t *testing.T) {
	flip := &flipConfig{}
	var urls []string
	served, err := flipBYO(flip, &urls).Build(context.Background(), "org-1")
	if err != nil || served == nil {
		t.Fatalf("Build = %v, %v; want the gate-checked config served", served, err)
	}
	if flip.reads != 1 {
		t.Fatalf("config read %d times, want once for the gate and the build", flip.reads)
	}
	for _, url := range append(urls, served.backend.baseURL) {
		if strings.Contains(url, flipCarriedKey) {
			t.Fatalf("a provider or backend got the key-carrying url saved after the gate")
		}
	}
	if len(urls) != 1 || urls[0] != "https://models.example.invalid/v1" {
		t.Fatalf("provider urls = %d, want the one gate-checked url", len(urls))
	}
}

func TestBuildRefusesAFlaggedConfigWithNoRequest(t *testing.T) {
	flip := &flipConfig{firstCarries: true}
	var urls []string
	served, err := flipBYO(flip, &urls).Build(context.Background(), "org-1")
	var refusal *BYORefusal
	if !errors.As(err, &refusal) || served != nil || refusal.Code != string(llmorgsettings.ConfigInvalidBaseURL) {
		t.Fatalf("Build = %v, %v; want an invalid_base_url refusal", served, err)
	}
	if len(urls) != 0 || flip.reads != 1 {
		t.Fatalf("provider builds = %d, reads = %d; want 0 and 1", len(urls), flip.reads)
	}
}

func TestOneConfigReadRefusesAnotherOrg(t *testing.T) {
	read := &oneConfigRead{read: (&flipConfig{}).read}
	if _, _, err := read.OrgConfig(context.Background(), "org-1"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := read.OrgConfig(context.Background(), "org-2"); err == nil || found {
		t.Fatalf("a read for another org was served: found=%v err=%v", found, err)
	}
}
