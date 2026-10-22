package admin

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	readerStoredKey  = "sk-stored-" + "0123456789"
	readerPurposeKey = "sk-purpose-" + "0123456789"
	readerFreshKey   = "sk-fresh-" + "0123456789"
	readerCleanURL   = "https://p.example.invalid/v1"
)

func readerHandlers(spy *resolverSpy) *handlers {
	return &handlers{
		validateURL:      spy.validate,
		storedKey:        func(context.Context, string) (string, error) { return readerStoredKey, nil },
		storedPurposeKey: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return readerPurposeKey, nil },
	}
}

// The purpose override path: the new key, the org's stored key and the override's
// stored key each refuse a base_url that carries them, with zero lookups.
func TestApplyLLMPurposeRefusesAKeyInTheBaseURLBeforeResolution(t *testing.T) {
	for name, tc := range map[string]llmPurposeUpsert{
		"new key in the host": {
			purpose: llmorgsettings.PurposeInvestigate, provider: "openai", model: "m",
			apiKey: strPtr(readerFreshKey), baseURL: strPtr("https://" + readerFreshKey + ".example.invalid/v1"),
		},
		"org stored key in the host": {
			purpose: llmorgsettings.PurposeInvestigate, provider: "openai", model: "m",
			baseURL: strPtr("https://" + readerStoredKey + ".example.invalid/v1"),
		},
		"purpose stored key in the path": {
			purpose: llmorgsettings.PurposeExplain, provider: "openai", model: "m",
			baseURL: strPtr("https://p.example.invalid/" + readerPurposeKey),
		},
	} {
		spy := &resolverSpy{valid: true}
		out, refusal, err := readerHandlers(spy).applyLLMPurpose(context.Background(), "org", tc)
		if err != nil || out != nil || refusal == nil || refusal.status != 400 || spy.calls != 0 {
			t.Fatalf("%s: out=%v refusal=%v err=%v resolver calls=%d", name, out, refusal, err, spy.calls)
		}
	}
}

func TestReadinessURLValidRefusesAKeyBeforeResolution(t *testing.T) {
	spy := &resolverSpy{valid: true}
	h := readerHandlers(spy)
	keyed := readinessBYOConfig{provider: "anthropic", apiKey: secrets.NewHidden(readerStoredKey), baseURL: "https://" + readerStoredKey + ".example.invalid"}
	if valid, err := h.readinessURLValid(context.Background(), keyed); err != nil || valid || spy.calls != 0 {
		t.Fatalf("keyed: valid=%v err=%v resolver calls=%d", valid, err, spy.calls)
	}
	clean := readinessBYOConfig{provider: "anthropic", apiKey: secrets.NewHidden(readerStoredKey), baseURL: readerCleanURL}
	if valid, err := h.readinessURLValid(context.Background(), clean); err != nil || !valid || spy.calls != 1 {
		t.Fatalf("clean: valid=%v err=%v resolver calls=%d", valid, err, spy.calls)
	}
}

func TestSpendUsabilityRefusesAKeyBeforeResolution(t *testing.T) {
	spy := &resolverSpy{valid: true}
	h := readerHandlers(spy)
	if ok, err := h.byoConfigUsable(context.Background(), "openai", readerStoredKey, "https://p.example.invalid/"+readerStoredKey); err != nil || ok || spy.calls != 0 {
		t.Fatalf("keyed: ok=%v err=%v resolver calls=%d", ok, err, spy.calls)
	}
	if ok, err := h.byoConfigUsable(context.Background(), "openai", readerStoredKey, readerCleanURL); err != nil || !ok || spy.calls != 1 {
		t.Fatalf("clean: ok=%v err=%v resolver calls=%d", ok, err, spy.calls)
	}
}

// A refused base_url answers the one safe class, with no key or url text.
func TestPurposeKeyRefusalCarriesNoKeyOrURLText(t *testing.T) {
	spy := &resolverSpy{valid: true}
	badURL := "https://" + readerStoredKey + ".example.invalid/v1"
	_, refusal, err := readerHandlers(spy).applyLLMPurpose(context.Background(), "org", llmPurposeUpsert{
		purpose: llmorgsettings.PurposeInvestigate, provider: "openai", model: "m", baseURL: &badURL,
	})
	if err != nil || refusal == nil {
		t.Fatalf("refusal=%v err=%v", refusal, err)
	}
	rec := httptest.NewRecorder()
	refusal.write(rec)
	text := strings.ToLower(rec.Body.String())
	for _, leak := range []string{readerStoredKey, "example.invalid"} {
		if strings.Contains(text, leak) {
			t.Fatalf("refusal leaked %q: %s", leak, text)
		}
	}
	if !strings.Contains(text, "invalid_base_url") || !strings.Contains(text, "invalid llm base_url") {
		t.Fatalf("refusal is not the safe class: %s", text)
	}
}
