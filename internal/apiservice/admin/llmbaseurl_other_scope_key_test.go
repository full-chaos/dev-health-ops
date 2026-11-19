package admin

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The default-row readers that send a request to the stored base url (readiness
// probe, model list) or answer with it (spend, status) refuse a url that carries
// a key of another scope (categorize, investigate, explain) before any lookup,
// not only the default row's key.

const otherScopeKey = "sk-other-scope-" + "0123456789abcdef"

func otherScopeHandlers(spy *resolverSpy, scopeKeys map[llmorgsettings.Purpose]string) *handlers {
	return &handlers{
		modelCache:         newLLMModelListCache(),
		validateURL:        spy.validate,
		storedKey:          func(context.Context, string) (string, error) { return readerStoredKey, nil },
		storedPurposeKey:   func(_ context.Context, _ string, p llmorgsettings.Purpose) (string, error) { return scopeKeys[p], nil },
		storedScopeBaseURL: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
	}
}

// otherScopeURLs are the url forms that carry otherScopeKey.
var otherScopeURLs = map[string]string{
	"query": "https://8.8.8.8/v1?k=" + otherScopeKey,
	"path":  "https://8.8.8.8/v1/" + otherScopeKey,
	"host":  "https://" + otherScopeKey + ".example.invalid/v1",
}

var otherScopes = []llmorgsettings.Purpose{llmorgsettings.PurposeCategorize, llmorgsettings.PurposeInvestigate, llmorgsettings.PurposeExplain}

func TestReadinessRefusesAnotherScopesKeyInTheBaseURL(t *testing.T) {
	for _, scope := range otherScopes {
		for form, url := range otherScopeURLs {
			spy := &resolverSpy{valid: true}
			h := otherScopeHandlers(spy, map[llmorgsettings.Purpose]string{scope: otherScopeKey})
			cfg := readinessBYOConfig{provider: "anthropic", model: "m", apiKey: secrets.NewHidden(readerStoredKey), baseURL: url}
			if valid, err := h.readinessURLValid(context.Background(), "org", cfg); err != nil || valid || spy.calls != 0 {
				t.Errorf("%s key in the %s: valid=%v err=%v resolver calls=%d", scope, form, valid, err, spy.calls)
			}
		}
	}
	spy := &resolverSpy{valid: true}
	h := otherScopeHandlers(spy, map[llmorgsettings.Purpose]string{llmorgsettings.PurposeCategorize: otherScopeKey})
	clean := readinessBYOConfig{provider: "anthropic", model: "m", apiKey: secrets.NewHidden(readerStoredKey), baseURL: readerCleanURL}
	if valid, err := h.readinessURLValid(context.Background(), "org", clean); err != nil || !valid || spy.calls != 1 {
		t.Fatalf("clean: valid=%v err=%v resolver calls=%d", valid, err, spy.calls)
	}
}

func TestSpendUsabilityRefusesAnotherScopesKeyInTheBaseURL(t *testing.T) {
	for _, scope := range otherScopes {
		for form, url := range otherScopeURLs {
			spy := &resolverSpy{valid: true}
			h := otherScopeHandlers(spy, map[llmorgsettings.Purpose]string{scope: otherScopeKey})
			if ok, err := h.byoConfigUsable(context.Background(), "org", "openai", readerStoredKey, url); err != nil || ok || spy.calls != 0 {
				t.Errorf("%s key in the %s: ok=%v err=%v resolver calls=%d", scope, form, ok, err, spy.calls)
			}
		}
	}
}

func TestModelListRefusesAnotherScopesKeyInTheBaseURL(t *testing.T) {
	for _, scope := range otherScopes {
		for form, url := range otherScopeURLs {
			spy := &resolverSpy{valid: true}
			h := otherScopeHandlers(spy, map[llmorgsettings.Purpose]string{scope: otherScopeKey})
			doer := &scriptedModelDoer{}
			h.upstreamDoer = doer
			cfg := readinessBYOConfig{provider: "openai", apiKey: secrets.NewHidden(readerStoredKey), baseURL: url}
			out, err := h.modelList(context.Background(), "org", cfg, "")
			if err != nil || spy.calls != 0 || len(doer.requests) != 0 {
				t.Errorf("%s key in the %s: err=%v resolver calls=%d provider calls=%d", scope, form, err, spy.calls, len(doer.requests))
				continue
			}
			body, _ := out.MarshalJSON()
			if !strings.Contains(string(body), `"reason":"invalid_base_url"`) || strings.Contains(string(body), otherScopeKey) {
				t.Errorf("%s key in the %s: body %s", scope, form, body)
			}
		}
	}
}

// The status reader: an IP-literal url needs no lookup, so main answered active.
func TestStatusRefusesAnotherScopesKeyInTheBaseURL(t *testing.T) {
	for _, scope := range otherScopes {
		for _, form := range []string{"query", "path"} {
			h := otherScopeHandlers(&resolverSpy{}, map[llmorgsettings.Purpose]string{scope: otherScopeKey})
			reason, err := h.statusCredentials(context.Background(), "org", "openai", readerStoredKey, otherScopeURLs[form])
			if err != nil || reason != llmorgsettings.ConfigInvalidBaseURL {
				t.Errorf("%s key in the %s: reason=%q err=%v", scope, form, reason, err)
			}
		}
	}
	h := otherScopeHandlers(&resolverSpy{}, map[llmorgsettings.Purpose]string{llmorgsettings.PurposeCategorize: otherScopeKey})
	if reason, err := h.statusCredentials(context.Background(), "org", "openai", readerStoredKey, "https://8.8.8.8/v1"); err != nil || reason != llmorgsettings.ConfigActive {
		t.Fatalf("clean ip-literal url: reason=%q err=%v", reason, err)
	}
}

// A reader with no base url reads no other scope's key.
func TestReadersReadNoOtherScopeKeyWithoutABaseURL(t *testing.T) {
	h := &handlers{
		storedKey: func(context.Context, string) (string, error) { t.Fatal("default key read"); return "", nil },
		storedPurposeKey: func(context.Context, string, llmorgsettings.Purpose) (string, error) {
			t.Fatal("purpose key read")
			return "", nil
		},
	}
	keys, err := h.readerLeakKeys(context.Background(), "org", "", "held")
	if err != nil || len(keys) != 1 || keys[0] != "held" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}
