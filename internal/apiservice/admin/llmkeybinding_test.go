package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

var (
	bindingKeyAt      = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	bindingProviderAt = bindingKeyAt.Add(time.Hour)
)

// The write table: purpose x stored provider x new provider x api_key in the
// body, over a scope that stores a key bound to its provider. Purposes see a
// default row on anthropic whose key is bound. The expectation is the rule as
// CHAOS-13096 states it, written out here, not read from the code.
func TestProviderChangeKeyStepTable(t *testing.T) {
	const defaultProvider = "anthropic"
	needsKey := map[string]bool{"openai": true, "anthropic": true}
	knownKeyless := map[string]bool{"ollama": true}
	rows := 0
	for _, purpose := range []string{"", "investigate", "explain", "categorize"} {
		for _, old := range []string{"openai", "anthropic", "ollama", "none"} {
			for _, next := range []string{"openai", " OpenAI ", "anthropic", "ollama", "none"} {
				for _, bodyKey := range []bool{true, false} {
					rows++
					stored := scopeKeyRows{provider: old, providerAt: bindingKeyAt, keyStored: true, keyAt: bindingKeyAt}
					def := scopeKeyRows{provider: defaultProvider, providerAt: bindingKeyAt, keyStored: true, keyAt: bindingKeyAt}
					inherits := purpose != "" && def.suppliesKeyFor(next)
					want := keyStepKeep
					nextName := strings.ToLower(strings.TrimSpace(next))
					switch {
					case bodyKey:
						want = keyStepWrite
					case old == nextName:
						want = keyStepKeep
					case purpose != "" && nextName == defaultProvider:
						want = keyStepDelete
					case needsKey[nextName]:
						want = keyStepRefuse
					case knownKeyless[nextName]:
						want = keyStepDelete
					}
					if got := providerChangeKeyStep(stored, next, bodyKey, inherits); got != want {
						t.Errorf("purpose=%q %s -> %q body_key=%v: step=%d, want %d", purpose, old, next, bodyKey, got, want)
					}
				}
			}
		}
	}
	t.Logf("rows=%d", rows)
}

// No stored key: nothing can cross, so a provider change keeps the (absent) row.
func TestProviderChangeWithNoStoredKeyIsNotRefused(t *testing.T) {
	stored := scopeKeyRows{provider: "openai", providerAt: bindingKeyAt}
	if got := providerChangeKeyStep(stored, "anthropic", false, false); got != keyStepKeep {
		t.Fatalf("step=%d, want keep", got)
	}
}

// An override inherits the default key only for the default's provider and
// only while that key is bound to it.
func TestTheDefaultSuppliesItsKeyOnlyWhenBoundAndSameProvider(t *testing.T) {
	for _, c := range []struct {
		name     string
		def      scopeKeyRows
		provider string
		want     bool
	}{
		{"bound, same", scopeKeyRows{provider: "anthropic", providerAt: bindingKeyAt, keyStored: true, keyAt: bindingKeyAt}, "Anthropic", true},
		{"bound, other", scopeKeyRows{provider: "anthropic", providerAt: bindingKeyAt, keyStored: true, keyAt: bindingKeyAt}, "openai", false},
		{"unbound, same", scopeKeyRows{provider: "anthropic", providerAt: bindingProviderAt, keyStored: true, keyAt: bindingKeyAt}, "anthropic", false},
		{"no key", scopeKeyRows{provider: "anthropic", providerAt: bindingKeyAt}, "anthropic", false},
	} {
		if got := c.def.suppliesKeyFor(c.provider); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The refusal names its code and carries no key.
func TestProviderChangeRefusalShape(t *testing.T) {
	rec := httptest.NewRecorder()
	providerChangeRefusal().write(rec)
	var body struct {
		Detail map[string]string `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 400 || body.Detail["error"] != "provider_change_requires_api_key" || body.Detail["feature"] != byoLLMFeature {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// The readers that send the default key out (live model list, readiness probe)
// get no key written for an earlier provider; the base_url check still sees it.
func TestAnUnboundDefaultKeyReachesNoVendor(t *testing.T) {
	stale := scopeKeyRows{provider: "anthropic", providerAt: bindingProviderAt, keyStored: true, keyAt: bindingKeyAt}
	bound := scopeKeyRows{provider: "anthropic", providerAt: bindingKeyAt, keyStored: true, keyAt: bindingProviderAt}
	if stale.keyBound() || !bound.keyBound() {
		t.Fatalf("keyBound: stale=%v bound=%v", stale.keyBound(), bound.keyBound())
	}
	key := "sk-openai-" + "0123456789"
	cfg := readinessBYOConfig{provider: "anthropic", model: "claude-haiku-5-5", apiKey: secrets.NewHidden(key), baseURL: "https://gateway.example.invalid/v1"}.bindDefaultKey(stale.keyBound())

	h, doer := newModelListHandlers()
	out, err := h.modelList(context.Background(), "org", cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	view, raw := viewModelList(t, out)
	t.Logf("model list: source=%s vendor_requests=%d", view.source, len(doer.requests))
	if len(doer.requests) != 0 || strings.Contains(raw, key) {
		t.Errorf("the model list sent an unbound key to the vendor: requests=%d", len(doer.requests))
	}
	if cfg.apiKey.Configured() {
		t.Errorf("the readiness probe would be usable with an unbound key")
	}

	spy := &resolverSpy{valid: true}
	keyed := cfg
	keyed.baseURL = "https://" + key + ".example.invalid/v1"
	if valid, err := readerHandlers(spy).readinessURLValid(context.Background(), keyed); err != nil || valid || spy.calls != 0 {
		t.Errorf("a base_url holding the unbound key passed: valid=%v err=%v resolver calls=%d", valid, err, spy.calls)
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg), key) {
		t.Errorf("a printed config carries the unbound key")
	}
}
