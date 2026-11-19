package admin

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// llmWriteScopes are the default row ("") and every purpose override a PUT writes.
var llmWriteScopes = []llmorgsettings.Purpose{"", llmorgsettings.PurposeInvestigate, llmorgsettings.PurposeExplain, llmorgsettings.PurposeCategorize}

func scopeName(scope llmorgsettings.Purpose) string {
	if scope == "" {
		return "default"
	}
	return string(scope)
}

func scopeKeyText(scope llmorgsettings.Purpose) string {
	return "sk-scope-" + scopeName(scope) + "-0123456789"
}

// keysetWrite runs the PUT write of one scope up to its transaction: a nil
// store panics at Begin, which reads as "reached the write".
func keysetWrite(h *handlers, scope llmorgsettings.Purpose, apiKey, baseURL *string) (refusal *llmRefusal, reachedWrite bool, err error) {
	defer func() {
		if recover() != nil {
			reachedWrite = true
		}
	}()
	if scope == "" {
		_, refusal, err = h.applyLLMSettings(context.Background(), "org", llmUpsert{provider: "openai", model: strPtr("m"), apiKey: apiKey, baseURL: baseURL})
	} else {
		_, refusal, err = h.applyLLMPurpose(context.Background(), "org", llmPurposeUpsert{purpose: scope, provider: "openai", model: "m", apiKey: apiKey, baseURL: baseURL})
	}
	return refusal, false, err
}

// keysetHandlers stores one api key, in scope stored, and no other.
func keysetHandlers(spy *resolverSpy, stored llmorgsettings.Purpose) *handlers {
	return &handlers{
		modelCache:  newLLMModelListCache(),
		validateURL: spy.validate,
		storedKey: func(context.Context, string) (string, error) {
			if stored == "" {
				return scopeKeyText(""), nil
			}
			return "", nil
		},
		storedPurposeKey: func(_ context.Context, _ string, purpose llmorgsettings.Purpose) (string, error) {
			if stored != "" && purpose == stored {
				return scopeKeyText(stored), nil
			}
			return "", nil
		},
		storedScopeBaseURL: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
	}
}

// baseURLHandlers stores one base_url, in scope stored, and no api key.
func baseURLHandlers(spy *resolverSpy, stored llmorgsettings.Purpose, baseURL string) *handlers {
	return &handlers{
		modelCache:       newLLMModelListCache(),
		validateURL:      spy.validate,
		storedKey:        func(context.Context, string) (string, error) { return "", nil },
		storedPurposeKey: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
		storedScopeBaseURL: func(_ context.Context, _ string, purpose llmorgsettings.Purpose) (string, error) {
			if purpose == stored {
				return baseURL, nil
			}
			return "", nil
		},
	}
}

// A base_url saved in any scope is refused when it carries the stored api key of
// any scope of the org, the default row's or any purpose override's.
func TestLLMWriteRefusesABaseURLCarryingAnyStoredKey(t *testing.T) {
	for _, writer := range llmWriteScopes {
		for _, stored := range llmWriteScopes {
			name := fmt.Sprintf("write %s, key stored in %s", scopeName(writer), scopeName(stored))
			spy := &resolverSpy{valid: true}
			baseURL := "https://p.example.invalid/" + scopeKeyText(stored)
			refusal, reached, err := keysetWrite(keysetHandlers(spy, stored), writer, nil, &baseURL)
			if err != nil || reached || refusal == nil || refusal.status != 400 || spy.calls != 0 {
				t.Errorf("%s: refusal=%v reached write=%v err=%v resolver calls=%d", name, refusal, reached, err, spy.calls)
			}
		}
	}
}

// A key saved in any scope is refused when the stored base_url of another scope
// already carries it, the default row's included; nothing is resolved or written.
func TestLLMWriteRefusesAKeyAnotherScopesStoredBaseURLCarries(t *testing.T) {
	fresh := "sk-fresh-" + "0123456789"
	for _, writer := range llmWriteScopes {
		for _, stored := range llmWriteScopes {
			if stored == writer {
				continue
			}
			name := fmt.Sprintf("write %s key, base_url stored in %s", scopeName(writer), scopeName(stored))
			spy := &resolverSpy{valid: true}
			h := baseURLHandlers(spy, stored, "https://p.example.invalid/"+fresh)
			for body, baseURL := range map[string]*string{"no base_url": nil, "clean base_url": strPtr(readerCleanURL)} {
				refusal, reached, err := keysetWrite(h, writer, &fresh, baseURL)
				if err != nil || reached || refusal == nil || refusal.status != 400 || spy.calls != 0 {
					t.Errorf("%s, %s: refusal=%v reached write=%v err=%v resolver calls=%d", name, body, refusal, reached, err, spy.calls)
				}
			}
		}
	}
}

// The own scope's stored base_url is replaced by the write, so it does not refuse
// the new key; a write with no key in any url reaches the write.
func TestLLMWriteAcceptsTheOwnStoredBaseURLAndCleanURLs(t *testing.T) {
	fresh := "sk-fresh-" + "0123456789"
	for _, writer := range llmWriteScopes {
		spy := &resolverSpy{valid: true}
		h := baseURLHandlers(spy, writer, "https://p.example.invalid/"+fresh)
		if refusal, reached, err := keysetWrite(h, writer, &fresh, strPtr(readerCleanURL)); err != nil || !reached || refusal != nil || spy.calls != 1 {
			t.Errorf("write %s, own stored base_url carries the key: refusal=%v reached write=%v err=%v resolver calls=%d", scopeName(writer), refusal, reached, err, spy.calls)
		}
		spy = &resolverSpy{valid: true}
		h = baseURLHandlers(spy, "", readerCleanURL)
		h.storedKey = func(context.Context, string) (string, error) { return readerStoredKey, nil }
		h.storedPurposeKey = func(context.Context, string, llmorgsettings.Purpose) (string, error) { return readerPurposeKey, nil }
		if refusal, reached, err := keysetWrite(h, writer, &fresh, strPtr(readerCleanURL)); err != nil || !reached || refusal != nil || spy.calls != 1 {
			t.Errorf("write %s, clean: refusal=%v reached write=%v err=%v resolver calls=%d", scopeName(writer), refusal, reached, err, spy.calls)
		}
	}
}

// The refusal of a key carried by another scope's base_url is the existing safe
// class, byte for byte, with no key or url text.
func TestLLMCarriedKeyRefusalIsTheSafeClass(t *testing.T) {
	fresh := "sk-fresh-" + "0123456789"
	for _, writer := range llmWriteScopes {
		stored := llmorgsettings.PurposeExplain
		if writer == stored {
			stored = ""
		}
		refusal, _, err := keysetWrite(baseURLHandlers(&resolverSpy{valid: true}, stored, "https://p.example.invalid/"+fresh), writer, &fresh, nil)
		if err != nil || refusal == nil {
			t.Fatalf("write %s: refusal=%v err=%v", scopeName(writer), refusal, err)
		}
		rec := httptest.NewRecorder()
		refusal.write(rec)
		text := rec.Body.String()
		for _, leak := range []string{fresh, "example.invalid"} {
			if strings.Contains(strings.ToLower(text), strings.ToLower(leak)) {
				t.Fatalf("write %s: refusal leaked %q: %s", scopeName(writer), leak, text)
			}
		}
		keyHit, _, _ := keysetWrite(keysetHandlers(&resolverSpy{valid: true}, writer), writer, nil, strPtr("https://p.example.invalid/"+scopeKeyText(writer)))
		want := httptest.NewRecorder()
		keyHit.write(want)
		if rec.Code != want.Code || text != want.Body.String() {
			t.Fatalf("write %s: refusal %d %s, want the key-hit refusal %d %s", scopeName(writer), rec.Code, text, want.Code, want.Body.String())
		}
	}
}
