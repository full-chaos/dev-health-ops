package admin

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

func TestLLMSettingsResponseMasksKeyCarryingBaseURL(t *testing.T) {
	fakeKey := maskTestKey()
	h := &handlers{decryptor: unitDecryptor(t, unitKeyAfter)}
	cases := map[string]string{
		"userinfo":    "https://user:" + fakeKey + "@example.test/v1",
		"query":       "https://example.test/v1?api_key=" + fakeKey,
		"path":        "https://example.test/" + fakeKey + "/v1",
		"other scope": "https://example.test/v1?token=" + fakeKey,
	}
	for name, baseURL := range cases {
		t.Run(name, func(t *testing.T) {
			db := settingsDB{rows: map[string]setting{
				"provider":                 plainRow("provider", "openai"),
				"api_key":                  sealedRow(t, "api_key", fakeKey, unitKeyAfter),
				"base_url":                 plainRow("base_url", baseURL),
				"purpose:explain:provider": plainRow("purpose:explain:provider", "openai"),
				"purpose:explain:base_url": plainRow("purpose:explain:base_url", baseURL),
			}}
			out, err := h.llmSettingsResponse(context.Background(), db, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			body, err := out.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), fakeKey) {
				t.Fatalf("GET body carries the key: %s", body)
			}
		})
	}
}

func TestLLMSettingsResponseKeepsCleanBaseURL(t *testing.T) {
	const clean = "https://example.test/v1"
	h := &handlers{decryptor: unitDecryptor(t, unitKeyAfter)}
	db := settingsDB{rows: map[string]setting{
		"provider": plainRow("provider", "openai"),
		"api_key":  sealedRow(t, "api_key", maskTestKey(), unitKeyAfter),
		"base_url": plainRow("base_url", clean),
	}}
	out, err := h.llmSettingsResponse(context.Background(), db, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	body, err := out.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"base_url":"`+clean+`"`) {
		t.Fatalf("clean base_url changed: %s", body)
	}
}

func llmPurposeOf(scope string) llmorgsettings.Purpose { return llmorgsettings.Purpose(scope) }

func shownBaseURL(t *testing.T, out *pyjson.Object, scope string) string {
	t.Helper()
	if scope != "" {
		purposes, _ := out.Get("purposes")
		item, _ := purposes.(*pyjson.Object).Get(scope)
		out = item.(*pyjson.Object)
	}
	value, ok := out.Get("base_url")
	if !ok {
		t.Fatal("no base_url in the response")
	}
	return value.(string)
}

func TestMaskBaseURLKeyLikeParamNames(t *testing.T) {
	for _, name := range llmKeyLikeQueryParams {
		raw := "https://example.test/v1?" + name + "=not-a-stored-key&x=1"
		got := maskBaseURL(raw, nil)
		if strings.Contains(got, "not-a-stored-key") || !strings.Contains(got, name+"=***") || !strings.Contains(got, "x=1") {
			t.Errorf("%s: got %q", name, got)
		}
		upper := strings.ToUpper(name)
		if got := maskBaseURL("https://example.test/v1?"+upper+"=v", nil); strings.Contains(got, "=v") {
			t.Errorf("%s upper-case name not masked: %q", name, got)
		}
	}
}

func TestMaskBaseURLShapes(t *testing.T) {
	keys := []string{maskTestKey()}
	for raw, want := range map[string]string{
		"https://example.test/v1":                 "https://example.test/v1",
		"https://example.test:8443/a/b?x=1&y=2":   "https://example.test:8443/a/b?x=1&y=2",
		"https://u:p@example.test/v1":             "https://masked@example.test/v1",
		"https://example.test/" + keys[0] + "/v1": "********",
		"https://" + keys[0] + ".example.test/v1": "********",
		"https://example.test/v1?x=" + keys[0]:    "https://example.test/v1?x=***",
		"https://example.test/v1#" + keys[0]:      "********",
	} {
		if got := maskBaseURL(raw, keys); got != want {
			t.Errorf("maskBaseURL(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestBaseURLRoundTripKeepsStoredValue(t *testing.T) {
	fakeKey := maskTestKey()
	for name, stored := range map[string]string{
		"userinfo": "https://user:" + fakeKey + "@example.test/v1",
		"query":    "https://example.test/v1?api_key=" + fakeKey,
		"path":     "https://example.test/" + fakeKey + "/v1",
	} {
		for _, scope := range []string{"", "explain"} {
			t.Run(name+"/"+scope, func(t *testing.T) {
				prefix := ""
				if scope != "" {
					prefix = "purpose:" + scope + ":"
				}
				h := &handlers{
					decryptor:        unitDecryptor(t, unitKeyAfter),
					storedKey:        func(context.Context, string) (string, error) { return fakeKey, nil },
					storedPurposeKey: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
					storedScopeBaseURL: func(_ context.Context, _ string, p llmorgsettings.Purpose) (string, error) {
						if string(p) == scope {
							return stored, nil
						}
						return "", nil
					},
				}
				org := uuid.NewString()
				db := settingsDB{rows: map[string]setting{
					"provider":          plainRow("provider", "openai"),
					"api_key":           sealedRow(t, "api_key", fakeKey, unitKeyAfter),
					prefix + "provider": plainRow(prefix+"provider", "openai"),
					prefix + "base_url": plainRow(prefix+"base_url", stored),
				}}
				out, err := h.llmSettingsResponse(context.Background(), db, uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				shown := shownBaseURL(t, out, scope)
				if shown == stored {
					t.Fatalf("GET did not mask %q", stored)
				}
				got, refused, err := h.resolveBaseURL(context.Background(), nil, org, llmPurposeOf(scope), &shown)
				if err != nil || refused {
					t.Fatalf("refused=%v err=%v", refused, err)
				}
				if got == nil || *got != stored {
					t.Fatalf("PUT of the GET text: got %v, want stored %q", got, stored)
				}
				edited := strings.Replace(shown, "example.test", "edited.test", 1)
				if _, refused, err := h.resolveBaseURL(context.Background(), nil, org, llmPurposeOf(scope), &edited); err != nil || (shown != "********" && !refused) {
					t.Fatalf("a mask text that is not the stored mask must be refused: refused=%v err=%v", refused, err)
				}
				// another admin changed the url between the GET and the PUT
				h.storedScopeBaseURL = func(context.Context, string, llmorgsettings.Purpose) (string, error) {
					return "https://other.example.test/v2", nil
				}
				if got, refused, err := h.resolveBaseURL(context.Background(), nil, org, llmPurposeOf(scope), &shown); err != nil || !refused || got != nil {
					t.Fatalf("a stale masked url must be refused: got=%v refused=%v err=%v", got, refused, err)
				}
			})
		}
	}
}

func TestBaseURLCleanRoundTripUnchanged(t *testing.T) {
	const clean = "https://example.test/v1"
	h := &handlers{
		storedKey:          func(context.Context, string) (string, error) { return maskTestKey(), nil },
		storedPurposeKey:   func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
		storedScopeBaseURL: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return clean, nil },
	}
	sent := clean
	got, refused, err := h.resolveBaseURL(context.Background(), nil, uuid.NewString(), "", &sent)
	if err != nil || refused || got != &sent {
		t.Fatalf("clean url must pass through untouched: got %v err=%v", got, err)
	}
}

func TestMaskTextWithNoStoredURLIsRefused(t *testing.T) {
	h := &handlers{
		storedKey:          func(context.Context, string) (string, error) { return "", nil },
		storedPurposeKey:   func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
		storedScopeBaseURL: func(context.Context, string, llmorgsettings.Purpose) (string, error) { return "", nil },
	}
	for _, sent := range []string{"********", "https://masked@example.test/v1", "https://example.test/v1?api_key=***"} {
		got, refused, err := h.resolveBaseURL(context.Background(), nil, uuid.NewString(), "", &sent)
		if err != nil || !refused || got != nil {
			t.Errorf("%q: got=%v refused=%v err=%v", sent, got, refused, err)
		}
	}
}

// maskTestKey is a low-entropy fake key, built at run time.
func maskTestKey() string { return "fake-" + strings.Repeat("q", 20) }
