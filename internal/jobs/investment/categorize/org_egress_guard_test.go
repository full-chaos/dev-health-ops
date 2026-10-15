package categorize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// CHAOS-2558: an org BYO provider with a stored base_url dials through the
// SSRF-guarded transport; a loopback (or any internal) address is refused
// before a request byte -- the org's key included -- is sent. The
// env-configured provider is not customer-controlled and is unchanged.
func TestOrgBYOProvidersRefuseAnInternalBaseURLAtDial(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{}"}}],"usage":{}}`))
	}))
	defer server.Close()
	for _, kind := range []ProviderKind{ProviderKindOpenAI, ProviderKindLocal, ProviderKindOllama} {
		t.Run(string(kind), func(t *testing.T) {
			hits.Store(0)
			provider, err := NewProviderFromCredentials(kind, "org-secret-key", server.URL+"/v1", "m")
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Complete(context.Background(), CompletionRequest{Prompt: "p", SystemMessage: "s"})
			if err == nil {
				t.Fatal("the internal base_url was reached")
			}
			if hits.Load() != 0 {
				t.Fatalf("the server received %d requests", hits.Load())
			}
			if strings.Contains(err.Error(), "org-secret-key") || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("error %q carries the key or the URL", err.Error())
			}
		})
	}
}

func TestEnvConfiguredLocalProviderIsNotGuarded(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{}"}}],"usage":{}}`))
	}))
	defer server.Close()
	provider := NewLocalProvider(LocalProviderConfig{BaseURL: server.URL + "/v1", Model: "m"})
	if _, err := provider.Complete(context.Background(), CompletionRequest{Prompt: "p", SystemMessage: "s"}); err != nil {
		t.Fatalf("the env-configured local provider must reach its own server: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
}

func TestOrgHTTPClientIsGuardedOnlyForASuppliedBaseURL(t *testing.T) {
	if orgHTTPClient("", nil) != nil || orgHTTPClient("  ", nil) != nil {
		t.Fatal("no base_url must keep the provider default client")
	}
	if client := orgHTTPClient("https://org.example/v1", nil); client == nil || client.Transport == nil {
		t.Fatal("a supplied base_url must get the guarded client")
	}
}

// The transport-injecting constructor is a guard bypass: no production file
// may call it except NewProviderFromCredentials, which passes nil (the guarded
// transport). Tests in this package may call it.
func TestOnlyNewProviderFromCredentialsCallsTheTransportInjectingConstructor(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		calls := strings.Count(text, "newProviderFromCredentialsWithTransport(")
		switch {
		case name == "providerkind.go" && calls == 2 && strings.Contains(text, "newProviderFromCredentialsWithTransport(kind, apiKey, baseURL, model, nil)"):
		case calls == 0:
		default:
			t.Errorf("%s calls newProviderFromCredentialsWithTransport (%d occurrences): only NewProviderFromCredentials may, with a nil transport", name, calls)
		}
	}
}
