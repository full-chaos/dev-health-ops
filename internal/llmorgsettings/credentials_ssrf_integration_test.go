//go:build integration

package llmorgsettings

import (
	"context"
	"testing"
)

// The SSRF guard of Store.Credentials had no test: with its refusal switched off every test of the package still passed,
// and Credentials then returned the stored api key together with the refused base_url (finding by gwc-vetter-3). These
// cases store a base_url the guard refuses and assert what every path that reaches Credentials answers: Credentials
// itself, Matches (which is Credentials without the bundle) and ResolveUsableProvider. The api key is a made-up value;
// every address is a literal, so no name is resolved.

const syntheticKey = "sk-synthetic-ssrf-refusal-test-value"

func TestCredentialsRefuseABaseURLTheSSRFGuardRefuses(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t) // one Postgres for every case; each case seeds its own org
	seedByoLLMFeature(ctx, t, pool, "team", true)
	for _, refused := range []struct{ name, url string }{
		{"http scheme to the metadata address", "http://169.254.169.254/latest"},
		{"https to a private IPv4 literal", "https://10.0.0.5/v1"},
		{"https to a loopback literal", "https://127.0.0.1/v1"},
		{"https to an IPv4-mapped CGNAT literal", "https://[::ffff:100.64.0.1]/v1"},
		{"https to a documentation address", "https://198.51.100.7/v1"},
		{"https to localhost", "https://localhost/v1"},
		{"userinfo in the url", "https://user@8.8.8.8/v1"},
	} {
		for _, provider := range []struct {
			name, key string
		}{
			{"openai", syntheticKey}, // a provider that needs a key
			{"ollama", ""},           // a provider that needs only a base_url
		} {
			t.Run(refused.name+"/"+provider.name, func(t *testing.T) {
				orgID := seedOrg(ctx, t, pool, "enterprise")
				insertSetting(ctx, t, pool, orgID, "provider", provider.name)
				if provider.key != "" {
					insertSetting(ctx, t, pool, orgID, "api_key", provider.key)
				}
				insertSetting(ctx, t, pool, orgID, "base_url", refused.url)

				creds, ok, err := store.Credentials(ctx, orgID.String(), provider.name)
				if err != nil {
					t.Fatalf("Credentials returned an error for a refused base_url: %v", err)
				}
				if ok {
					t.Fatalf("Credentials accepted a base_url the SSRF guard refuses")
				}
				if creds.APIKey != "" || creds.BaseURL != "" {
					t.Fatalf("Credentials returned key material or a base_url with ok=false (api key set: %v, base_url set: %v)", creds.APIKey != "", creds.BaseURL != "")
				}
				// "auto" asks for whatever the org configured: the same refusal.
				creds, ok, err = store.Credentials(ctx, orgID.String(), "auto")
				if err != nil || ok || creds.APIKey != "" || creds.BaseURL != "" {
					t.Fatalf("Credentials(auto): ok=%v err=%v api key set=%v base_url set=%v", ok, err, creds.APIKey != "", creds.BaseURL != "")
				}
				matches, err := store.Matches(ctx, orgID.String(), provider.name)
				if err != nil || matches {
					t.Fatalf("Matches: %v err=%v, want false and no error", matches, err)
				}
				usable, err := store.ResolveUsableProvider(ctx, orgID.String())
				if err != nil || usable != "" {
					t.Fatalf("ResolveUsableProvider = %q err=%v, want \"\" and no error", usable, err)
				}
			})
		}
	}
}

// TestCredentialsAnswerForASafeBaseURL is the control: the same rows with a base_url the guard accepts (a public IPv4
// literal) give ok, the stored key and the stored base_url, so the cases above fail because of the base_url and not
// because the fixture is incomplete.
func TestCredentialsAnswerForASafeBaseURL(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", syntheticKey)
	insertSetting(ctx, t, pool, orgID, "base_url", "https://8.8.8.8/v1")

	creds, ok, err := store.Credentials(ctx, orgID.String(), "openai")
	if err != nil || !ok {
		t.Fatalf("Credentials: ok=%v err=%v, want ok", ok, err)
	}
	if creds.APIKey != syntheticKey || creds.BaseURL != "https://8.8.8.8/v1" {
		t.Fatalf("Credentials returned a different bundle than the stored one")
	}
	if matches, err := store.Matches(ctx, orgID.String(), "openai"); err != nil || !matches {
		t.Fatalf("Matches = %v err=%v, want true", matches, err)
	}
	if usable, err := store.ResolveUsableProvider(ctx, orgID.String()); err != nil || usable != "openai" {
		t.Fatalf("ResolveUsableProvider = %q err=%v, want openai", usable, err)
	}
}
