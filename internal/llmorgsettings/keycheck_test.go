package llmorgsettings

import (
	"context"
	"net"
	"testing"
)

const (
	storedTestKey = "sk-stored-" + "0123456789"
	freshTestKey  = "sk-fresh-" + "0123456789"
)

// spyHostResolver swaps the DNS lookup for a counter that answers a public address.
func spyHostResolver(t *testing.T) *int {
	t.Helper()
	calls := 0
	orig := hostResolver
	hostResolver = func(context.Context, string) ([]net.IP, error) {
		calls++
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}
	t.Cleanup(func() { hostResolver = orig })
	return &calls
}

func TestBaseURLCarriesKeyAtWriteTime(t *testing.T) {
	if !BaseURLCarriesKey("https://p.example.invalid/v1/sk-live-0123456789", "sk-live-0123456789") {
		t.Fatal("a base url holding the key must be refused")
	}
	if BaseURLCarriesKey("https://p.example.invalid/v1", "sk-live-0123456789") || BaseURLCarriesKey("https://p.example.invalid/v1", "") {
		t.Fatal("a clean base url must pass")
	}
}

func TestCheckBaseURLRefusesAKeyBeforeAnyResolution(t *testing.T) {
	calls := 0
	spy := func(context.Context, string) (bool, string, error) { calls++; return true, "", nil }
	for name, tc := range map[string]struct {
		url  string
		keys []string
	}{
		"new key in host":    {"https://" + freshTestKey + ".example.invalid/v1", []string{freshTestKey, storedTestKey}},
		"stored key in host": {"https://" + storedTestKey + ".example.invalid/v1", []string{"", storedTestKey}},
		"stored key in path": {"https://p.example.invalid/" + storedTestKey, []string{storedTestKey}},
	} {
		valid, reason, keyHit, err := CheckBaseURL(context.Background(), tc.url, tc.keys, spy)
		if err != nil || valid || !keyHit || calls != 0 || reason != BaseURLKeyRefusal {
			t.Fatalf("%s: valid=%v keyHit=%v err=%v reason=%q resolver calls=%d", name, valid, keyHit, err, reason, calls)
		}
	}
	valid, _, keyHit, err := CheckBaseURL(context.Background(), "https://p.example.invalid/v1", []string{storedTestKey}, spy)
	if err != nil || !valid || keyHit || calls != 1 {
		t.Fatalf("clean url: valid=%v keyHit=%v err=%v calls=%d", valid, keyHit, err, calls)
	}
}

// Every reader that takes a stored (or submitted) base_url: a key-bearing url is
// refused with ZERO lookups, a clean one reaches the lookup exactly once.
func TestEveryStoredBaseURLReaderRefusesAKeyBeforeAnyLookup(t *testing.T) {
	keyURL := "https://" + storedTestKey + ".example.invalid/v1"
	cleanURL := "https://p.example.invalid/v1"
	readers := map[string]func(baseURL string) (usable bool, err error){
		"CheckCredentials": func(baseURL string) (bool, error) {
			reason, err := CheckCredentials(context.Background(), "openai", storedTestKey, baseURL)
			return reason == ConfigActive, err
		},
		"CheckConfig": func(baseURL string) (bool, error) {
			reason, err := CheckConfig(context.Background(), "openai", storedTestKey, baseURL)
			return reason == ConfigActive, err
		},
		"ResolveUsableProvider": func(baseURL string) (bool, error) {
			provider, err := usableProviderOf(context.Background(), orgSettings{keyProvider: "openai", keyAPIKey: storedTestKey, keyBaseURL: baseURL})
			return provider != "", err
		},
		"Credentials": func(baseURL string) (bool, error) {
			_, ok, err := credentialsOf(context.Background(), orgSettings{keyProvider: "openai", keyAPIKey: storedTestKey, keyBaseURL: baseURL}, "openai")
			return ok, err
		},
	}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			calls := spyHostResolver(t)
			usable, err := read(keyURL)
			if err != nil || usable || *calls != 0 {
				t.Fatalf("key in base_url: usable=%v err=%v lookups=%d, want refused with 0 lookups", usable, err, *calls)
			}
			usable, err = read(cleanURL)
			if err != nil || !usable || *calls != 1 {
				t.Fatalf("clean base_url: usable=%v err=%v lookups=%d, want usable with 1 lookup", usable, err, *calls)
			}
		})
	}
}

func TestStoredBaseURLCarriesKeyReadsTheRowsWithoutALookup(t *testing.T) {
	calls := spyHostResolver(t)
	settings := orgSettings{keyProvider: "openai", keyAPIKey: storedTestKey, keyBaseURL: "https://" + storedTestKey + ".example.invalid/v1"}
	if hit := storedBaseURLCarriesKey(settings, "openai"); !hit {
		t.Fatal("a stored base_url holding the stored key must be a key hit")
	}
	settings[keyBaseURL] = "https://p.example.invalid/v1"
	if hit := storedBaseURLCarriesKey(settings, "openai"); hit {
		t.Fatal("a clean stored base_url must not be a key hit")
	}
	if hit := storedBaseURLCarriesKey(orgSettings{keyProvider: "anthropic", keyAPIKey: storedTestKey, keyBaseURL: "https://" + storedTestKey + ".x.invalid"}, "openai"); hit {
		t.Fatal("another provider's row is not this provider's base_url")
	}
	if *calls != 0 {
		t.Fatalf("the key read made %d lookups", *calls)
	}
}
