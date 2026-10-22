package llmorgsettings

import (
	"slices"
	"testing"
	"time"
)

// The expected digests were computed outside Go from the documented form: for
// each row, key NUL updated_at (UTC, RFC 3339 with trimmed nanoseconds) NUL
// hex sha256 of the stored value, newline; all hashed with sha256.
const (
	digestWithoutPromptCache = "a3da15d0f26fad26caecf74efbb881766b6bdf943a396d4428dda4b477f2e7da"
	digestWithPromptCache    = "fc20fa1a3bcbee128bd2e8e9db52088562a00d475ea1983327e7d9a081f6cdfd"
)

func savedConfigRows() []versionRow {
	at := func(layout string) time.Time {
		parsed, err := time.Parse(time.RFC3339Nano, layout)
		if err != nil {
			panic(err)
		}
		return parsed
	}
	return []versionRow{
		{"api_key", "enc-ciphertext", at("2026-10-01T10:00:00.123456789Z")},
		{"base_url", "https://api.anthropic.com", at("2026-10-01T10:00:00Z")},
		{"model", "claude-haiku-5-5", at("2026-10-02T11:30:15.5Z")},
		{"provider", "anthropic", at("2026-10-01T10:00:00.000000001Z")},
	}
}

func withPromptCacheRow(rows []versionRow, value string) []versionRow {
	row := versionRow{KeyPromptCache, value, time.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)}
	out := slices.Clone(rows)
	return slices.Insert(out, 3, row)
}

func TestVersionDigestIsUnchangedForAnOrgWithNoPromptCacheRow(t *testing.T) {
	got, stored := versionDigest(savedConfigRows(), Fields)
	if got != digestWithoutPromptCache {
		t.Fatalf("digest = %s, want %s", got, digestWithoutPromptCache)
	}
	if want := []string{"api_key", "base_url", "model", "provider"}; !slices.Equal(stored, want) {
		t.Fatalf("stored = %v, want %v", stored, want)
	}
}

func TestVersionDigestMovesWithThePromptCacheRow(t *testing.T) {
	on, _ := versionDigest(withPromptCacheRow(savedConfigRows(), "true"), Fields)
	if on != digestWithPromptCache {
		t.Fatalf("digest with the opt-in = %s, want %s", on, digestWithPromptCache)
	}
	off, _ := versionDigest(withPromptCacheRow(savedConfigRows(), "false"), Fields)
	if off == on || off == digestWithoutPromptCache {
		t.Fatalf("a flipped opt-in must move the digest: off=%s on=%s none=%s", off, on, digestWithoutPromptCache)
	}
}

func TestVersionDigestKeepsThePromptCacheRowOutOfTheStoredKeys(t *testing.T) {
	_, stored := versionDigest(withPromptCacheRow(savedConfigRows(), "true"), Fields)
	if slices.Contains(stored, KeyPromptCache) {
		t.Fatalf("stored = %v: the opt-in row is not a credential row", stored)
	}
}

func TestPromptCacheOptIn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settings orgSettings
		provider string
		want     bool
	}{
		{"opted in, anthropic", orgSettings{KeyPromptCache: "true"}, "anthropic", true},
		{"opted in, openai", orgSettings{KeyPromptCache: "true"}, "openai", false},
		{"opted in, no provider", orgSettings{KeyPromptCache: "true"}, "", false},
		{"no row", orgSettings{}, "anthropic", false},
		{"false", orgSettings{KeyPromptCache: "false"}, "anthropic", false},
		{"empty", orgSettings{KeyPromptCache: ""}, "anthropic", false},
		{"upper case", orgSettings{KeyPromptCache: "TRUE"}, "anthropic", false},
		{"one", orgSettings{KeyPromptCache: "1"}, "anthropic", false},
		{"padded", orgSettings{KeyPromptCache: " true"}, "anthropic", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := promptCacheOptIn(tc.settings, tc.provider); got != tc.want {
				t.Fatalf("promptCacheOptIn = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestControlRowsAloneLeaveNoSavedConfig(t *testing.T) {
	rest, turnedOff := withoutControlRows(orgSettings{KeyPromptCache: "true", KeyOwnModelEnabled: "false"})
	if len(rest) != 0 || !turnedOff {
		t.Fatalf("rest = %v turnedOff = %v, want no rows and turned off", rest, turnedOff)
	}
	rest, turnedOff = withoutControlRows(orgSettings{KeyPromptCache: "true"})
	if len(rest) != 0 || turnedOff {
		t.Fatalf("an org with only the opt-in: rest = %v turnedOff = %v, want no rows and not turned off", rest, turnedOff)
	}
	rest, _ = withoutControlRows(orgSettings{keyProvider: "anthropic", KeyPromptCache: "true"})
	if _, ok := rest[KeyPromptCache]; ok || rest[keyProvider] != "anthropic" {
		t.Fatalf("rest = %v, want only the provider", rest)
	}
}

func TestEffectiveProviderIsTheInvestigateOverrideElseTheDefault(t *testing.T) {
	for _, tc := range []struct{ name, defaultProvider, override, want string }{
		{"default only", "anthropic", "", "anthropic"},
		{"override replaces the default", "anthropic", "openai", "openai"},
		{"override on anthropic over another default", "openai", "anthropic", "anthropic"},
		{"case and padding", " Anthropic ", "", "anthropic"},
		{"nothing stored reads as auto", "", "", "auto"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveProvider(tc.defaultProvider, tc.override); got != tc.want {
				t.Fatalf("effectiveProvider(%q, %q) = %q, want %q", tc.defaultProvider, tc.override, got, tc.want)
			}
		})
	}
}
