package llmorgsettings

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Keys under eight bytes still get their hex and base64 forms checked, but only
// forms long enough not to match ordinary url text; a key of eight bytes or more
// keeps exactly the forms it had.
func TestShortKeysAreCheckedInTheirEncodedForms(t *testing.T) {
	const tiny = "tiny"
	if !BaseURLCarriesKey("https://p.example.invalid/"+hex.EncodeToString([]byte(tiny)), tiny) {
		t.Fatal("the hex form of a four-byte key must be a key hit")
	}
	if !BaseURLCarriesKey("https://p.example.invalid/tiny", tiny) {
		t.Fatal("the raw form of a four-byte key must be a key hit")
	}
	if BaseURLCarriesKey("https://p.example.invalid/6162", "ab") {
		t.Fatal("a two-byte key's four-character hex form is ordinary url text, not a key hit")
	}
	if !BaseURLCarriesKey("https://p.example.invalid/ab", "ab") {
		t.Fatal("a two-byte key is still checked raw")
	}
	if got := len(KeyLeakForms(storedTestKey)); got != 10 {
		t.Fatalf("a long key has %d forms, want the 10 it always had", got)
	}
}

// A purpose override on another provider drops the default row's key from the
// projected settings; a base_url that holds that key is still refused, by every
// store reader, with zero lookups.
func TestStoreReadersRefuseABaseURLCarryingAKeyTheProjectionDropped(t *testing.T) {
	const overrideKey = "sk-override-" + "0123456789"
	stored := orgSettings{
		keyProvider: "anthropic", keyModel: "big-model", keyAPIKey: storedTestKey,
		PurposeKey(PurposeInvestigate, keyProvider): "openai",
		PurposeKey(PurposeInvestigate, keyModel):    "mid-model",
		PurposeKey(PurposeInvestigate, keyAPIKey):   overrideKey,
	}
	withURL := func(baseURL string) orgSettings {
		in := orgSettings{}
		for key, value := range stored {
			in[key] = value
		}
		in[PurposeKey(PurposeInvestigate, keyBaseURL)] = baseURL
		return markKeyInBaseURL(in, project(in, PurposeInvestigate))
	}
	keyed := withURL("https://p.example.invalid/" + storedTestKey)
	clean := withURL("https://p.example.invalid/v1")
	if keyed[keyAPIKey] != overrideKey {
		t.Fatalf("the projection must serve the override's own key, got a different one")
	}

	calls := spyHostResolver(t)
	if provider, err := usableProviderOf(context.Background(), keyed); err != nil || provider != "" || *calls != 0 {
		t.Fatalf("ResolveUsableProvider: provider=%q err=%v lookups=%d, want refused with 0 lookups", provider, err, *calls)
	}
	if _, ok, err := credentialsOf(context.Background(), keyed, "openai"); err != nil || ok || *calls != 0 {
		t.Fatalf("Credentials: ok=%v err=%v lookups=%d, want refused with 0 lookups", ok, err, *calls)
	}
	if !storedBaseURLCarriesKey(keyed, "openai") {
		t.Fatal("the fallback-log key read must see the dropped key")
	}
	if provider, err := usableProviderOf(context.Background(), clean); err != nil || provider != "openai" || *calls != 1 {
		t.Fatalf("clean ResolveUsableProvider: provider=%q err=%v lookups=%d", provider, err, *calls)
	}
	if _, ok, err := credentialsOf(context.Background(), clean, "openai"); err != nil || !ok || *calls != 2 {
		t.Fatalf("clean Credentials: ok=%v err=%v lookups=%d", ok, err, *calls)
	}
	if storedBaseURLCarriesKey(clean, "openai") {
		t.Fatal("a clean url is not a key hit")
	}
}

// A base_url that carries a stored key never prints: not in %v, not in slog, and
// the reserved mark entry never shows as a log key. Purpose rows hide their key
// and their key-bearing url the same way.
func TestOrgSettingsPrintNoKeyBearingBaseURLAndNoMark(t *testing.T) {
	const leakedURL = "https://canary-host.example.invalid/" + llmKeySentinel
	const overrideKey = "override-key-8716-not-a-key"
	projected := orgSettings{keyProvider: "openai", keyAPIKey: overrideKey, keyBaseURL: "https://canary-dropped.example.invalid/" + llmKeySentinel}
	projected[keyInBaseURLMark] = "1"
	stored := orgSettings{
		keyProvider: "anthropic", keyAPIKey: llmKeySentinel, keyBaseURL: leakedURL,
		PurposeKey(PurposeExplain, keyAPIKey):  overrideKey,
		PurposeKey(PurposeExplain, keyBaseURL): "https://canary-purpose.example.invalid/" + overrideKey,
		PurposeKey(PurposeExplain, keyModel):   "small-explain",
	}
	clean := orgSettings{keyProvider: "openai", keyBaseURL: "https://p.example.invalid/v1"}
	for name, subject := range map[string]orgSettings{"projected and marked": projected, "stored with purpose rows": stored} {
		var jsonLog, textLog bytes.Buffer
		slog.New(slog.NewJSONHandler(&jsonLog, nil)).Info("m", slog.Any("s", subject))
		slog.New(slog.NewTextHandler(&textLog, nil)).Info("m", slog.Any("s", subject))
		outputs := []string{fmt.Sprintf("%v", subject), fmt.Sprintf("%+v", subject), fmt.Sprintf("%#v", subject), fmt.Sprintf("%q", []any{subject}), jsonLog.String(), textLog.String()}
		for _, out := range outputs {
			for _, leak := range []string{llmKeySentinel, overrideKey, "canary-host", "canary-dropped", "canary-purpose", "key_in_base_url", "\x00", `\u0000`} {
				if strings.Contains(out, leak) {
					t.Fatalf("%s: output carries %q: %s", name, leak, out)
				}
			}
			if !strings.Contains(out, "REDACTED") {
				t.Fatalf("%s: no redaction marker: %s", name, out)
			}
		}
	}
	if out := fmt.Sprint(clean); !strings.Contains(out, "p.example.invalid/v1") || strings.Contains(out, "REDACTED") {
		t.Fatalf("a clean url must still print: %s", out)
	}
	if projected[keyInBaseURLMark] != "1" || stored[keyBaseURL] != leakedURL {
		t.Fatal("redaction changed the stored values")
	}
}

// The model gate reads the OrgConfig flag: a url that carries another stored key
// is an invalid base_url even when it holds not the served key.
func TestGateRefusesAnOrgConfigWhoseBaseURLCarriesAnotherStoredKey(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	cfg := OrgConfig{Provider: "openai", Model: "m", APIKey: secrets.NewHidden("sk-served-0123456789"), BaseURL: "https://p.example.invalid/v1", KeyInBaseURL: true}
	inputs, err := PgSource{Licensing: both, Queryer: saved(true), Config: fakeConfig{cfg: cfg, found: true}}.Inputs(context.Background(), "org")
	if err != nil || inputs.ConfigUsable || inputs.ConfigReason != ConfigInvalidBaseURL {
		t.Fatalf("usable=%v reason=%q err=%v, want unusable with invalid_base_url", inputs.ConfigUsable, inputs.ConfigReason, err)
	}
	cfg.KeyInBaseURL = false
	inputs, err = PgSource{Licensing: both, Queryer: saved(true), Config: fakeConfig{cfg: cfg, found: true}}.Inputs(context.Background(), "org")
	if err != nil || !inputs.ConfigUsable {
		t.Fatalf("clean: usable=%v reason=%q err=%v", inputs.ConfigUsable, inputs.ConfigReason, err)
	}
}
