package llmorgsettings

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-14232: a store other than the categorize store refuses a served
// base_url that carries the org's categorize api key, without the key's
// plaintext reaching any output.

const sealedTestCategorizeKey = "sk-sealed-categorize-" + "0123456789abcdef"

func sealedKeyStore(t *testing.T, purpose Purpose) (Store, string) {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := decryptor.Encrypt([]byte(sealedTestCategorizeKey))
	if err != nil {
		t.Fatal(err)
	}
	return Store{Decryptor: decryptor, purpose: purpose}, ciphertext.Reveal()
}

func countOpens(t *testing.T) *int {
	t.Helper()
	opens := 0
	previous := openSealedKey
	openSealedKey = func(decryptor providerfoundation.FernetDecryptor, sealed string) ([]byte, error) {
		opens++
		return previous(decryptor, sealed)
	}
	t.Cleanup(func() { openSealedKey = previous })
	return &opens
}

func publicResolver(t *testing.T) *[]string {
	t.Helper()
	hosts := []string{}
	previous := hostResolver
	hostResolver = func(_ context.Context, host string) ([]net.IP, error) {
		hosts = append(hosts, host)
		return []net.IP{net.ParseIP("8.8.8.8")}, nil
	}
	t.Cleanup(func() { hostResolver = previous })
	return &hosts
}

// sealedKeyRows is an org whose default row (or the store's override) serves
// baseURL, with an encrypted categorize override key.
func sealedKeyRows(purpose Purpose, ciphertext, baseURL string) []skipRow {
	rows := []skipRow{
		{keyProvider, "openai", false, skipAt},
		{keyModel, "m", false, skipAt},
		{keyAPIKey, "sk-default-" + "0123456789", false, skipAt},
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), ciphertext, true, skipAt},
	}
	if baseURL != "" {
		rows = append(rows, skipRow{keyBaseURL, baseURL, false, skipAt})
	}
	if purpose != "" {
		rows = append(rows,
			skipRow{PurposeKey(purpose, keyProvider), "openai", false, skipAt},
			skipRow{PurposeKey(purpose, keyModel), "m-" + string(purpose), false, skipAt},
			skipRow{PurposeKey(purpose, keyAPIKey), "sk-" + string(purpose) + "-0123456789", false, skipAt})
		if baseURL != "" {
			rows = append(rows, skipRow{PurposeKey(purpose, keyBaseURL), baseURL, false, skipAt})
		}
	}
	return rows
}

// readersOf runs the loaded-settings readers of one store over rows and
// returns the projected settings and the Credentials answer.
func readersOf(t *testing.T, store Store, rows []skipRow) (raw, projected orgSettings, creds Credentials, ok bool) {
	t.Helper()
	raw, err := store.loadRawSettingsFrom(context.Background(), skipQuerier(rows), "00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	projected = store.markKeyInBaseURL(raw, project(raw, store.purpose))
	creds, ok, err = credentialsOf(context.Background(), projected, "openai")
	if err != nil {
		t.Fatal(err)
	}
	return raw, projected, creds, ok
}

// C1-C4 (unit form): the served url carries the categorize key in the query,
// the path or a host label: refused, and no host carrying it is looked up.
func TestNonCategorizeStoresRefuseAServedURLCarryingTheCategorizeKey(t *testing.T) {
	for _, purpose := range nonCategorizePurposes {
		for form, url := range map[string]string{
			"query":   "https://llm.example.com/v1?k=" + sealedTestCategorizeKey,
			"path":    "https://llm.example.com/v1/" + sealedTestCategorizeKey,
			"host":    "https://" + sealedTestCategorizeKey + ".example.com/v1",
			"escaped": "https://llm.example.com/v1/%73k-sealed-categorize-0123456789abcdef",
		} {
			hosts := publicResolver(t)
			store, ciphertext := sealedKeyStore(t, purpose)
			_, projected, _, ok := readersOf(t, store, sealedKeyRows(purpose, ciphertext, url))
			if ok || !keyInBaseURLMarked(projected) || !storedBaseURLCarriesKey(projected, "openai") {
				t.Errorf("purpose=%q %s: served=%v marked=%v", purpose, form, ok, keyInBaseURLMarked(projected))
			}
			if provider, err := usableProviderOf(context.Background(), projected); err != nil || provider != "" {
				t.Errorf("purpose=%q %s: usable provider=%q err=%v", purpose, form, provider, err)
			}
			if len(*hosts) != 0 {
				t.Errorf("purpose=%q %s: %d lookups, want 0", purpose, form, len(*hosts))
			}
		}
	}
}

// C6: an org with no served base_url never has its categorize key opened.
func TestNoServedBaseURLOpensNoCategorizeKey(t *testing.T) {
	for _, purpose := range nonCategorizePurposes {
		publicResolver(t)
		opens := countOpens(t)
		store, ciphertext := sealedKeyStore(t, purpose)
		rows := sealedKeyRows(purpose, ciphertext, "")
		rows = append(rows, skipRow{PurposeKey(PurposeCategorize, keyBaseURL), "https://cat.example.com/v1", false, skipAt})
		_, _, _, ok := readersOf(t, store, rows)
		if !ok || *opens != 0 {
			t.Errorf("purpose=%q: served=%v opens=%d, want served and 0 opens", purpose, ok, *opens)
		}
	}
}

// C7: a sealed key that cannot be opened refuses a served url (fail closed) and
// counts no decrypt failure for a store that does not serve it.
func TestUnopenableCategorizeKeyRefusesAServedURL(t *testing.T) {
	for _, purpose := range nonCategorizePurposes {
		publicResolver(t)
		failures := 0
		previous := recordDecryptFailure
		recordDecryptFailure = func(context.Context, string) { failures++ }
		t.Cleanup(func() { recordDecryptFailure = previous })
		store, _ := sealedKeyStore(t, purpose)
		_, projected, _, ok := readersOf(t, store, sealedKeyRows(purpose, "not-a-fernet-token", "https://llm.example.com/v1"))
		if ok || !keyInBaseURLMarked(projected) || failures != 0 {
			t.Errorf("purpose=%q: served=%v marked=%v decrypt failures=%d, want refused, marked, 0", purpose, ok, keyInBaseURLMarked(projected), failures)
		}
		recordDecryptFailure = previous
	}
}

// printsOf is every printed form of v: fmt verbs (alone and nested) and slog.
func printsOf(v any) map[string]string {
	out := map[string]string{}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		out[verb] = fmt.Sprintf(verb, v)
		out["slice "+verb] = fmt.Sprintf(verb, []any{v})
	}
	for label, newHandler := range map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var buf bytes.Buffer
		slog.New(newHandler(&buf)).Info("m", slog.Any("v", v))
		out["slog/"+label] = buf.String()
	}
	out["error"] = fmt.Errorf("wrapped: %v", v).Error()
	out["panic"] = func() (msg string) {
		defer func() { msg = fmt.Sprint(recover()) }()
		panic(fmt.Sprintf("%+v", v))
	}()
	return out
}

// C5: the categorize key opened for the check (clean served url, so it is opened
// and does not hit) reaches no output: no settings map value, no projection,
// no Credentials field, no printed form of the store, the maps, the
// Credentials or an error, and no recovered panic message.
func TestOpenedCategorizeKeyReachesNoOutput(t *testing.T) {
	for _, purpose := range nonCategorizePurposes {
		publicResolver(t)
		opens := countOpens(t)
		store, ciphertext := sealedKeyStore(t, purpose)
		raw, projected, creds, ok := readersOf(t, store, sealedKeyRows(purpose, ciphertext, "https://llm.example.com/v1"))
		if !ok || *opens != 1 {
			t.Fatalf("purpose=%q: served=%v opens=%d, want served and 1 open", purpose, ok, *opens)
		}
		for name, settings := range map[string]orgSettings{"raw": raw, "projected": projected} {
			for key, value := range settings {
				if strings.Contains(value, sealedTestCategorizeKey) {
					t.Errorf("purpose=%q: %s settings entry %q holds the categorize key", purpose, name, key)
				}
			}
		}
		if creds.APIKey.Reveal() == sealedTestCategorizeKey || strings.Contains(creds.BaseURL, sealedTestCategorizeKey) {
			t.Errorf("purpose=%q: Credentials carry the categorize key", purpose)
		}
		for name, v := range map[string]any{"store": store, "raw": raw, "projected": projected, "credentials": creds} {
			for label, text := range printsOf(v) {
				if strings.Contains(text, sealedTestCategorizeKey) {
					t.Errorf("purpose=%q: %s %s holds the categorize key", purpose, name, label)
				}
				if name != "store" && strings.Contains(text, ciphertext) {
					t.Errorf("purpose=%q: %s %s holds the sealed ciphertext", purpose, name, label)
				}
			}
		}
	}
}

// CheckStoredCredentials refuses a url flagged by the caller before any lookup,
// after the missing-credentials answer, as CheckOrgConfig does.
func TestCheckStoredCredentialsRefusesAFlaggedURLBeforeLookup(t *testing.T) {
	hosts := publicResolver(t)
	ctx := context.Background()
	if reason, err := CheckStoredCredentials(ctx, "openai", "sk-a-0123456789", "https://llm.example.com/v1", true); err != nil || reason != ConfigInvalidBaseURL || len(*hosts) != 0 {
		t.Fatalf("flagged: reason=%q err=%v lookups=%d", reason, err, len(*hosts))
	}
	if reason, err := CheckStoredCredentials(ctx, "openai", "", "https://llm.example.com/v1", true); err != nil || reason != ConfigMissingCredentials {
		t.Fatalf("flagged, no key: reason=%q err=%v", reason, err)
	}
	if reason, err := CheckStoredCredentials(ctx, "openai", "sk-a-0123456789", "https://llm.example.com/v1", false); err != nil || reason != ConfigActive || len(*hosts) != 1 {
		t.Fatalf("clean: reason=%q err=%v lookups=%d", reason, err, len(*hosts))
	}
}
