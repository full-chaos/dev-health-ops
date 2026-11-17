package llmorgsettings

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const categorizeBaseURLWithKey = "https://llm.example.invalid/v1?token=" + llmKeySentinel

// assertSettingsPrintNoKey checks the print forms an orgSettings guards (a
// top-level print, a slice element, slog Any and Group; TestOrgSettingsRedactAPIKey
// pins that a map inside a struct field is not guarded).
func assertSettingsPrintNoKey(t *testing.T, name string, settings orgSettings) {
	t.Helper()
	outputs := map[string]string{"LogValue": settings.LogValue().String()}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		outputs[verb] = fmt.Sprintf(verb, settings)
		outputs["slice "+verb] = fmt.Sprintf(verb, []any{settings})
	}
	for label, newHandler := range map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var anyBuf, groupBuf bytes.Buffer
		slog.New(newHandler(&anyBuf)).Info("m", slog.Any("s", settings))
		slog.New(newHandler(&groupBuf)).Info("m", slog.Group("g", slog.Any("s", settings)))
		outputs["slog.Any/"+label] = anyBuf.String()
		outputs["slog.Group/"+label] = groupBuf.String()
	}
	for label, out := range outputs {
		if strings.Contains(out, llmKeySentinel) {
			t.Errorf("%s: %s output leaks the key: %s", name, label, out)
		}
		if !strings.Contains(out, "REDACTED") {
			t.Errorf("%s: %s output has no redaction marker: %s", name, label, out)
		}
	}
}

// A categorize base_url that carries the loaded categorize key prints redacted
// from every store's loaded map and projection.
func TestCategorizeBaseURLCarryingLoadedKeyIsRedacted(t *testing.T) {
	rows := []skipRow{
		{keyProvider, "openai", false, skipAt},
		{keyAPIKey, "sk-default-other", false, skipAt},
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyBaseURL), categorizeBaseURLWithKey, false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), llmKeySentinel, false, skipAt},
	}
	for _, purpose := range append(append([]Purpose{}, nonCategorizePurposes...), PurposeCategorize) {
		raw, _ := loadCountingDecryptFailures(t, purpose, rows)
		if raw[PurposeKey(PurposeCategorize, keyAPIKey)] != llmKeySentinel {
			t.Fatalf("purpose=%q: the plaintext categorize key is not loaded", purpose)
		}
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q loaded", purpose), raw)
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q projected", purpose), project(raw, purpose))
	}
}

// A non-categorize store skips an encrypted categorize key undecrypted, so the
// categorize base_url cannot be checked there and prints redacted.
func TestCategorizeBaseURLWithSkippedKeyIsRedacted(t *testing.T) {
	rows := []skipRow{
		{keyProvider, "openai", false, skipAt},
		{keyAPIKey, "sk-default-other", false, skipAt},
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyBaseURL), categorizeBaseURLWithKey, false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), "ciphertext-not-read", true, skipAt},
	}
	for _, purpose := range nonCategorizePurposes {
		raw, _ := loadCountingDecryptFailures(t, purpose, rows)
		if _, ok := raw[PurposeKey(PurposeCategorize, keyAPIKey)]; ok {
			t.Fatalf("purpose=%q: the encrypted categorize key is loaded", purpose)
		}
		if raw[PurposeKey(PurposeCategorize, keyBaseURL)] != categorizeBaseURLWithKey {
			t.Fatalf("purpose=%q: the categorize base_url is not loaded", purpose)
		}
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q loaded", purpose), raw)
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q projected", purpose), project(raw, purpose))
	}
}

// A base_url that carries no loaded key still prints in clear.
func TestBaseURLWithoutLoadedKeyPrintsInClear(t *testing.T) {
	const defaultURL, categorizeURL = "https://plain.example.invalid/v1", "https://cat.example.invalid/v1"
	for _, purpose := range append(append([]Purpose{}, nonCategorizePurposes...), PurposeCategorize) {
		raw, _ := loadCountingDecryptFailures(t, purpose, []skipRow{
			{keyProvider, "openai", false, skipAt},
			{keyBaseURL, defaultURL, false, skipAt},
			{keyAPIKey, "sk-default-other", false, skipAt},
		})
		if out := fmt.Sprint(raw); !strings.Contains(out, defaultURL) {
			t.Errorf("purpose=%q: the default base_url is redacted: %s", purpose, out)
		}
	}
	raw, _ := loadCountingDecryptFailures(t, PurposeCategorize, []skipRow{
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyBaseURL), categorizeURL, false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), "sk-categorize-other", false, skipAt},
	})
	for name, subject := range map[string]orgSettings{"loaded": raw, "projected": project(raw, PurposeCategorize)} {
		if out := fmt.Sprint(subject); !strings.Contains(out, categorizeURL) {
			t.Errorf("categorize store %s: the categorize base_url is redacted: %s", name, out)
		}
	}
}

// The unbound mark of a key is no key: a base_url holding its value "1" prints in clear.
func TestUnboundMarkIsNoLeakCheckKey(t *testing.T) {
	const url = "https://h1.example.invalid/v1"
	raw, _ := loadCountingDecryptFailures(t, "", []skipRow{
		{keyProvider, "openai", false, skipAt.Add(1)},
		{keyBaseURL, url, false, skipAt},
		{keyAPIKey, "sk-default-other", false, skipAt},
	})
	if raw[unboundMarkPrefix+keyAPIKey] == "" {
		t.Fatal("the default key written before its provider row is not marked unbound")
	}
	if out := fmt.Sprint(raw); !strings.Contains(out, url) {
		t.Errorf("the default base_url is redacted by the unbound mark: %s", out)
	}
}
