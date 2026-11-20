package llmorgsettings

import (
	"bytes"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
)

const baseURLWithKey = "https://llm.example.invalid/v1?token=" + llmKeySentinel

// settingsPrints are the print forms an orgSettings guards (a top-level print, a
// slice element, slog Any and Group, LogValue; TestOrgSettingsRedactAPIKey pins
// that a map inside a struct field is not guarded).
func settingsPrints(settings orgSettings) map[string]string {
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
	return outputs
}

func assertSettingsPrintNoKey(t *testing.T, name string, settings orgSettings) {
	t.Helper()
	for label, out := range settingsPrints(settings) {
		if strings.Contains(out, llmKeySentinel) {
			t.Errorf("%s: %s output leaks the key: %s", name, label, out)
		}
		if !strings.Contains(out, "REDACTED") {
			t.Errorf("%s: %s output has no redaction marker: %s", name, label, out)
		}
	}
}

var everyStore = append(append([]Purpose{}, nonCategorizePurposes...), PurposeCategorize)

// A categorize base_url that carries the loaded categorize key.
func TestCategorizeBaseURLCarryingLoadedKeyIsRedacted(t *testing.T) {
	rows := []skipRow{
		{keyProvider, "openai", false, skipAt},
		{keyAPIKey, "sk-default-other", false, skipAt},
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyBaseURL), baseURLWithKey, false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), llmKeySentinel, false, skipAt},
	}
	for _, purpose := range everyStore {
		raw, _ := loadCountingDecryptFailures(t, purpose, rows)
		if raw[PurposeKey(PurposeCategorize, keyAPIKey)] != llmKeySentinel {
			t.Fatalf("purpose=%q: the plaintext categorize key is not loaded", purpose)
		}
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q loaded", purpose), raw)
		assertSettingsPrintNoKey(t, fmt.Sprintf("purpose=%q projected", purpose), project(raw, purpose))
	}
}

// A non-categorize store skips an encrypted categorize key undecrypted; every
// url row that may carry it (categorize, default, investigate, explain) still
// prints redacted.
func TestBaseURLCarryingSkippedCategorizeKeyIsRedacted(t *testing.T) {
	for _, urlKey := range []string{
		PurposeKey(PurposeCategorize, keyBaseURL), keyBaseURL,
		PurposeKey(PurposeInvestigate, keyBaseURL), PurposeKey(PurposeExplain, keyBaseURL),
	} {
		rows := []skipRow{
			{keyProvider, "openai", false, skipAt},
			{keyAPIKey, "sk-default-other", false, skipAt},
			{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
			{PurposeKey(PurposeCategorize, keyAPIKey), "ciphertext-not-read", true, skipAt},
			{urlKey, baseURLWithKey, false, skipAt},
		}
		for _, purpose := range nonCategorizePurposes {
			raw, _ := loadCountingDecryptFailures(t, purpose, rows)
			if _, ok := raw[PurposeKey(PurposeCategorize, keyAPIKey)]; ok {
				t.Fatalf("purpose=%q: the encrypted categorize key is loaded", purpose)
			}
			if raw[urlKey] != baseURLWithKey {
				t.Fatalf("purpose=%q: %s is not loaded", purpose, urlKey)
			}
			name := fmt.Sprintf("purpose=%q url=%s", purpose, urlKey)
			assertSettingsPrintNoKey(t, name+" loaded", raw)
			assertSettingsPrintNoKey(t, name+" projected", project(raw, purpose))
		}
	}
}

// A projection keeps an override's base_url but drops the key it carries
// (another purpose's key, or its own unbound key).
func TestProjectedBaseURLWhoseKeyWasDroppedIsRedacted(t *testing.T) {
	investigateURL := PurposeKey(PurposeInvestigate, keyBaseURL)
	cases := map[string][]skipRow{
		"categorize key in the investigate url": {
			{PurposeKey(PurposeInvestigate, keyProvider), "openai", false, skipAt},
			{investigateURL, baseURLWithKey, false, skipAt},
			{PurposeKey(PurposeInvestigate, keyAPIKey), "sk-investigate-other", false, skipAt},
			{PurposeKey(PurposeCategorize, keyAPIKey), llmKeySentinel, false, skipAt},
		},
		"unbound own key in the investigate url": {
			{PurposeKey(PurposeInvestigate, keyProvider), "openai", false, skipAt.Add(1)},
			{investigateURL, baseURLWithKey, false, skipAt},
			{PurposeKey(PurposeInvestigate, keyAPIKey), llmKeySentinel, false, skipAt},
		},
	}
	for name, rows := range cases {
		raw, _ := loadCountingDecryptFailures(t, PurposeInvestigate, rows)
		projected := project(raw, PurposeInvestigate)
		if projected[keyBaseURL] != baseURLWithKey {
			t.Fatalf("%s: the projection did not keep the url: %v", name, projected.redacted())
		}
		if projected[keyAPIKey] == llmKeySentinel {
			t.Fatalf("%s: the projection kept the key, so the case does not drop it", name)
		}
		assertSettingsPrintNoKey(t, name+" projected", projected)
	}
}

// By design every base_url row prints redacted, a clean one included (a print
// cannot tell it from a url carrying a key the map does not hold); the other
// rows print in clear.
func TestEveryBaseURLRowPrintsRedactedByDesignOtherRowsInClear(t *testing.T) {
	const cleanURL = "https://clean.example.invalid/v1"
	rows := []skipRow{{keyProvider, "openai", false, skipAt}, {keyModel, "gpt-x", false, skipAt}, {keyBaseURL, cleanURL, false, skipAt}}
	for _, purpose := range Purposes {
		rows = append(rows, skipRow{PurposeKey(purpose, keyBaseURL), cleanURL, false, skipAt})
	}
	rows = append(rows, skipRow{PurposeKey(PurposeCategorize, keyBaseURL), cleanURL, false, skipAt})
	for _, purpose := range everyStore {
		raw, _ := loadCountingDecryptFailures(t, purpose, rows)
		for label, out := range settingsPrints(raw) {
			if strings.Contains(out, cleanURL) {
				t.Errorf("purpose=%q %s: a base_url prints in clear: %s", purpose, label, out)
			}
			if !strings.Contains(out, "openai") || !strings.Contains(out, "gpt-x") {
				t.Errorf("purpose=%q %s: provider or model does not print in clear: %s", purpose, label, out)
			}
		}
	}
}

// Printing changes no served answer: the projection, the key-in-base_url
// checks and the served url and key are byte-equal before and after every
// print form, and equal to the stored values.
func TestServedAnswersUnchangedByPrintRedaction(t *testing.T) {
	rows := []skipRow{
		{keyProvider, "openai", false, skipAt},
		{keyBaseURL, baseURLWithKey, false, skipAt},
		{keyAPIKey, llmKeySentinel, false, skipAt},
		{PurposeKey(PurposeCategorize, keyProvider), "openai", false, skipAt},
		{PurposeKey(PurposeCategorize, keyBaseURL), "https://cat.example.invalid/v1", false, skipAt},
		{PurposeKey(PurposeCategorize, keyAPIKey), "sk-categorize-other", false, skipAt},
	}
	type served struct {
		projected             map[string]string
		storedKey, carriesKey bool
	}
	answer := func(raw orgSettings, purpose Purpose) served {
		projected := project(raw, purpose)
		return served{
			projected:  map[string]string(projected),
			storedKey:  Store{}.storedKeyInBaseURL(raw, projected[keyBaseURL]),
			carriesKey: storedBaseURLCarriesKey(projected, "openai"),
		}
	}
	for _, purpose := range everyStore {
		raw, _ := loadCountingDecryptFailures(t, purpose, rows)
		before := answer(raw, purpose)
		settingsPrints(raw)
		settingsPrints(project(raw, purpose))
		after := answer(raw, purpose)
		if !reflect.DeepEqual(before, after) {
			t.Errorf("purpose=%q: a print changed a served answer: %+v -> %+v", purpose, before, after)
		}
		wantURL := baseURLWithKey
		if purpose == PurposeCategorize {
			wantURL = "https://cat.example.invalid/v1"
		}
		if after.projected[keyBaseURL] != wantURL {
			t.Errorf("purpose=%q: served base_url = %q, want %q", purpose, after.projected[keyBaseURL], wantURL)
		}
		if purpose != PurposeCategorize && (!after.storedKey || !after.carriesKey || after.projected[keyAPIKey] != llmKeySentinel) {
			t.Errorf("purpose=%q: the served default key or its key-in-base_url checks moved: %+v", purpose, after)
		}
	}
}
