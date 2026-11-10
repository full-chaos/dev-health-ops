package llmorgsettings

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// bindingRow is one llm settings row as rawSettingsSQL selects it.
type bindingRow struct {
	key, value string
	at         time.Time
}

// bindingRows answers rawSettingsSQL with fixed rows (plaintext values).
type bindingRows struct {
	rows []bindingRow
	i    int
}

func (r *bindingRows) Close()                                       {}
func (r *bindingRows) Err() error                                   { return nil }
func (r *bindingRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *bindingRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *bindingRows) Values() ([]any, error)                       { return nil, nil }
func (r *bindingRows) RawValues() [][]byte                          { return nil }
func (r *bindingRows) Conn() *pgx.Conn                              { return nil }
func (r *bindingRows) Next() bool                                   { r.i++; return r.i <= len(r.rows) }
func (r *bindingRows) Scan(dest ...any) error {
	if len(dest) != 4 {
		return fmt.Errorf("rawSettingsSQL scans %d columns, the fake answers 4", len(dest))
	}
	row := r.rows[r.i-1]
	value := row.value
	*dest[0].(*string) = row.key
	*dest[1].(**string) = &value
	*dest[2].(*bool) = false
	*dest[3].(*time.Time) = row.at
	return nil
}

type bindingQuerier []bindingRow

func (q bindingQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &bindingRows{rows: q}, nil
}

func loadRows(t *testing.T, rows []bindingRow) orgSettings {
	t.Helper()
	settings, err := Store{}.loadRawSettingsFrom(context.Background(), bindingQuerier(rows), "00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return settings
}

const (
	plantedOpenAIKey  = "sk-PLANTEDopenaiOverrideKey0123456789"
	plantedDefaultKey = "sk-PLANTEDdefaultRowKey0123456789"
)

var (
	keyWritten      = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	providerChanged = keyWritten.Add(time.Hour)
)

// The q6 cell of CHAOS-13093 (red on main 46530638, lane-scratch/cag-13093/logs/
// q6-main-13093.log sha256 3461dce9...) with the one fact its map lacks: the
// rows' updated_at, the provider switched to anthropic after the openai key was
// written. The effective config serves no key, and the config check does not
// pass it.
func TestQ6HeadAProviderSwitchWithNoNewKeyServesNoKey(t *testing.T) {
	for _, purpose := range []Purpose{PurposeExplain, PurposeInvestigate, PurposeCategorize} {
		settings := loadRows(t, []bindingRow{
			{"provider", "openai", keyWritten}, {"model", "gpt-5", keyWritten}, {"api_key", plantedDefaultKey, keyWritten},
			{PurposeKey(purpose, "provider"), "anthropic", providerChanged},
			{PurposeKey(purpose, "model"), "claude-haiku-5-5", providerChanged},
			{PurposeKey(purpose, "api_key"), plantedOpenAIKey, keyWritten},
		})
		effective := project(settings, purpose)
		reason, err := CheckOrgConfig(context.Background(), OrgConfig{Purpose: purpose, Provider: effective["provider"],
			Model: effective["model"], APIKey: secrets.NewHidden(effective["api_key"])})
		t.Logf("%s: effective provider=%s api_key_is_openai_override_key=%v api_key_is_default_key=%v CheckOrgConfig=%q err=%v",
			purpose, effective["provider"], effective["api_key"] == plantedOpenAIKey, effective["api_key"] == plantedDefaultKey, reason, err)
		if effective["api_key"] != "" || reason == ConfigActive {
			t.Errorf("%s: the anthropic config is served with a key written for openai", purpose)
		}
	}
}

// The same switch on the default row: a default provider changed after its key
// was written serves no key, to the default reader and to an override that
// inherits it.
func TestADefaultProviderSwitchWithNoNewKeyServesNoKey(t *testing.T) {
	settings := loadRows(t, []bindingRow{
		{"provider", "anthropic", providerChanged}, {"model", "claude-haiku-5-5", providerChanged},
		{"api_key", plantedOpenAIKey, keyWritten},
		{PurposeKey(PurposeExplain, "provider"), "anthropic", providerChanged},
		{PurposeKey(PurposeExplain, "model"), "claude-haiku-5-5", providerChanged},
	})
	for _, purpose := range []Purpose{"", PurposeExplain, PurposeInvestigate} {
		effective := project(settings, purpose)
		t.Logf("purpose=%q: effective provider=%s api_key_is_openai_key=%v", purpose, effective["provider"], effective["api_key"] == plantedOpenAIKey)
		if effective["api_key"] != "" {
			t.Errorf("purpose=%q: the default key written for openai is served with anthropic", purpose)
		}
	}
	if settings["api_key"] != plantedOpenAIKey {
		t.Error("the unbound key left the loaded rows: the base_url key-leak checks no longer see it")
	}
}

// The reader table: scope x (key row older | same instant | newer than its
// provider row). Only an older key is withheld.
func TestTheReaderServesAKeyOnlyWhenItIsNotOlderThanItsProvider(t *testing.T) {
	for _, scope := range KeyScopes() {
		for _, c := range []struct {
			name   string
			keyAt  time.Time
			served bool
		}{
			{"key older", keyWritten, false},
			{"same instant", providerChanged, true},
			{"key newer", providerChanged.Add(time.Millisecond), true},
		} {
			rows := []bindingRow{{scope.ProviderKey, "anthropic", providerChanged}, {scope.APIKeyKey, plantedOpenAIKey, c.keyAt}}
			if scope.ProviderKey != keyProvider {
				rows = append(rows, bindingRow{keyProvider, "openai", keyWritten}, bindingRow{keyModel, "gpt-5", keyWritten})
			}
			settings := loadRows(t, rows)
			purpose := Purpose("")
			for _, p := range append(append([]Purpose{}, Purposes...), PurposeCategorize) {
				if PurposeKey(p, keyProvider) == scope.ProviderKey {
					purpose = p
				}
			}
			got := project(settings, purpose)[keyAPIKey] == plantedOpenAIKey
			if got != c.served {
				t.Errorf("%s / %s: served=%v, want %v", scope.APIKeyKey, c.name, got, c.served)
			}
		}
	}
}

// An override with no key of its own inherits the default key only while that
// key is bound to the default provider.
func TestAnOverrideInheritsOnlyABoundDefaultKey(t *testing.T) {
	for _, c := range []struct {
		name   string
		keyAt  time.Time
		served bool
	}{{"bound", providerChanged, true}, {"unbound", keyWritten, false}} {
		settings := loadRows(t, []bindingRow{
			{"provider", "anthropic", providerChanged}, {"api_key", plantedDefaultKey, c.keyAt},
			{PurposeKey(PurposeInvestigate, "provider"), "anthropic", providerChanged},
			{PurposeKey(PurposeInvestigate, "model"), "claude-haiku-5-5", providerChanged},
		})
		if got := project(settings, PurposeInvestigate)[keyAPIKey] == plantedDefaultKey; got != c.served {
			t.Errorf("%s: inherited=%v, want %v", c.name, got, c.served)
		}
	}
}

// The Q3 cell of CHAOS-13096 (red on main 2ed1bd5d, lane-scratch/cag-13096/
// logs/q3-main-13096.log sha256 6760052c...) in head form: the same provider
// with a NEW base_url and no new key. The default key is older than the new
// base_url row, so it is not served; an override on its own base_url does not
// inherit the default key.
func TestQ3HeadSameProviderNewBaseURLNoKeyServesNoKey(t *testing.T) {
	const storedKey = "sk-PLANTEDstoredOpenAIKey0123456789"
	const newHost = "https://8.8.8.8/v1"
	for _, purpose := range []Purpose{"", PurposeExplain} {
		rows := []bindingRow{{"provider", "openai", keyWritten}, {"model", "gpt-5", keyWritten}, {"api_key", storedKey, keyWritten}}
		if purpose == "" {
			rows = append(rows, bindingRow{"base_url", newHost, providerChanged})
		} else {
			rows = append(rows, bindingRow{PurposeKey(purpose, "provider"), "openai", providerChanged},
				bindingRow{PurposeKey(purpose, "model"), "gpt-5", providerChanged},
				bindingRow{PurposeKey(purpose, "base_url"), newHost, providerChanged})
		}
		effective := project(loadRows(t, rows), purpose)
		reason, err := CheckOrgConfig(context.Background(), OrgConfig{Purpose: purpose, Provider: effective["provider"],
			Model: effective["model"], BaseURL: effective["base_url"], APIKey: secrets.NewHidden(effective["api_key"])})
		t.Logf("purpose=%q: effective base_url=%s api_key_is_stored_key=%v CheckOrgConfig=%q err=%v", purpose, effective["base_url"], effective["api_key"] == storedKey, reason, err)
		if effective["api_key"] == storedKey || reason == ConfigActive {
			t.Errorf("purpose=%q: the stored key is served to the new base_url host", purpose)
		}
	}
}

// The base_url half of the reader table: scope x (key older | same instant |
// newer than its base_url row, set or cleared).
func TestTheReaderServesAKeyOnlyWhenItIsNotOlderThanItsBaseURL(t *testing.T) {
	for _, scope := range KeyScopes() {
		for _, baseURL := range []string{"https://8.8.8.8/v1", ""} {
			for _, c := range []struct {
				name   string
				keyAt  time.Time
				served bool
			}{
				{"key older", keyWritten, false},
				{"same instant", providerChanged, true},
				{"key newer", providerChanged.Add(time.Millisecond), true},
			} {
				rows := []bindingRow{{scope.ProviderKey, "openai", keyWritten}, {scope.BaseURLKey, baseURL, providerChanged}, {scope.APIKeyKey, plantedOpenAIKey, c.keyAt}}
				if scope.ProviderKey != keyProvider {
					rows = append(rows, bindingRow{keyProvider, "anthropic", keyWritten})
				}
				purpose := Purpose("")
				for _, p := range append(append([]Purpose{}, Purposes...), PurposeCategorize) {
					if ScopeOf(p) == scope {
						purpose = p
					}
				}
				got := project(loadRows(t, rows), purpose)[keyAPIKey] == plantedOpenAIKey
				if got != c.served {
					t.Errorf("%s / base_url %q / %s: served=%v, want %v", scope.APIKeyKey, baseURL, c.name, got, c.served)
				}
			}
		}
	}
}

// An override on the default provider but another base_url never inherits the
// default key, whatever the timestamps.
func TestAnOverrideOnAnotherBaseURLDoesNotInherit(t *testing.T) {
	settings := orgSettings{"provider": "openai", "api_key": plantedDefaultKey,
		PurposeKey(PurposeExplain, "provider"): "openai", PurposeKey(PurposeExplain, "model"): "gpt-5",
		PurposeKey(PurposeExplain, "base_url"): "https://8.8.8.8/v1"}
	if got := project(settings, PurposeExplain)["api_key"]; got != "" {
		t.Fatal("an override on another base_url inherits the default key")
	}
}
