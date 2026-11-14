package llmorgsettings

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type skipRow struct {
	key, value string
	encrypted  bool
	at         time.Time
}

type skipRows struct {
	rows []skipRow
	i    int
}

func (r *skipRows) Close()                                       {}
func (r *skipRows) Err() error                                   { return nil }
func (r *skipRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *skipRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *skipRows) Values() ([]any, error)                       { return nil, nil }
func (r *skipRows) RawValues() [][]byte                          { return nil }
func (r *skipRows) Conn() *pgx.Conn                              { return nil }
func (r *skipRows) Next() bool                                   { r.i++; return r.i <= len(r.rows) }
func (r *skipRows) Scan(dest ...any) error {
	if len(dest) != 4 {
		return fmt.Errorf("rawSettingsSQL scans %d columns, the fake answers 4", len(dest))
	}
	row := r.rows[r.i-1]
	value := row.value
	*dest[0].(*string) = row.key
	*dest[1].(**string) = &value
	*dest[2].(*bool) = row.encrypted
	*dest[3].(*time.Time) = row.at
	return nil
}

type skipQuerier []skipRow

func (q skipQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &skipRows{rows: q}, nil
}

// loadCountingDecryptFailures loads rows through a zero-value Decryptor, which
// fails every Decrypt (settings.go Store doc), and returns the settings and how
// many decrypt failures were recorded: one per Decrypt attempted.
func loadCountingDecryptFailures(t *testing.T, purpose Purpose, rows []skipRow) (orgSettings, int) {
	t.Helper()
	failures := 0
	previous := recordDecryptFailure
	recordDecryptFailure = func(context.Context, string) { failures++ }
	t.Cleanup(func() { recordDecryptFailure = previous })
	settings, err := Store{purpose: purpose}.loadRawSettingsFrom(context.Background(), skipQuerier(rows), "00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return settings, failures
}

var skipAt = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

var nonCategorizePurposes = []Purpose{"", PurposeInvestigate, PurposeExplain}

func TestSkipDecryptCategorizeKeyForNonCategorizeStores(t *testing.T) {
	categorizeKey := PurposeKey(PurposeCategorize, keyAPIKey)
	for _, purpose := range nonCategorizePurposes {
		settings, failures := loadCountingDecryptFailures(t, purpose, []skipRow{
			{keyProvider, "openai", false, skipAt},
			{categorizeKey, "ciphertext-not-read", true, skipAt},
		})
		if failures != 0 {
			t.Errorf("purpose=%q: %d decrypt attempts of the categorize api_key row, want 0", purpose, failures)
		}
		if _, ok := settings[categorizeKey]; ok {
			t.Errorf("purpose=%q: the categorize api_key is loaded", purpose)
		}
		if settings[keyProvider] != "openai" {
			t.Errorf("purpose=%q: the plaintext provider row was not loaded", purpose)
		}
	}
}

func TestSkipDecryptKeepsEveryOtherEncryptedRowDecrypting(t *testing.T) {
	keys := []string{keyAPIKey, PurposeKey(PurposeInvestigate, keyAPIKey), PurposeKey(PurposeExplain, keyAPIKey)}
	for _, purpose := range nonCategorizePurposes {
		for _, key := range keys {
			_, failures := loadCountingDecryptFailures(t, purpose, []skipRow{{key, "ciphertext-not-read", true, skipAt}})
			if failures != 1 {
				t.Errorf("purpose=%q key=%s: %d decrypt attempts, want 1", purpose, key, failures)
			}
		}
	}
}

func TestSkipDecryptCategorizeStoreStillDecryptsAndRecordsFailure(t *testing.T) {
	_, failures := loadCountingDecryptFailures(t, PurposeCategorize, []skipRow{
		{PurposeKey(PurposeCategorize, keyAPIKey), "ciphertext-not-read", true, skipAt},
	})
	if failures != 1 {
		t.Errorf("categorize store: %d decrypt failures recorded for an undecryptable categorize api_key, want 1", failures)
	}
}

func TestSkipDecryptTouchesOnlyEncryptedRows(t *testing.T) {
	categorizeModel := PurposeKey(PurposeCategorize, keyModel)
	categorizeKey := PurposeKey(PurposeCategorize, keyAPIKey)
	for _, purpose := range nonCategorizePurposes {
		settings, failures := loadCountingDecryptFailures(t, purpose, []skipRow{
			{categorizeModel, "clef", false, skipAt},
			{categorizeKey, "plain-value", false, skipAt},
		})
		if failures != 0 {
			t.Errorf("purpose=%q: %d decrypt failures for plaintext rows", purpose, failures)
		}
		if settings[categorizeModel] != "clef" || settings[categorizeKey] != "plain-value" {
			t.Errorf("purpose=%q: plaintext categorize rows were dropped: %v", purpose, settings.redacted())
		}
	}
}

func TestSkipDecryptKeepsTheUnboundMarkOfASkippedKey(t *testing.T) {
	categorizeKey := PurposeKey(PurposeCategorize, keyAPIKey)
	for _, purpose := range []Purpose{"", PurposeInvestigate, PurposeExplain, PurposeCategorize} {
		settings, _ := loadCountingDecryptFailures(t, purpose, []skipRow{
			{PurposeKey(PurposeCategorize, keyProvider), "anthropic", false, skipAt.Add(time.Hour)},
			{categorizeKey, "ciphertext-not-read", true, skipAt},
		})
		if settings[unboundMarkPrefix+categorizeKey] == "" {
			t.Errorf("purpose=%q: the categorize api_key written before its provider row is not marked unbound", purpose)
		}
	}
}

// The base_url leak-check set is the default, investigate and explain api_keys
// for every store purpose; a categorize api_key never enters it.
func TestLeakCheckSetPerStorePurposeNeverHoldsTheCategorizeKey(t *testing.T) {
	const defaultKey, investigateKey, explainKey, categorizeKey = "sk-default", "sk-investigate", "sk-explain", "sk-categorize"
	want := []string{defaultKey, explainKey, investigateKey}
	for _, purpose := range []Purpose{"", PurposeInvestigate, PurposeExplain, PurposeCategorize} {
		settings, _ := loadCountingDecryptFailures(t, purpose, []skipRow{
			{keyAPIKey, defaultKey, false, skipAt},
			{PurposeKey(PurposeInvestigate, keyAPIKey), investigateKey, false, skipAt},
			{PurposeKey(PurposeExplain, keyAPIKey), explainKey, false, skipAt},
			{PurposeKey(PurposeCategorize, keyAPIKey), categorizeKey, false, skipAt},
		})
		got := keysForLeakChecks(settings)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("purpose=%q: leak-check set = %v, want %v", purpose, got, want)
		}
	}
}
