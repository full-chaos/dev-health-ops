package llmorgsettings

import (
	"context"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

var (
	categorizeSettingsFunction = regexp.MustCompile(`(?s)CREATE\s+FUNCTION\s+public\.categorize_byo_settings\s*\(.*?\$\$(.*?)\$\$`)
	pinnedKeyArray             = regexp.MustCompile(`(?s)OPERATOR\(pg_catalog\.=\)\s+ANY\s*\(\s*ARRAY\[(.*?)\]::pg_catalog\.text\[\]\)`)
	quotedKey                  = regexp.MustCompile(`'([^']*)'`)
)

// migrationPinnedKeys is the llm key list of the chain file that creates
// public.categorize_byo_settings, found by its content (not its number).
func migrationPinnedKeys(t *testing.T) []string {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for _, file := range chain {
		for _, match := range categorizeSettingsFunction.FindAllStringSubmatch(file.SQL, -1) {
			bodies = append(bodies, match[1])
		}
	}
	if len(bodies) != 1 {
		t.Fatalf("%d chain files create public.categorize_byo_settings, want exactly 1", len(bodies))
	}
	arrays := pinnedKeyArray.FindAllStringSubmatch(bodies[0], -1)
	if len(arrays) != 1 {
		t.Fatalf("the function body holds %d pinned key arrays, want exactly 1", len(arrays))
	}
	var keys []string
	for _, match := range quotedKey.FindAllStringSubmatch(arrays[0][1], -1) {
		keys = append(keys, match[1])
	}
	return keys
}

// The definer function and the Go readers name the same llm keys: a key the
// readers need and the function does not return reads as "not stored" on the
// worker, and a key the function returns that no reader needs widens what the
// domain role can read for nothing.
func TestTheCategorizeSettingsFunctionReturnsExactlyTheKeysTheReadersRead(t *testing.T) {
	sqlKeys := migrationPinnedKeys(t)
	goKeys := CategorizeBYODefinerKeys()
	var missingInSQL, extraInSQL []string
	for _, key := range goKeys {
		if !slices.Contains(sqlKeys, key) {
			missingInSQL = append(missingInSQL, key)
		}
	}
	for _, key := range sqlKeys {
		if !slices.Contains(goKeys, key) {
			extraInSQL = append(extraInSQL, key)
		}
	}
	if len(missingInSQL) > 0 {
		t.Errorf("keys the Go readers read that the function does not return: %v", missingInSQL)
	}
	if len(extraInSQL) > 0 {
		t.Errorf("keys the function returns that no Go reader reads: %v", extraInSQL)
	}
	if len(sqlKeys) != len(goKeys) {
		t.Errorf("the function lists %d keys (duplicates?), the Go set %d", len(sqlKeys), len(goKeys))
	}
}

// The Go key set is the one the readers use; it is built from their constants,
// so this pins what it holds and what it must never hold.
func TestTheDefinerKeySetHoldsTheBuildReadsAndNoOtherPurposeSecret(t *testing.T) {
	got := CategorizeBYODefinerKeys()
	sort.Strings(got)
	want := []string{
		"api_key", "base_url", "model", "own_model_enabled", "prompt_cache", "provider",
		"purpose:categorize:api_key", "purpose:categorize:base_url", "purpose:categorize:model", "purpose:categorize:provider",
		"purpose:explain:provider", "purpose:investigate:provider",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("CategorizeBYODefinerKeys = %v, want %v", got, want)
	}
	for _, key := range append([]string{KeyCategorizeBYOEnabled}, PurposeKeys(PurposeInvestigate)[1:]...) {
		if slices.Contains(got, key) {
			t.Errorf("the definer key set holds %q", key)
		}
	}
	for _, key := range PurposeKeys(PurposeExplain)[1:] {
		if slices.Contains(got, key) {
			t.Errorf("the definer key set holds %q", key)
		}
	}
	for _, key := range append(append(append([]string{KeyPromptCache}, byoKeys...), PurposeKeys(PurposeCategorize)...), savedProviderKeys()...) {
		if !slices.Contains(got, key) {
			t.Errorf("a key the categorize readers read is missing from the definer key set: %q", key)
		}
	}
}

// With the table relation every reader's SQL is byte-identical to what it was
// before the relation existed (the api path does not change); with the definer
// relation no reader names the settings table.
func TestTheReaderSQLFollowsTheRelation(t *testing.T) {
	table := map[string]string{
		"raw":     rawSettingsSQL(SettingsTable),
		"version": versionRowsSQL(SettingsTable),
		"saved":   savedProvidersSQL(SettingsTable),
	}
	before := map[string]string{
		"raw": `
SELECT key, value, is_encrypted FROM settings
WHERE org_id = $1 AND category = $2`,
		"version": `
SELECT key, updated_at, coalesce(value, '') FROM settings
WHERE org_id = $1 AND category = $2 AND key = ANY($3) ORDER BY key`,
		"saved": `
SELECT coalesce(json_agg(coalesce(value, '')), '[]'::json) FROM settings
WHERE org_id = $1 AND category = $2 AND key = ANY($3) AND coalesce(value, '') <> ''`,
	}
	for name, sql := range table {
		if sql != before[name] {
			t.Errorf("%s: the table-relation SQL changed:\n%s", name, sql)
		}
	}
	bareTable := regexp.MustCompile(`(?i)\bFROM\s+settings\b`)
	for name, sql := range map[string]string{
		"raw": rawSettingsSQL(CategorizeBYODefiner), "version": versionRowsSQL(CategorizeBYODefiner), "saved": savedProvidersSQL(CategorizeBYODefiner),
	} {
		if bareTable.MatchString(sql) || !strings.Contains(sql, "FROM public.categorize_byo_settings($1) AS settings") {
			t.Errorf("%s: the definer-relation SQL does not read the function only:\n%s", name, sql)
		}
	}
}

// recordingQueryer records the SQL of every statement and answers each with no row.
type recordingQueryer struct{ statements []string }

func (r *recordingQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.statements = append(r.statements, sql)
	return savedRow{}
}

// PgSource on the definer relation sends its own saved-provider reads through
// the function, never the table (the config reads are the Store's, above).
func TestPgSourceOnTheDefinerRelationReadsNoSettingsTable(t *testing.T) {
	plan := licensing.Decision{Allowed: false, Reason: licensing.ReasonTierRequired}
	queryer := &recordingQueryer{}
	source := PgSource{
		Licensing: fakeLicensing{ACRServiceModelFeatureKey: plan, PlatformModelFallbackFeatureKey: plan},
		Queryer:   queryer, Config: fakeConfig{}, Relation: CategorizeBYODefiner,
	}
	if _, err := source.Inputs(context.Background(), "org"); err != nil {
		t.Fatal(err)
	}
	if len(queryer.statements) == 0 {
		t.Fatal("PgSource.Inputs sent no saved-provider read")
	}
	for _, sql := range queryer.statements {
		if !strings.Contains(sql, categorizeBYOSettingsFrom) || regexp.MustCompile(`(?i)\bFROM\s+settings\b`).MatchString(sql) {
			t.Errorf("PgSource on the definer relation sent: %s", sql)
		}
	}
}

// The readers on the categorize Build path build every settings FROM item from
// the relation: no SQL literal in their files names the table directly.
func TestTheBuildPathReaderFilesNameNoSettingsTableLiteral(t *testing.T) {
	bareTable := regexp.MustCompile("(?i)FROM\\s+settings\\b")
	for _, file := range []string{"settings.go", "orgconfig.go", "saved.go", "modelgate.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if bareTable.Match(source) {
			t.Errorf("%s names the settings table in a FROM; build it from the SettingsRelation", file)
		}
	}
}
