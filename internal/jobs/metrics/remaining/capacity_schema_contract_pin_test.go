package remaining

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestCapacityPhysicalContractMatchesMigrationHead pins the probe's expected
// version column and sorting key for work_item_metrics_daily against what the
// ClickHouse migration chain currently declares for that table, so a
// migration that changes either fails THIS test rather than leaving the
// probe's literals silently stale.
//
// It reads the migration SOURCE FILES rather than standing up a database:
// the probe's literals are a proxy for "what the chain currently declares",
// and this test exists to keep that proxy honest without needing docker to
// catch drift.
//
// The scan is chronological over every migration file, not a lookup of two
// hardcoded names: it recognises the shapes this repo's migrations already
// use to declare a table's version column (an RMT_VERSION_COLUMN constant in
// a Python migration that also names the table) and sorting key (a dict
// entry keyed by the table name, as a comma-joined string or a tuple of
// column names), and keeps the declaration with the HIGHEST migration
// ordinal for each. A future migration that moves either value -- including
// one that reuses today's shapes under a new file -- changes what this scan
// finds and fails the comparison below without this test naming that file.
func TestCapacityPhysicalContractMatchesMigrationHead(t *testing.T) {
	const table = "work_item_metrics_daily"
	requirement, ok := capacityTableRequirements[table]
	if !ok || !requirement.readWithFINAL {
		t.Fatalf("%s is no longer a table this probe reads with FINAL; this "+
			"pin test is stale and should be retargeted or removed", table)
	}

	files := readClickHouseMigrations(t)

	version := latestVersionColumnDeclaration(files, table)
	if version == nil {
		t.Fatalf(
			"no migration declares a ReplacingMergeTree version column for %s; "+
				"the probe requires %q with nothing in the chain to check it "+
				"against", table, requirement.versionColumn)
	}
	if version.column != requirement.versionColumn {
		t.Fatalf(
			"migration %s declares %s's version column as %q, but the probe "+
				"requires %q -- update capacityTableRequirements to match",
			version.file, table, version.column, requirement.versionColumn)
	}

	sortingKey := latestSortingKeyDeclaration(files, table)
	if sortingKey == nil {
		t.Fatalf(
			"no migration declares a sorting key for %s; the probe requires "+
				"%v with nothing in the chain to check it against",
			table, requirement.sortingKey)
	}
	if !equalStringSlices(sortingKey.columns, requirement.sortingKey) {
		t.Fatalf(
			"migration %s declares %s's sorting key as %v, but the probe "+
				"requires %v -- update capacityTableRequirements to match",
			sortingKey.file, table, sortingKey.columns, requirement.sortingKey)
	}
}

// clickHouseMigration is one migration file's ordinal, name and raw source.
type clickHouseMigration struct {
	ordinal int
	name    string
	content string
}

// readClickHouseMigrations reads every .sql/.py migration in the real chain,
// ordered the way the migration runner applies them.
func readClickHouseMigrations(t *testing.T) []clickHouseMigration {
	t.Helper()
	root := clickHouseMigrationsDir(t)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read migrations directory: %v", err)
	}

	ordinalPattern := regexp.MustCompile(`^(\d+)`)
	var migrations []clickHouseMigration
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !(strings.HasSuffix(name, ".sql") || strings.HasSuffix(name, ".py")) {
			continue
		}
		match := ordinalPattern.FindString(name)
		if match == "" {
			continue
		}
		ordinal, err := strconv.Atoi(match)
		if err != nil {
			continue
		}
		content, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, clickHouseMigration{
			ordinal: ordinal, name: name, content: string(content),
		})
	}
	if len(migrations) == 0 {
		t.Fatalf("no migrations found under %s", root)
	}
	sort.Slice(migrations, func(i, j int) bool {
		if migrations[i].ordinal != migrations[j].ordinal {
			return migrations[i].ordinal < migrations[j].ordinal
		}
		return migrations[i].name < migrations[j].name
	})
	return migrations
}

// clickHouseMigrationsDir walks up from THIS test file, matching how
// internal/testsupport/chschema locates the same chain, so a pin test and the
// integration tests that apply the chain for real cannot disagree about which
// directory either one reads.
func clickHouseMigrationsDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	directory := filepath.Dir(file)
	for {
		migrations := filepath.Join(directory, "src", "dev_health_ops", "migrations", "clickhouse")
		if info, err := os.Stat(migrations); err == nil && info.IsDir() {
			return migrations
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops/migrations/clickhouse found above %s", file)
		}
		directory = parent
	}
}

// migrationVersionColumnDeclaration names the file and version column of the
// latest migration to declare one for a table.
type migrationVersionColumnDeclaration struct {
	file   string
	column string
}

// migrationSortingKeyDeclaration names the file and sorting-key columns, in
// order, of the latest migration to declare one for a table.
type migrationSortingKeyDeclaration struct {
	file    string
	columns []string
}

// migrationRMTVersionColumnPattern matches the RMT_VERSION_COLUMN constant
// the repo's shadow-table-rebuild migrations (027/055/087/088/096) all
// define, naming the column every table that migration converts is versioned
// by.
var migrationRMTVersionColumnPattern = regexp.MustCompile(`RMT_VERSION_COLUMN\s*=\s*"([^"]+)"`)

// latestVersionColumnDeclaration scans every migration in order and keeps the
// last one that declares a version column for table, so a later migration's
// declaration always wins over an earlier one.
func latestVersionColumnDeclaration(
	migrations []clickHouseMigration, table string,
) *migrationVersionColumnDeclaration {
	quotedTable := `"` + table + `"`
	statementEngine := regexp.MustCompile(
		`(?is)` + regexp.QuoteMeta(table) + `.*?ReplacingMergeTree\(\s*` + "`?" + `([^` + "`" + `\s)]+)` + "`?" + `\s*\)`)

	var latest *migrationVersionColumnDeclaration
	for _, migration := range migrations {
		// The Python shadow-table-rebuild shape: a module-level
		// RMT_VERSION_COLUMN constant applies to every table the same file
		// also names literally, e.g. inside a TABLES tuple or a
		// TARGET_SORT_KEYS dict key.
		if strings.Contains(migration.content, quotedTable) {
			if match := migrationRMTVersionColumnPattern.FindStringSubmatch(migration.content); match != nil {
				latest = &migrationVersionColumnDeclaration{file: migration.name, column: match[1]}
				continue
			}
		}
		// A literal DDL shape: ENGINE = ReplacingMergeTree(<col>) naming the
		// table in the same statement. Restricted to .sql migrations: a .py
		// migration's DOCSTRING is free-form prose and can contain this same
		// text describing what the file does, which is not a declaration.
		if !strings.HasSuffix(migration.name, ".sql") {
			continue
		}
		if match := statementEngine.FindStringSubmatch(migration.content); match != nil {
			latest = &migrationVersionColumnDeclaration{file: migration.name, column: match[1]}
		}
	}
	return latest
}

// migrationQuotedNamePattern extracts one Python double-quoted identifier,
// used to read a dict's tuple-of-columns value.
var migrationQuotedNamePattern = regexp.MustCompile(`"([A-Za-z0-9_]+)"`)

// latestSortingKeyDeclaration scans every migration in order and keeps the
// last one that declares a sorting key for table.
//
// Two Python dict shapes are recognised, matching 027 (a comma-joined string
// value) and 096 (a tuple-of-columns value) -- both key the dict by the exact
// table name, which is also how a reader finds either today. A DDL ORDER BY
// clause naming the table is recognised too, for a migration that declares
// the key as literal SQL instead of through one of those dicts.
func latestSortingKeyDeclaration(
	migrations []clickHouseMigration, table string,
) *migrationSortingKeyDeclaration {
	quoted := regexp.QuoteMeta(`"` + table + `"`)
	stringValue := regexp.MustCompile(quoted + `\s*:\s*"([^"]*)"`)
	tupleValue := regexp.MustCompile(quoted + `\s*:\s*\(([^)]*)\)`)
	statementOrderBy := regexp.MustCompile(
		`(?is)` + regexp.QuoteMeta(table) + `.*?ORDER BY\s*\(([^)]*)\)`)

	var latest *migrationSortingKeyDeclaration
	for _, migration := range migrations {
		if match := stringValue.FindStringSubmatch(migration.content); match != nil {
			latest = &migrationSortingKeyDeclaration{
				file: migration.name, columns: splitSortingKeyColumns(match[1]),
			}
			continue
		}
		if match := tupleValue.FindStringSubmatch(migration.content); match != nil {
			latest = &migrationSortingKeyDeclaration{
				file: migration.name, columns: quotedColumnNames(match[1]),
			}
			continue
		}
		// A literal DDL shape: ORDER BY (<cols>) naming the table in the same
		// statement. Restricted to .sql migrations for the same reason as the
		// version-column fallback above -- a .py migration's prose can
		// contain this same text without declaring anything.
		if !strings.HasSuffix(migration.name, ".sql") {
			continue
		}
		if match := statementOrderBy.FindStringSubmatch(migration.content); match != nil {
			latest = &migrationSortingKeyDeclaration{
				file: migration.name, columns: splitSortingKeyColumns(match[1]),
			}
		}
	}
	return latest
}

// splitSortingKeyColumns parses "(org_id, provider, day)" or
// "org_id, provider, day" into its column names.
func splitSortingKeyColumns(raw string) []string {
	raw = strings.Trim(raw, "() ")
	if raw == "" {
		return nil
	}
	var columns []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	return columns
}

// quotedColumnNames parses `"org_id", "provider", "day"` into its column
// names, in order.
func quotedColumnNames(raw string) []string {
	var columns []string
	for _, match := range migrationQuotedNamePattern.FindAllStringSubmatch(raw, -1) {
		columns = append(columns, match[1])
	}
	return columns
}

// equalStringSlices reports whether two column lists agree on both members
// and order -- the sorting key is a tuple, not a set, so a reordering is a
// real difference even when the members match.
func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index, value := range left {
		if right[index] != value {
			return false
		}
	}
	return true
}
