package pgmigrate

import (
	"strings"
	"testing"
	"time"
)

func TestUnitsIndexDropBudgetIsNamedAndIgnoresLockAndRetryDelay(t *testing.T) {
	want := unitsIndexDropBudget(IndexBuildOptions{})
	if want != DefaultIndexCleanupBudget || want > 5*time.Minute {
		t.Fatalf("default budget %s, want the %s cap", want, DefaultIndexCleanupBudget)
	}
	if got := unitsIndexDropBudget(IndexBuildOptions{LockTimeout: time.Minute, RetryDelay: 20 * time.Minute}); got != want {
		t.Fatalf("budget depends on LockTimeout or RetryDelay: %s vs %s", got, want)
	}
	if got := unitsIndexDropBudget(IndexBuildOptions{CleanupBudget: 3 * time.Second}); got != 3*time.Second {
		t.Fatalf("explicit budget ignored: %s", got)
	}
}

// The Job's whole run (startup, the build window, the cleanup) fits its activeDeadlineSeconds with the chart default of 2400.
func TestDefaultBuildWindowAndCleanupFitTheJobDeadline(t *testing.T) {
	const startup = time.Minute
	if total := startup + DefaultIndexBuildDeadline + DefaultIndexCleanupBudget; total > 2400*time.Second {
		t.Fatalf("default window %s + cleanup %s + startup %s = %s exceeds the 2400 s Job deadline of the chart", DefaultIndexBuildDeadline, DefaultIndexCleanupBudget, startup, total)
	}
}

// IF NOT EXISTS over an INVALID index is a silent no-op that reports success.
func TestUnitsIndexBuildSQLHasNoIfNotExists(t *testing.T) {
	for _, index := range unitsIndexes {
		if strings.Contains(index.buildSQL(), "IF NOT EXISTS") {
			t.Fatalf("build SQL %q must not use IF NOT EXISTS", index.buildSQL())
		}
		if !strings.Contains(index.buildSQL(), "CREATE INDEX CONCURRENTLY ") || !strings.Contains(index.dropSQL(), "DROP INDEX CONCURRENTLY IF EXISTS") {
			t.Fatalf("build/drop SQL not concurrent: %q / %q", index.buildSQL(), index.dropSQL())
		}
	}
}

// The build lock is its own key: a build-index run never holds up, or reads as, a migrator.
func TestBuildIndexLockKeyIsNotTheMigratorLock(t *testing.T) {
	if buildIndexLockKey == lockKey {
		t.Fatalf("build lock key %#x is the migrator's lock key", buildIndexLockKey)
	}
	if buildIndexLockKey != 0x64686f5f6978 {
		t.Fatalf("build lock key %#x, want 0x64686f5f6978 (\"dho_ix\")", buildIndexLockKey)
	}
}

func TestIndexInfoState(t *testing.T) {
	for want, info := range map[string]indexInfo{
		"none":      {},
		"valid":     {exists: true, valid: true, isReady: true},
		"invalid":   {exists: true, isReady: true},
		"not_ready": {exists: true, valid: true},
	} {
		if got := info.state(); got != want {
			t.Errorf("state() = %s, want %s", got, want)
		}
	}
}

// The two sync_run_units indexes send the statements they sent before the table list existed, byte for byte (the texts
// below are copied from the code before the list), and the outbox index sends the same shape on its own table only.
func TestSyncRunUnitsIndexStatementsAreUnchanged(t *testing.T) {
	want := indexTable{
		name:      "sync_run_units",
		existsSQL: "SELECT pg_catalog.to_regclass('public.sync_run_units') IS NOT NULL",
		lockSQL:   "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE",
		infoSQL: `
SELECT i.indisvalid, i.indisready
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'public'::regnamespace AND c.relname = $1 AND i.indrelid = 'public.sync_run_units'::regclass`,
	}
	tables := map[string]indexTable{}
	for _, index := range unitsIndexes {
		tables[index.verb] = index.table
	}
	for _, verb := range []string{"failure-streak", "open-units"} {
		if tables[verb] != want {
			t.Fatalf("%s statements changed:\n got %#v\nwant %#v", verb, tables[verb], want)
		}
	}
	outbox := tables["finalize-outbox"]
	for _, statement := range []string{outbox.existsSQL, outbox.lockSQL, outbox.infoSQL} {
		if strings.Contains(statement, "sync_run_units") || !strings.Contains(statement, "public.worker_job_outbox") {
			t.Fatalf("finalize-outbox statement not on public.worker_job_outbox alone: %q", statement)
		}
	}
	if len(unitsIndexes) != 3 || len(tables) != 3 {
		t.Fatalf("indexes %d (verbs %d), want the closed list of 3", len(unitsIndexes), len(tables))
	}
}
