package storedversion_test

import (
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"github.com/full-chaos/dev-health-ops/internal/storedversion/storedversiontest"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
)

// allSpecs gathers every contracted writer of every in-scope table across
// the packages that write them.
func allSpecs(t *testing.T) map[string][]storedversion.Spec {
	t.Helper()
	streamSpecs, err := streamhandlers.StoredVersionSpecs()
	if err != nil {
		t.Fatal(err)
	}
	all := map[string][]storedversion.Spec{}
	for _, specs := range []map[string][]storedversion.Spec{streamSpecs, providersync.StoredVersionSpecs()} {
		if len(specs) == 0 {
			t.Fatal("a package lists no stored-version writers")
		}
		for table, writers := range specs {
			all[table] = append(all[table], writers...)
		}
	}
	return all
}

// The invariant holds for every writer of each table together: after any
// sequence of writes by any of them on one key, FINAL serves the last value
// an authoritative writer stated, no writer's silence erases a value, and a
// column any writer marks terminal is never replaced by null.
func TestEveryContractedWriterKeepsTheStoredVersionInvariantTogether(t *testing.T) {
	all := allSpecs(t)
	tables := make([]string, 0, len(all))
	for table := range all {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		writers := all[table]
		t.Run(table, func(t *testing.T) {
			cells := storedversiontest.Enumerate(t, writers, 3)
			t.Logf("%s: %d writers, %d cells", table, len(writers), cells)
		})
	}
}
