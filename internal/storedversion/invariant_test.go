package storedversion_test

import (
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/localgit"
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
	for _, specs := range []map[string][]storedversion.Spec{streamSpecs, providersync.StoredVersionSpecs(), localgit.StoredVersionSpecs()} {
		if len(specs) == 0 {
			t.Fatal("a package lists no stored-version writers")
		}
		for table, writers := range specs {
			all[table] = append(all[table], writers...)
		}
	}
	return all
}

// repos.provider_repo_id is the provider's stable repository id, the anchor a
// rename is resolved by. Every writer of repos inserts it, so its version
// carries the held id: a writer that left it out would write empty over it. Only
// the provider sync states it, and only when its row has one (a provider sync
// row without it, a local repository and a pushed repository keep the held id).
func TestEveryReposWriterCarriesTheProviderRepoID(t *testing.T) {
	writers := allSpecs(t)["repos"]
	if len(writers) < 3 {
		t.Fatalf("found %d repos writers, want at least 3 (provider sync, local sync, external ingest)", len(writers))
	}
	for _, writer := range writers {
		positions, err := storedversion.Positions(writer.Insert)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := positions["provider_repo_id"]; !ok {
			t.Errorf("%s: its insert does not name provider_repo_id", writer.Name)
		}
		var rule storedversion.Rule
		for _, column := range writer.Contract.Columns {
			if column.Name == "provider_repo_id" {
				rule = column.Rule
			}
		}
		switch {
		case rule == 0:
			t.Errorf("%s: its contract does not name provider_repo_id", writer.Name)
		case writer.Contract.Writer == "provider_sync" && rule != storedversion.Unstated:
			t.Errorf("%s: provider_repo_id rule %d, want Unstated (kept when the row states none)", writer.Name, rule)
		case writer.Contract.Writer != "provider_sync" && rule != storedversion.NoField:
			t.Errorf("%s: provider_repo_id rule %d, want NoField (the writer has no provider id)", writer.Name, rule)
		}
		carry := writer.Carry(map[string]any{})
		if !carry["provider_repo_id"] {
			t.Errorf("%s: a row that states no provider_repo_id does not keep the held one", writer.Name)
		}
	}
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
