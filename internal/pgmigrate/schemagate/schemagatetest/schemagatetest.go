// Package schemagatetest builds the recorded revisions and fake databases that service
// tests hand to schemagate.
package schemagatetest

import (
	"context"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Database is a process database fake whose alembic_version rows are Revisions (or Err).
type Database struct {
	Revisions []string
	Err       error
}

// RecordedRevisions satisfies schemagate.Recorder.
func (d Database) RecordedRevisions(context.Context) ([]string, error) {
	return append([]string(nil), d.Revisions...), d.Err
}

// AtHead is a database recorded at this build's heads.
func AtHead(t testing.TB) Database {
	t.Helper()
	return Database{Revisions: heads(t)}
}

// OneBehind is a database one migration behind this build: its application head is the revision before the build's
// (from the live chain, or from the archive the baseline was squashed from).
func OneBehind(t testing.TB) Database {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	all := append(pgmigrate.ArchivedChain(), chain...)
	previous := map[string]string{}
	for i := 1; i < len(all); i++ {
		previous[all[i].Revision] = all[i-1].Revision
	}
	revisions := []string{}
	for _, revision := range heads(t) {
		if before, ok := previous[revision]; ok {
			revision = before
		}
		revisions = append(revisions, revision)
	}
	sort.Strings(revisions)
	if equal(revisions, heads(t)) {
		t.Fatalf("no revision before the build heads %v", heads(t))
	}
	return Database{Revisions: revisions}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Ahead is a database that records a revision this build does not know.
func Ahead(t testing.TB) Database {
	t.Helper()
	return Database{Revisions: append(heads(t), "9999")}
}

func heads(t testing.TB) []string {
	t.Helper()
	build, err := pgmigrate.LoadBuildHeads()
	if err != nil {
		t.Fatal(err)
	}
	return build.Heads()
}
