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

// OneBehind is a database one migration behind this build.
func OneBehind(t testing.TB) Database {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil || len(chain) < 2 {
		t.Fatalf("the chain must hold two revisions: %v", err)
	}
	build := heads(t)
	top := chain[len(chain)-1].Revision
	revisions := []string{}
	for _, revision := range build {
		if revision == top {
			revision = chain[len(chain)-2].Revision
		}
		revisions = append(revisions, revision)
	}
	sort.Strings(revisions)
	return Database{Revisions: revisions}
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
