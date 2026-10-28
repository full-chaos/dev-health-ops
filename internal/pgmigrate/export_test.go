package pgmigrate

import (
	"context"
	"io/fs"
	"log/slog"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
)

// UseChainFS makes LoadChain and LoadDownChain read fsys (layout sql/ and sql/down/) until the test ends. Tests
// that use it must not run in parallel with a test that reads the chain.
func UseChainFS(t testing.TB, fsys fs.FS) {
	t.Helper()
	previous := chainFS.Load()
	chainFS.Store(&fsys)
	t.Cleanup(func() { chainFS.Store(previous) })
}

// PlantedChainFS is the embedded chain (the revisions above the baseline) plus every file of extra (the same
// layout), which is how a test plants a revision without touching the real sql dir.
func PlantedChainFS(t testing.TB, extra fs.FS) fs.FS {
	t.Helper()
	out := fstest.MapFS{}
	for _, dir := range []string{"sql", "sql/down"} {
		entries, err := fs.ReadDir(chainFiles, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || chainName.FindStringSubmatch(entry.Name()) == nil {
				continue
			}
			data, err := fs.ReadFile(chainFiles, dir+"/"+entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			out[dir+"/"+entry.Name()] = &fstest.MapFile{Data: data}
		}
	}
	out["sql/down/README.md"] = &fstest.MapFile{Data: []byte("placeholder\n")}
	if extra != nil {
		err := fs.WalkDir(extra, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(extra, name)
			if err != nil {
				return err
			}
			out[name] = &fstest.MapFile{Data: data}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// UsePlantedChain installs the embedded chain plus the revisions under extra (layout sql/ and sql/down/).
func UsePlantedChain(t testing.TB, extra fs.FS) {
	t.Helper()
	UseChainFS(t, PlantedChainFS(t, extra))
}

// DropInvalidUnitsIndex runs the cleanup that follows a final failed build of the index the verb names.
func DropInvalidUnitsIndex(ctx context.Context, conn *pgx.Conn, verb string, options IndexBuildOptions, logger *slog.Logger) {
	index, _ := unitsIndexByVerb(verb)
	var pid int32
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		panic(err)
	}
	dropInvalidUnitsIndex(ctx, lockedSession{conn: conn, pid: pid}, index, options, logger)
}

// BuildIndexLockKey is the advisory lock key one build-index run holds.
const BuildIndexLockKey = buildIndexLockKey

// OpenUnitsBuildIndexLockKey is the advisory lock key one open-units build-index run holds.
const OpenUnitsBuildIndexLockKey = openUnitsBuildIndexLockKey
