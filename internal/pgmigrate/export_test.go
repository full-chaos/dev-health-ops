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

// FrozenChainFS is the embedded chain files with a revision at or below FrozenHead, plus every file of extra (the
// same layout), which is how a test plants a Go-only revision above the cut without touching the real sql dir.
func FrozenChainFS(t testing.TB, extra fs.FS) fs.FS {
	t.Helper()
	out := fstest.MapFS{}
	for _, dir := range []string{"sql", "sql/down"} {
		entries, err := fs.ReadDir(chainFiles, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			match := chainName.FindStringSubmatch(entry.Name())
			if entry.IsDir() || match == nil || match[1] > FrozenHead {
				continue
			}
			data, err := fs.ReadFile(chainFiles, dir+"/"+entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			out[dir+"/"+entry.Name()] = &fstest.MapFile{Data: data}
		}
	}
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

// UseFrozenChain installs the chain through FrozenHead: what every comparison with a frozen golden runs on.
func UseFrozenChain(t testing.TB) {
	t.Helper()
	UseChainFS(t, FrozenChainFS(t, nil))
}

// UsePlantedChain installs the chain through FrozenHead plus the revisions under dir (layout sql/ and sql/down/).
func UsePlantedChain(t testing.TB, extra fs.FS) {
	t.Helper()
	UseChainFS(t, FrozenChainFS(t, extra))
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
