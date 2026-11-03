package pgmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"strconv"
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

// fixtureHead is the head the planted fixtures (testdata/postfreeze, the plants tests write) are numbered above:
// their first revision is fixtureHead+1. PlantedChainFS moves them to sit above the live head, so a released revision
// never collides with a plant.
const fixtureHead = 162

// LiveHead is the application head of the embedded build: the last revision of the embedded chain, or the
// baseline's application head while the chain is empty.
func LiveHead(t testing.TB) string {
	t.Helper()
	head := ""
	entries, err := fs.ReadDir(chainFiles, "sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if match := chainName.FindStringSubmatch(entry.Name()); match != nil && !entry.IsDir() && (head == "" || revisionLess(head, match[1])) {
			head = match[1]
		}
	}
	if head == "" {
		baseline, err := LoadBaseline()
		if err != nil {
			t.Fatal(err)
		}
		head = applicationHead(baseline)
	}
	return head
}

// PlantedRevision is where fixture revision n (written above fixtureHead) sits once planted: n moved by the live
// head's distance from fixtureHead.
func PlantedRevision(t testing.TB, n int) string {
	t.Helper()
	live, err := strconv.Atoi(LiveHead(t))
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%04d", n-fixtureHead+live)
}

var (
	plantedName   = regexp.MustCompile(`^(sql/(?:down/)?)([0-9]{4})(_.*)$`)
	plantedHeader = regexp.MustCompile(`\b(Revision|down_revision) ([0-9]{4})\b`)
)

func movePlant(t testing.TB, name string, data []byte) (string, []byte) {
	shift := func(revision string) string {
		n, err := strconv.Atoi(revision)
		if err != nil {
			t.Fatal(err)
		}
		return PlantedRevision(t, n)
	}
	if match := plantedName.FindStringSubmatch(name); match != nil {
		name = match[1] + shift(match[2]) + match[3]
	}
	data = plantedHeader.ReplaceAllFunc(data, func(header []byte) []byte {
		match := plantedHeader.FindSubmatch(header)
		return []byte(string(match[1]) + " " + shift(string(match[2])))
	})
	return name, data
}

// PlantedChainFS is the embedded chain (the revisions above the baseline) plus every file of extra (the same
// layout, numbered above fixtureHead), moved to sit above the live head, which is how a test plants a revision
// without touching the real sql dir.
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
			name, data = movePlant(t, name, data)
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
