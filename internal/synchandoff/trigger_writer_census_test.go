package synchandoff

import (
	"io/fs"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// mintCallers is every production file that writes a trigger through Mint. Mint
// is the one writer of sync_manual_triggers and holds the scope guard
// (checkSelectedSources, CHAOS-10064), so a new trigger writer is a design
// change: it is added here on purpose, mapping UnselectedSourcesError to its own
// refusal, or not at all.
var mintCallers = map[string]string{
	"internal/backfillrun/handoff.go":             "dho backfill run: refused with exit 3",
	"internal/api/integrationsadmin/handoff.go":   "integration Sync now and Backfill: 400",
	"internal/api/syncadmin/manual_trigger.go":    "Sync admin Sync now and Backfill wizard: 400",
	"internal/scheduler/sync/webhook_requests.go": "webhook sync requests: the request is refused",
	"internal/scheduler/sync/initial_backfill.go": "the initial backfill: names no sources",
}

var (
	triggerTableWrite = regexp.MustCompile(`(?is)\binsert\s+into\s+(public\.)?sync_manual_triggers\b|\bupdate\s+(public\.)?sync_manual_triggers\s+set\b`)
	mintCall          = regexp.MustCompile(`\bsynchandoff\.Mint\(`)
	namedCaller       = regexp.MustCompile(`\bCaller:\s*"[a-z0-9_.:]+`)
)

// triggerCensus reads the production Go files under internal/ and cmd/ of
// fsys and returns every way they write sync_manual_triggers outside Mint or
// call Mint outside mintCallers, and the number of files it read.
func triggerCensus(fsys fs.FS) (failures []string, files int) {
	var writers, callers []string
	for _, dir := range []string{"internal", "cmd"} {
		err := fs.WalkDir(fsys, dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files++
			source, err := fs.ReadFile(fsys, path)
			if err != nil {
				return err
			}
			if triggerTableWrite.Match(source) {
				writers = append(writers, path)
			}
			if mintCall.Match(source) {
				callers = append(callers, path)
				if !namedCaller.Match(source) {
					failures = append(failures, path+" calls Mint without naming its Caller (the scope guard's log line names the path)")
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			failures = append(failures, err.Error())
		}
	}
	if want := []string{"internal/synchandoff/handoff.go"}; !reflect.DeepEqual(writers, want) {
		failures = append(failures, "sync_manual_triggers is written in "+strings.Join(writers, ", ")+"; only Mint may write it, behind its scope guard")
	}
	want := make([]string, 0, len(mintCallers))
	for file := range mintCallers {
		want = append(want, file)
	}
	sort.Strings(want)
	sort.Strings(callers)
	if !reflect.DeepEqual(callers, want) {
		failures = append(failures, "Mint is called from "+strings.Join(callers, ", ")+", the census names "+strings.Join(want, ", ")+
			": add a new trigger writer to mintCallers with its refusal, or remove a stale entry")
	}
	return failures, files
}

// TestEveryTriggerWriteGoesThroughTheScopeGuard fails when production code
// writes sync_manual_triggers anywhere but Mint, or calls Mint from a file not
// named in mintCallers: either would bypass or silently inherit the one scope
// guard of trigger writes.
func TestEveryTriggerWriteGoesThroughTheScopeGuard(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	failures, files := triggerCensus(os.DirFS(root))
	if files < 500 {
		t.Fatalf("read %d Go files; the census must see the production tree", files)
	}
	for _, failure := range failures {
		t.Error(failure)
	}
}

const censusModule = "github.com/full-chaos/dev-health-ops/internal/synchandoff"

// censusTree is a production tree the census accepts: Mint writes the table,
// and each mintCallers file calls Mint once, naming its Caller.
func censusTree() fstest.MapFS {
	tree := fstest.MapFS{
		"internal/synchandoff/handoff.go": {Data: []byte(`package synchandoff

func Mint(ctx context.Context, tx pgx.Tx, config *Config, input MintInput, now time.Time) (Trigger, error) {
	_, err := tx.Exec(ctx, ` + "`" + `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode)
VALUES ($1, $2)` + "`" + `, 1, input.Mode)
	return Trigger{}, err
}
`)},
		"internal/api/syncadmin/manual_trigger.go": {Data: []byte(`package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode, Caller: "syncadmin." + spec.triggeredBy}
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`)},
	}
	for _, file := range []string{
		"internal/backfillrun/handoff.go",
		"internal/api/integrationsadmin/handoff.go",
		"internal/scheduler/sync/webhook_requests.go",
		"internal/scheduler/sync/initial_backfill.go",
	} {
		tree[file] = &fstest.MapFile{Data: []byte(`package caller

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}
`)}
	}
	return tree
}

// TestTriggerCensusFailsEveryBypass plants, one at a time, each way to write
// sync_manual_triggers outside Mint or to reach Mint without the census seeing
// the call, in a tree the census otherwise accepts.
func TestTriggerCensusFailsEveryBypass(t *testing.T) {
	if failures, _ := triggerCensus(censusTree()); len(failures) > 0 {
		t.Fatalf("the census refuses the accepted tree: %v", failures)
	}
	const sneak = `package synchandoff

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, %s)
	return err
}
`
	for name, plant := range map[string]map[string]string{
		"a second writing function in handoff.go": {
			"internal/synchandoff/handoff.go": string(censusTree()["internal/synchandoff/handoff.go"].Data) +
				strings.TrimPrefix(strings.Replace(sneak, "%s", "`INSERT INTO public.sync_manual_triggers (occurrence_id) VALUES ($1)`, 1", 1), "package synchandoff\n"),
		},
		"a split literal in package synchandoff": {
			"internal/synchandoff/sneak.go": strings.Replace(sneak, "%s", `"INSERT INTO public.sync_manual" + "_triggers (occurrence_id) VALUES ($1)", 1`, 1),
		},
		"a package-level const table name": {
			"internal/scheduler/sync/sneak.go": `package sync

const triggerTable = "sync_manual_triggers"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT INTO public." + triggerTable + " (occurrence_id) VALUES ($1)", 1)
	return err
}
`,
		},
		"a statement split across variables": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	verb := "UPDATE public.sync_manual"
	statement := verb + "_triggers SET mode = $1"
	_, err := tx.Exec(ctx, statement, "x")
	return err
}
`,
		},
		"a table name passed to fmt.Sprintf": {
			"internal/scheduler/sync/sneak.go": `package sync

import "fmt"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO public.%s (occurrence_id) VALUES ($1)", "sync_manual_triggers"), 1)
	return err
}
`,
		},
		"a second Mint call with no Caller in a listed file": {
			"internal/scheduler/sync/initial_backfill.go": `package sync

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}

func again() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill"}, now)
	return err
}
`,
		},
		"a second named Mint call in a listed file": {
			"internal/backfillrun/handoff.go": `package backfillrun

import "` + censusModule + `"

func trigger() error {
	for _, caller := range []string{"a", "b"} {
		_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
		_, err = synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture_" + caller}, now)
		return err
	}
	return nil
}
`,
		},
		"a Mint call through an import alias": {
			"internal/other/aliased.go": `package other

import handoff "` + censusModule + `"

func trigger() error {
	_, err := handoff.Mint(ctx, tx, config, handoff.MintInput{Mode: "backfill", Caller: "aliased"}, now)
	return err
}
`,
		},
		"a Mint call through a dot import": {
			"internal/other/dotted.go": `package other

import . "` + censusModule + `"

func trigger() error {
	_, err := Mint(ctx, tx, config, MintInput{Mode: "backfill", Caller: "dotted"}, now)
	return err
}
`,
		},
		"Mint taken as a value": {
			"internal/other/value.go": `package other

import "` + censusModule + `"

var mint = synchandoff.Mint

func trigger() error {
	_, err := mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "value"}, now)
	return err
}
`,
		},
		"a Caller set only after the input is built": {
			"internal/api/syncadmin/manual_trigger.go": `package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode}
	input.Mode = "backfill"
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`,
		},
	} {
		tree := censusTree()
		for path, source := range plant {
			tree[path] = &fstest.MapFile{Data: []byte(source)}
		}
		if failures, _ := triggerCensus(tree); len(failures) == 0 {
			t.Errorf("%s: the census accepts it", name)
		}
	}
}
