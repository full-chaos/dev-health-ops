package synchandoff

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

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
)

// TestEveryTriggerWriteGoesThroughTheScopeGuard fails when production code
// writes sync_manual_triggers anywhere but Mint, or calls Mint from a file not
// named in mintCallers: either would bypass or silently inherit the one scope
// guard of trigger writes.
func TestEveryTriggerWriteGoesThroughTheScopeGuard(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var writers, callers []string
	files := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files++
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if triggerTableWrite.Match(source) {
				writers = append(writers, relative)
			}
			if mintCall.Match(source) {
				callers = append(callers, relative)
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if files < 500 {
		t.Fatalf("read %d Go files; the census must see the production tree", files)
	}
	if want := []string{"internal/synchandoff/handoff.go"}; !reflect.DeepEqual(writers, want) {
		t.Fatalf("sync_manual_triggers is written in %v; only Mint (%v) may write it, behind its scope guard", writers, want)
	}
	want := make([]string, 0, len(mintCallers))
	for file := range mintCallers {
		want = append(want, file)
	}
	sort.Strings(want)
	sort.Strings(callers)
	if !reflect.DeepEqual(callers, want) {
		t.Fatalf("Mint is called from %v, the census names %v: add a new trigger writer to mintCallers with its refusal, or remove a stale entry", callers, want)
	}
}
