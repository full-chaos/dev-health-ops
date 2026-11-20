package helmrev

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/deploy/run"
)

// The real `helm history` tables of the kit (tests/fixtures/real; their
// sha256 is pinned to the kit SHA256SUMS by internal/deploy/run).
const realDir = "../run/testdata/real"

func readReal(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(realDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fake answers `helm history` with history and anything else with rc 0; it records every argv.
type fake struct {
	history []byte
	calls   []string
}

func (f *fake) Run(_ context.Context, c run.Cmd) (run.Output, error) {
	f.calls = append(f.calls, strings.Join(c.Argv, " "))
	if len(c.Argv) > 1 && c.Argv[1] == "history" {
		return run.Output{Stdout: f.history}, nil
	}
	return run.Output{}, nil
}

const hdr = "REVISION\tUPDATED                 \tSTATUS    \tCHART               \tAPP VERSION\tDESCRIPTION     \n"

func row(rev, status string) string {
	return rev + "     \tThu Nov 19 10:00:00 2026\t" + status + "\tdev-health-0.1.0    \t0.1.0      \tUpgrade complete\n"
}

func stop(t *testing.T, err error, what string) {
	t.Helper()
	if run.Code(err) != run.CodeStop {
		t.Fatalf("%s: want STOP, got %v", what, err)
	}
}

func TestTheRealHistoryTablesParse(t *testing.T) {
	rows, err := ParseHistory(readReal(t, "11699-helm-history.out"))
	if err != nil || !reflect.DeepEqual(rows, []Row{{3, "superseded"}, {4, "deployed"}}) {
		t.Fatalf("HISTORY: %v %v", rows, err)
	}
	rows, err = ParseHistory(readReal(t, "11699-helm-history-max1.out"))
	if err != nil || !reflect.DeepEqual(rows, []Row{{4, "deployed"}}) {
		t.Fatalf("NEWEST: %v %v", rows, err)
	}
	for name, out := range map[string]string{
		"empty":           "",
		"header only":     hdr,
		"no header":       row("4", "deployed"),
		"other header":    strings.Replace(hdr, "STATUS", "STATE", 1) + row("4", "deployed"),
		"5 columns":       hdr + "4\tThu\tdeployed\tc\t1\n",
		"a word revision": hdr + row("four", "deployed"),
		"revision 0":      hdr + row("0", "deployed"),
		"revision order":  hdr + row("5", "superseded") + row("4", "deployed"),
		"empty status":    hdr + row("4", "  "),
		"a status phrase": hdr + row("4", "pending upgrade"),
		"a blank row":     hdr + "\n" + row("4", "deployed"),
	} {
		if _, err := ParseHistory([]byte(out)); err == nil {
			t.Errorf("%s: want a parse error", name)
		}
	}
	if rows, err := ParseHistory([]byte(hdr + row("4", "deployed") + "\n\n")); err != nil || len(rows) != 1 {
		t.Fatalf("trailing blank lines: %v %v", rows, err)
	}
}

func TestTheReleaseIsTheKitRelArgs(t *testing.T) {
	if got := strings.Join(DevHealth.HistoryCmd(1).Argv, " "); got != "helm history dev-health -n dev-health --max 1" {
		t.Fatalf("argv %q (kit REL_ARGS='dev-health -n dev-health')", got)
	}
}

// G12: the live newest revision is the pre LIVEREV, deployed; with FAILED_REVS,
// the newest deployed row is LIVEREV and the rows above it are those, failed.
func TestNewestIs(t *testing.T) {
	ctx := context.Background()
	f := &fake{history: readReal(t, "11699-helm-history-max1.out")}
	n, err := NewestIs(ctx, f, DevHealth, 4, nil)
	if err != nil || n.Record() != "NEWEST 4 deployed = the pre LIVEREV" || f.calls[0] != "helm history dev-health -n dev-health --max 1" {
		t.Fatalf("%v %q %q", err, n.Record(), f.calls)
	}
	_, err = NewestIs(ctx, f, DevHealth, 3, nil)
	stop(t, err, "newest 4 is not LIVEREV 3")
	_, err = NewestIs(ctx, &fake{history: []byte(hdr + row("449", "deployed") + row("450", "failed"))}, DevHealth, 449, nil)
	stop(t, err, "a failed row above LIVEREV with no FAILED_REVS")
	_, err = NewestIs(ctx, &fake{history: []byte(hdr + row("449", "pending-upgrade"))}, DevHealth, 449, nil)
	stop(t, err, "newest is LIVEREV but not deployed")

	failed := []byte(hdr + row("448", "superseded") + row("449", "deployed") + row("450", "failed") + row("451", "failed"))
	f = &fake{history: failed}
	n, err = NewestIs(ctx, f, DevHealth, 449, []int{450, 451})
	if err != nil || n.Record() != "NEWEST deployed 449 = the pre LIVEREV; rows above it 450:failed,451:failed" || f.calls[0] != "helm history dev-health -n dev-health --max 4" {
		t.Fatalf("FAILED_REVS form: %v %q %q", err, n.Record(), f.calls)
	}
	for name, c := range map[string]struct {
		hist   string
		failed []int
	}{
		"one failed row missing":  {hdr + row("449", "deployed") + row("450", "failed") + row("451", "failed"), []int{450}},
		"a row above not failed":  {hdr + row("449", "deployed") + row("450", "failed") + row("451", "pending-upgrade"), []int{450, 451}},
		"deployed is not LIVEREV": {hdr + row("448", "deployed") + row("450", "failed"), []int{450}},
		"no deployed row":         {hdr + row("450", "failed"), []int{450}},
	} {
		_, err := NewestIs(ctx, &fake{history: []byte(c.hist)}, DevHealth, 449, c.failed)
		stop(t, err, name)
	}
}

// G12: a forged state.txt LIVEREV stops before the write; the fake Runner sees
// no `helm upgrade`. With the true LIVEREV the write runs right after the read.
func TestAForgedLiveRevStopsBeforeTheUpgrade(t *testing.T) {
	ctx := context.Background()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC) }
	sha := strings.Repeat("a", 64)
	upgrade := run.Cmd{Argv: []string{"helm", "upgrade", "dev-health", "./dev-health", "-n", "dev-health"}}
	for _, c := range []struct {
		liverev string
		pass    bool
	}{{"3", false}, {"4", true}} {
		s, err := run.BeginPre(root, "dev-health", "s41-"+c.liverev, sha, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.Dir, "state.txt"), []byte("LIVEREV="+c.liverev+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		live, err := s.GetInt("LIVEREV")
		if err != nil {
			t.Fatal(err)
		}
		f := &fake{history: readReal(t, "11699-helm-history-max1.out")}
		_, _, err = Guarded(ctx, f, DevHealth, int(live), nil, upgrade)
		want := []string{"helm history dev-health -n dev-health --max 1"}
		if c.pass {
			want = append(want, strings.Join(upgrade.Argv, " "))
			if err != nil {
				t.Fatalf("true LIVEREV: %v", err)
			}
		} else {
			stop(t, err, "a forged LIVEREV")
		}
		if !reflect.DeepEqual(f.calls, want) {
			t.Fatalf("LIVEREV %s: calls %q, want %q", c.liverev, f.calls, want)
		}
	}
}

// G13: ROLLBACK_REV == the LIVEREV read live (g_rbx) and is in the history
// (g_HAS); a miss stops after the one history read, before any later step.
func TestRollbackRevIsTheLiveRev(t *testing.T) {
	ctx := context.Background()
	f := &fake{history: readReal(t, "11699-helm-history.out")}
	live, err := RollbackGate(ctx, f, DevHealth, 4)
	if err != nil || live.Record() != "STATE LIVEREV=4" || !reflect.DeepEqual(f.calls, []string{"helm history dev-health -n dev-health --max 256"}) {
		t.Fatalf("%v %q %q", err, live.Record(), f.calls)
	}
	f = &fake{history: readReal(t, "11699-helm-history.out")}
	_, err = RollbackGate(ctx, f, DevHealth, 3)
	stop(t, err, "ROLLBACK_REV 3 != the live 4")
	if len(f.calls) != 1 {
		t.Fatalf("a refused rollback rev must stop after the history read; calls %q", f.calls)
	}
	_, _, err = LiveRev(ctx, &fake{history: []byte(hdr + row("450", "failed"))}, DevHealth)
	stop(t, err, "no deployed revision")

	rows, err := ParseHistory(readReal(t, "11699-helm-history.out"))
	if err != nil {
		t.Fatal(err)
	}
	if r, err := InHistory(rows, 3); err != nil || r != (Row{3, "superseded"}) {
		t.Fatalf("rev 3 is in the history: %v %v", r, err)
	}
	_, err = InHistory(rows, 5)
	stop(t, err, "rev 5 is not in the history")
	stop(t, RollbackIsLive(449, 450), "ROLLBACK_REV 449 != LIVEREV 450")
	if err := RollbackIsLive(449, 449); err != nil {
		t.Fatal(err)
	}
}
