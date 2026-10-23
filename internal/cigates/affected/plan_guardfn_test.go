package affected

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func guardFnInput(t *testing.T, changed string, fn func() ([]string, error)) planInput {
	t.Helper()
	root := t.TempDir()
	return planInput{
		event:   "pull_request",
		changed: []string{changed},
		golist: []goListEntry{{
			ImportPath: "m/a", Dir: filepath.Join(root, "a"),
			Module: &struct{ Path string }{Path: "m"},
		}, {ImportPath: "m/a.test"}},
		keys:    []string{"a"},
		root:    root,
		guardFn: fn,
	}
}

// The scan behind the guard parses every Go source; a verdict that returns before it must not pay for it.
func TestGuardFnIsNotCalledByAVerdictThatReturnsEarly(t *testing.T) {
	calls := 0
	in := guardFnInput(t, "go.mod", func() ([]string, error) {
		calls++
		return nil, errors.New("must not be called")
	})
	v, err := plan(in)
	if err != nil || v.Kind != "FULL" {
		t.Fatalf("verdict %+v, err %v: want FULL from the module-file rule", v, err)
	}
	if calls != 0 {
		t.Fatalf("guardFn called %d time(s) by a verdict that returns before the guard", calls)
	}
}

func TestGuardFnIsCalledOnceAndItsDefectPlansFull(t *testing.T) {
	calls := 0
	in := guardFnInput(t, "a/a.go", func() ([]string, error) {
		calls++
		return []string{"tables differ"}, nil
	})
	v, err := plan(in)
	if err != nil || v.Kind != "FULL" || !strings.Contains(v.Reason, "tables differ") {
		t.Fatalf("verdict %+v, err %v: want FULL naming the guard defect", v, err)
	}
	if calls != 1 {
		t.Fatalf("guardFn called %d time(s), want exactly 1", calls)
	}
}

func TestGuardFnWithoutDefectsReachesTheSelection(t *testing.T) {
	calls := 0
	in := guardFnInput(t, "a/a.go", func() ([]string, error) {
		calls++
		return nil, nil
	})
	v, err := plan(in)
	if err != nil || v.Kind != "AFFECTED" {
		t.Fatalf("verdict %+v, err %v: want AFFECTED", v, err)
	}
	if calls != 1 {
		t.Fatalf("guardFn called %d time(s), want exactly 1", calls)
	}
}

func TestGuardFnErrorSurfacesOnlyWhereTheGuardIsReached(t *testing.T) {
	boom := errors.New("scan the Go sources for reads: boom")
	if _, err := plan(guardFnInput(t, "a/a.go", func() ([]string, error) { return nil, boom })); !errors.Is(err, boom) {
		t.Fatalf("err %v, want the scan error from the guard path", err)
	}
	v, err := plan(guardFnInput(t, "go.mod", func() ([]string, error) { return nil, boom }))
	if err != nil || v.Kind != "FULL" {
		t.Fatalf("early verdict %+v, err %v: want FULL, the scan error is not reached", v, err)
	}
}

// On a tree whose Go source does not parse, a verdict that returns before the guard no longer fails on the scan: it
// plans FULL (every package runs, so go build/vet and plan-reads fail on the same source in that run). A verdict that
// reaches the guard still fails with the scan error.
func TestUnparsableTreeEarlyFullDoesNotScanAndGuardPathStillFails(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"a/a.go": "package a\n\nfunc f(\n",
		"golist.json": `{"ImportPath":"m/a","Dir":` + strconv.Quote(filepath.Join(root, "a")) + `,"Module":{"Path":"m"}}` + "\n" +
			`{"ImportPath":"m/a.test"}` + "\n",
		"keys.txt":    "a\n",
		"early.txt":   "go.mod\n",
		"guarded.txt": "a/a.go\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	verdict := func(changed string) (int, string, string) {
		var out, errb bytes.Buffer
		code := PlanVerdict([]string{"--event", "pull_request",
			"--changed", filepath.Join(root, changed), "--golist", filepath.Join(root, "golist.json"),
			"--keys", filepath.Join(root, "keys.txt"), "--root", root}, &out, &errb)
		return code, out.String(), errb.String()
	}
	if code, out, errs := verdict("early.txt"); code != 0 || !strings.HasPrefix(out, "FULL shared change: go.mod") || errs != "" {
		t.Fatalf("early FULL on an unparsable tree: rc %d stdout %q stderr %q, want rc 0, FULL, no scan error", code, out, errs)
	}
	if code, _, errs := verdict("guarded.txt"); code != 1 || !strings.Contains(errs, "scan the Go sources for reads:") {
		t.Fatalf("guard path on an unparsable tree: rc %d stderr %q, want rc 1 with the scan error", code, errs)
	}
}
