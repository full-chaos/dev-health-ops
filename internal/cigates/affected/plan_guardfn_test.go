package affected

import (
	"errors"
	"path/filepath"
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
