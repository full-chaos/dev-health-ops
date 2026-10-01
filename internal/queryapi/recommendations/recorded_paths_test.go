package recommendations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded failure-mode golden is declared; all four are
// claims (a change to any makes a test of this package fail; backed by the mutation run in the
// pull request that added this file).

var failureModesClaims = []string{
	"[].exception",
	"[].mode",
	"[].rows",
	"[].window",
}

var failureModesNotClaims = map[string]string{}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile("testdata/failure_modes_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, failureModesClaims, failureModesNotClaims)
}

// TestRecordedExceptionIsTheOverflowThePortRefuses closes the gap in the failure-mode test, which
// reads `exception` only for being non-empty: every recorded exception must be Python's
// OverflowError (the uncaught date arithmetic on an oversized window), and the Go resolver must
// fail the same field with its overflow refusal, not with some other error.
func TestRecordedExceptionIsTheOverflowThePortRefuses(t *testing.T) {
	raw, err := os.ReadFile("testdata/failure_modes_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []failureModeGolden
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	raised := 0
	for _, c := range cases {
		if c.Exception == "" {
			continue
		}
		raised++
		if c.Exception != "OverflowError" {
			t.Errorf("%s/%s: recorded exception %q, want OverflowError", c.Window, c.Mode, c.Exception)
		}
		window := model.WindowInput{Value: 142857143, Unit: model.WindowUnitWeek}
		if c.Window == "oversized window" {
			window = model.WindowInput{Value: 1000000, Unit: model.WindowUnitDay}
		}
		client := &fakeClient{scanner: &fakeRowScanner{}}
		got, err := Resolve(context.Background(), client, "test-org", "team-a", window, computedAtFixture)
		if got != nil || !errors.Is(err, errWindowOverflow) {
			t.Errorf("%s/%s: python raised %s; Go answered %d rows, err %v, want errWindowOverflow", c.Window, c.Mode, c.Exception, len(got), err)
		}
	}
	if raised == 0 {
		t.Fatal("no recorded exception: the check proved nothing")
	}
}
