package pgmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// stubSteps scripts the steps of applyChain: each call of step returns the next
// scripted outcome, and recorded answers from a table (or fails the test when the
// loop asks about a revision it should not).
type stubSteps struct {
	t                 *testing.T
	script            []stubStep
	called            int
	recordedRevisions map[string]bool
	asked             []string
}

type stubStep struct {
	applied, attempted string // "" = nil
	err                error
}

func (s *stubSteps) step(context.Context) (string, string, error) {
	s.t.Helper()
	if s.called >= len(s.script) {
		s.t.Fatalf("the loop asked for step %d of %d: it does not stop", s.called+1, len(s.script))
	}
	outcome := s.script[s.called]
	s.called++
	return outcome.applied, outcome.attempted, outcome.err
}

func (s *stubSteps) recorded(_ context.Context, revision string) bool {
	s.asked = append(s.asked, revision)
	return s.recordedRevisions[revision]
}

var errStep = errors.New("step failed")

// A step that fails before it chose a revision (its observation failed, or the
// database is no longer at a known revision) is the run's failure: there is no
// revision to look up, and the loop must not dereference one.
func TestApplyChainReturnsAFailureBeforeAnyRevisionWasChosen(t *testing.T) {
	steps := &stubSteps{t: t, script: []stubStep{{applied: "0139"}, {err: errStep}}}
	applied, err := applyChain(context.Background(), steps, slog.New(slog.DiscardHandler))
	if !errors.Is(err, errStep) {
		t.Fatalf("err = %v, want the step's own", err)
	}
	if fmt.Sprint(applied) != "[0139]" {
		t.Errorf("applied = %v, want the revision applied before the failure", applied)
	}
	if len(steps.asked) != 0 {
		t.Errorf("the loop asked whether %v was recorded after a failure with no revision", steps.asked)
	}
}

// A failed revision that is now recorded was applied by another migrator: the run
// goes on, and reports only what it applied itself.
func TestApplyChainCarriesOnWhenAnotherMigratorRecordedTheRevision(t *testing.T) {
	steps := &stubSteps{
		t:                 t,
		script:            []stubStep{{attempted: "0139", err: errStep}, {applied: "0140"}, {}},
		recordedRevisions: map[string]bool{"0139": true},
	}
	applied, err := applyChain(context.Background(), steps, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fmt.Sprint(applied) != "[0140]" {
		t.Errorf("applied = %v, want only the revision this run applied", applied)
	}
	if fmt.Sprint(steps.asked) != "[0139]" {
		t.Errorf("the loop asked about %v, want the failed revision", steps.asked)
	}
}

// A failed revision nothing recorded is the step's own failure.
func TestApplyChainReturnsTheStepFailureWhenTheRevisionIsNotRecorded(t *testing.T) {
	steps := &stubSteps{t: t, script: []stubStep{{applied: "0139"}, {attempted: "0140", err: errStep}}, recordedRevisions: map[string]bool{}}
	applied, err := applyChain(context.Background(), steps, slog.New(slog.DiscardHandler))
	if !errors.Is(err, errStep) {
		t.Fatalf("err = %v, want the step's own", err)
	}
	if fmt.Sprint(applied) != "[0139]" {
		t.Errorf("applied = %v", applied)
	}
}

func TestApplyChainStopsWhenNothingIsPendingAndReportsTheOrder(t *testing.T) {
	steps := &stubSteps{t: t, script: []stubStep{{applied: "0139"}, {applied: "0140"}, {applied: "0141"}, {}}}
	applied, err := applyChain(context.Background(), steps, slog.New(slog.DiscardHandler))
	if err != nil || fmt.Sprint(applied) != "[0139 0140 0141]" {
		t.Fatalf("applied = %v, %v", applied, err)
	}
}

// The recovery is logged (Info, with the revision and the SQLSTATE, never the
// error text), and only the recovery: a clean run and a plain failure log nothing.
func TestApplyChainLogsExactlyTheRecovery(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42701", Message: "column \"secret_row_value\" already exists"}
	run := func(script []stubStep, recorded map[string]bool) string {
		var out bytes.Buffer
		steps := &stubSteps{t: t, script: script, recordedRevisions: recorded}
		_, _ = applyChain(context.Background(), steps, slog.New(slog.NewJSONHandler(&out, nil)))
		return out.String()
	}
	got := run([]stubStep{{attempted: "0139", err: fmt.Errorf("0139_x.sql: %w", pgErr)}, {}}, map[string]bool{"0139": true})
	for _, want := range []string{`"level":"INFO"`, `"revision":"0139"`, `"sqlstate":"42701"`, "another migrator recorded the revision"} {
		if !strings.Contains(got, want) {
			t.Errorf("the recovery log %q lacks %s", got, want)
		}
	}
	if strings.Contains(got, "secret_row_value") {
		t.Errorf("the recovery log carries the server's message: %q", got)
	}
	if clean := run([]stubStep{{applied: "0139"}, {}}, nil); clean != "" {
		t.Errorf("a clean run logged %q", clean)
	}
	if failed := run([]stubStep{{attempted: "0139", err: errStep}}, map[string]bool{}); failed != "" {
		t.Errorf("a plain failure logged %q", failed)
	}
	if early := run([]stubStep{{err: errStep}}, nil); early != "" {
		t.Errorf("a failure before a revision was chosen logged %q", early)
	}
}
