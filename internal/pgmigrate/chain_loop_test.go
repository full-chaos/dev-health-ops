package pgmigrate

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// scriptedWalks scripts walkWithRetry: each call of walk returns the next scripted
// outcome (or fails the test when the loop walks again when it should not), and
// recorded answers from a table.
type scriptedWalks struct {
	t        *testing.T
	script   []walked
	errs     []error
	called   int
	recorded map[string]bool
	asked    []string
}

func (s *scriptedWalks) walk() (walked, error) {
	s.t.Helper()
	if s.called >= len(s.script) {
		s.t.Fatalf("the loop walked %d times, the script has %d: it does not stop", s.called+1, len(s.script))
	}
	i := s.called
	s.called++
	return s.script[i], s.errs[i]
}

func (s *scriptedWalks) wasRecorded(revision string) bool {
	s.asked = append(s.asked, revision)
	return s.recorded[revision]
}

var errWalk = errors.New("walk failed")

// A walk that failed before it chose a revision is the run's failure: there is no revision to
// look up and no second walk.
func TestWalkWithRetryReturnsAFailureBeforeAnyRevisionWasChosen(t *testing.T) {
	walks := &scriptedWalks{t: t, script: []walked{{}}, errs: []error{errWalk}}
	_, err := walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.DiscardHandler))
	if !errors.Is(err, errWalk) || walks.called != 1 || len(walks.asked) != 0 {
		t.Fatalf("err=%v walks=%d asked=%v, want the walk's own failure, one walk and no lookup", err, walks.called, walks.asked)
	}
}

// A failed walk whose revision is now recorded was overtaken by another migrator: the walk is
// planned and run again, ONCE, and the run reports what the second walk applied.
func TestWalkWithRetryPlansAgainWhenAnotherMigratorRecordedTheRevision(t *testing.T) {
	walks := &scriptedWalks{
		t:        t,
		script:   []walked{{attempted: "0139"}, {action: "chain_applied", applied: []string{"0140", "0141"}}},
		errs:     []error{errWalk, nil},
		recorded: map[string]bool{"0139": true},
	}
	done, err := walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.DiscardHandler))
	if err != nil || fmt.Sprint(done.applied) != "[0140 0141]" || walks.called != 2 || fmt.Sprint(walks.asked) != "[0139]" {
		t.Fatalf("done=%+v err=%v walks=%d asked=%v", done, err, walks.called, walks.asked)
	}
}

// The retry happens once: a second failure is the run's, even when its revision is recorded too.
func TestWalkWithRetryRetriesOnlyOnce(t *testing.T) {
	walks := &scriptedWalks{
		t:        t,
		script:   []walked{{attempted: "0139"}, {attempted: "0140"}},
		errs:     []error{errWalk, errWalk},
		recorded: map[string]bool{"0139": true, "0140": true},
	}
	_, err := walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.DiscardHandler))
	if !errors.Is(err, errWalk) || walks.called != 2 {
		t.Fatalf("err=%v walks=%d, want the second failure after exactly two walks", err, walks.called)
	}
}

// A failed revision nothing recorded is the walk's own failure, with no second walk.
func TestWalkWithRetryReturnsTheFailureWhenTheRevisionIsNotRecorded(t *testing.T) {
	walks := &scriptedWalks{t: t, script: []walked{{attempted: "0140"}}, errs: []error{errWalk}, recorded: map[string]bool{}}
	_, err := walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.DiscardHandler))
	if !errors.Is(err, errWalk) || walks.called != 1 {
		t.Fatalf("err=%v walks=%d", err, walks.called)
	}
}

// The recovery is logged (Info, with the revision and the SQLSTATE, never the error text), and
// only the recovery: a clean run and a plain failure log nothing.
func TestWalkWithRetryLogsExactlyTheRecovery(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42701", Message: "column \"secret_row_value\" already exists"}
	run := func(script []walked, errs []error, recorded map[string]bool) string {
		var out bytes.Buffer
		walks := &scriptedWalks{t: t, script: script, errs: errs, recorded: recorded}
		_, _ = walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.NewJSONHandler(&out, nil)))
		return out.String()
	}
	got := run([]walked{{attempted: "0139"}, {}}, []error{fmt.Errorf("0139_x.sql: %w", pgErr), nil}, map[string]bool{"0139": true})
	for _, want := range []string{`"level":"INFO"`, `"revision":"0139"`, `"sqlstate":"42701"`, "another migrator recorded the revision"} {
		if !strings.Contains(got, want) {
			t.Errorf("the recovery log %q lacks %s", got, want)
		}
	}
	if strings.Contains(got, "secret_row_value") {
		t.Errorf("the recovery log carries the server's message: %q", got)
	}
	if clean := run([]walked{{}}, []error{nil}, nil); clean != "" {
		t.Errorf("a clean run logged %q", clean)
	}
	if failed := run([]walked{{attempted: "0139"}}, []error{errWalk}, map[string]bool{}); failed != "" {
		t.Errorf("a plain failure logged %q", failed)
	}
	if early := run([]walked{{}}, []error{errWalk}, nil); early != "" {
		t.Errorf("a failure before a revision was chosen logged %q", early)
	}
}
