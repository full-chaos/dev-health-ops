package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	chgoproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/full-chaos/dev-health-ops/internal/platform/connerr"
)

// droppedFirstBlockError has the shape of the driver error of a connection
// that dropped under an INSERT: the clickhouse-go first-block line over the
// ch-go "read: EOF" wrap of io.EOF (the real chain is built in
// internal/platform/connerr TestIsDroppedOnTheRealFirstBlockReadChain).
func droppedFirstBlockError() error {
	return fmt.Errorf("query processing: failed to read first block packet from %s (conn_id=%d): %w",
		"127.0.0.1:9000", 1, fmt.Errorf("read: %w", io.EOF))
}

// droppedInsertError is the error the service conn returns for an INSERT
// whose connection dropped: the driver error marked connerr.AtWrite
// (internal/storage/clickhouse TestTheServiceConnMarksOnlyItsInsertCalls).
func droppedInsertError() error { return connerr.AtWrite(droppedFirstBlockError()) }

// failingEffectSink fails every write with err; onWrite runs first.
type failingEffectSink struct {
	err     error
	onWrite func()
}

func (sink failingEffectSink) WriteEffect(context.Context, Claim, EffectBatch) error {
	if sink.onWrite != nil {
		sink.onWrite()
	}
	return sink.err
}

// commitWithFailingSink commits one pending effect whose write fails with
// sinkErr, through the real EffectCommitter, and returns its error.
func commitWithFailingSink(
	t *testing.T, ctx context.Context, destination string, recovery EffectRecoveryPolicy,
	sinkErr error, onWrite func(),
) error {
	t.Helper()
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	claim := nativeTestClaim("github", "cicd")
	batch := effectBatchFixture(t, destination, recovery, `{"org_id":"org-acme","run_id":"9001"}`)
	ledger := &memoryEffectLedger{}
	_, err := (EffectCommitter{
		Ledger: ledger, Sink: failingEffectSink{err: sinkErr, onWrite: onWrite},
		Readback: staticEffectReadback{}, Now: func() time.Time { return now },
	}).Commit(ctx, claim, []EffectBatch{batch}, now)
	if err == nil {
		t.Fatal("Commit() = nil, want the sink error")
	}
	if len(ledger.state.Effects) != 1 || ledger.state.Effects[0].Status != GenerationBlockWriting {
		t.Fatalf("ledger=%+v, want the effect left Writing by the failed write", ledger.state.Effects)
	}
	return err
}

func requireSinkDropWrapped(t *testing.T, err, sinkErr error, destination string) {
	t.Helper()
	var dropped *SinkConnectionDroppedError
	if !errors.As(err, &dropped) || dropped.Destination != destination {
		t.Fatalf("error %v (%T) is not a *SinkConnectionDroppedError for %q", err, err, destination)
	}
	if !errors.Is(err, io.EOF) || !errors.Is(err, sinkErr) {
		t.Fatalf("wrapped error lost the driver chain: errors.Is(io.EOF)=%v errors.Is(sinkErr)=%v",
			errors.Is(err, io.EOF), errors.Is(err, sinkErr))
	}
	if err.Error() != sinkErr.Error() {
		t.Fatalf("text %q, want the driver text %q unchanged", err.Error(), sinkErr.Error())
	}
}

func requireSinkErrorUnchanged(t *testing.T, err, sinkErr error) {
	t.Helper()
	var dropped *SinkConnectionDroppedError
	if errors.As(err, &dropped) || err != sinkErr {
		t.Fatalf("error %v (%T), want the sink error returned unchanged", err, err)
	}
}

// (a) A dropped write to a ReadbackRequired ReplacingMergeTree destination.
func TestEffectCommitterNamesADroppedReadbackRequiredWrite(t *testing.T) {
	sinkErr := droppedInsertError()
	err := commitWithFailingSink(t, context.Background(), "test_suite_results", EffectReadbackRequired, sinkErr, nil)
	requireSinkDropWrapped(t, err, sinkErr, "test_suite_results")
}

// (b) A dropped write to a ReplaySafe ReplacingMergeTree destination.
func TestEffectCommitterNamesADroppedReplaySafeWrite(t *testing.T) {
	sinkErr := droppedInsertError()
	err := commitWithFailingSink(t, context.Background(), "feature_flag", EffectReplaySafe, sinkErr, nil)
	requireSinkDropWrapped(t, err, sinkErr, "feature_flag")
}

// (c) A Blocked effect has no recovery: its dropped write is not named.
func TestEffectCommitterDoesNotNameADroppedBlockedWrite(t *testing.T) {
	sinkErr := droppedInsertError()
	err := commitWithFailingSink(t, context.Background(), "test_suite_results", EffectRecoveryBlocked, sinkErr, nil)
	requireSinkErrorUnchanged(t, err, sinkErr)
}

// (d) feature_flag_event is a plain MergeTree: a second copy of a dropped
// INSERT the server applied late is a real duplicate, so it is not named.
func TestEffectCommitterDoesNotNameADroppedFeatureFlagEventWrite(t *testing.T) {
	sinkErr := droppedInsertError()
	err := commitWithFailingSink(t, context.Background(), "feature_flag_event", EffectReadbackRequired, sinkErr, nil)
	requireSinkErrorUnchanged(t, err, sinkErr)
}

// (e) The work context ended during the write (shutdown, lease loss): the
// driver closed the connection itself, so the EOF is no drop.
func TestEffectCommitterDoesNotNameAnEOFAfterTheWorkContextEnded(t *testing.T) {
	sinkErr := droppedInsertError()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := commitWithFailingSink(t, ctx, "test_suite_results", EffectReadbackRequired, sinkErr, cancel)
	requireSinkErrorUnchanged(t, err, sinkErr)
}

// (f) A ClickHouse server exception is not a dropped connection.
func TestEffectCommitterDoesNotNameAClickHouseException(t *testing.T) {
	sinkErr := connerr.AtWrite(&chgoproto.Exception{Code: 241, Name: "DB::Exception", Message: "Memory limit (total) exceeded"})
	err := commitWithFailingSink(t, context.Background(), "test_suite_results", EffectReadbackRequired, sinkErr, nil)
	requireSinkErrorUnchanged(t, err, sinkErr)
}

// Every route's effect destination has a stated decision: wrappable (a
// ReplacingMergeTree) or not wrapped (with a reason). A new destination fails
// here until someone decides.
func TestEveryRouteDestinationHasASinkDropDecision(t *testing.T) {
	routeDestinations := map[string]struct{}{}
	for _, provider := range MatrixProviders() {
		for _, capability := range Capabilities(provider) {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok {
				t.Fatalf("%s/%s: no descriptor", provider, capability.Dataset)
			}
			for _, destination := range descriptor.Destinations {
				routeDestinations[destination] = struct{}{}
				_, wrappable := sinkDropWrappableDestinations[destination]
				_, notWrapped := sinkDropNotWrappedDestinations[destination]
				if wrappable == notWrapped {
					t.Errorf("%s/%s destination %q: wrappable=%v not-wrapped=%v, want exactly one decision",
						provider, capability.Dataset, destination, wrappable, notWrapped)
				}
			}
		}
	}
	if len(routeDestinations) == 0 {
		t.Fatal("no route destinations found")
	}
	for destination := range sinkDropWrappableDestinations {
		if _, ok := routeDestinations[destination]; !ok {
			t.Errorf("wrappable destination %q is no route destination", destination)
		}
	}
	for destination := range sinkDropNotWrappedDestinations {
		if _, ok := routeDestinations[destination]; !ok {
			t.Errorf("not-wrapped destination %q is no route destination", destination)
		}
	}
}

// The allowlist is not a guess: each wrappable destination is created as a
// ReplacingMergeTree by the ClickHouse migrations, and each not-wrapped one is
// not.
func TestSinkDropWrappableDestinationsAreReplacingMergeTree(t *testing.T) {
	var schema []byte
	head, err := os.ReadFile(filepath.Join("..", "chmigrate", "baseline", "head.json"))
	if err != nil {
		t.Fatal(err)
	}
	schema = append(schema, head...)
	later, err := filepath.Glob(filepath.Join("..", "chmigrate", "sql", "*.sql"))
	if err != nil || len(later) == 0 {
		t.Fatalf("migration files: %v (%d)", err, len(later))
	}
	for _, path := range later {
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		schema = append(schema, body...)
	}
	engineOf := func(table string) string {
		create := regexp.MustCompile(`CREATE TABLE (?:IF NOT EXISTS )?` + regexp.QuoteMeta(table) +
			`\s[^;"]*?ENGINE = (\w+)`)
		matches := create.FindAllSubmatch(schema, -1)
		if len(matches) != 1 {
			return fmt.Sprintf("%d CREATE statements", len(matches))
		}
		return string(matches[0][1])
	}
	for destination := range sinkDropWrappableDestinations {
		if engine := engineOf(destination); engine != "ReplacingMergeTree" {
			t.Errorf("wrappable destination %q: engine %s, want ReplacingMergeTree", destination, engine)
		}
	}
	for destination := range sinkDropNotWrappedDestinations {
		if engine := engineOf(destination); engine == "ReplacingMergeTree" {
			t.Errorf("not-wrapped destination %q is a ReplacingMergeTree: move it to the wrappable set", destination)
		}
	}
}

// (g) A dropped READ inside the write (the stored-finish-day SELECT before the
// ci_pipeline_runs INSERT, a readback after it) is no dropped INSERT: no mark,
// so the error is returned unchanged (unclassified, cap 2). Before, after and
// joined with a successful INSERT alike.
func TestEffectCommitterDoesNotNameADroppedReadInsideTheWrite(t *testing.T) {
	for name, sinkErr := range map[string]error{
		"read before the insert":      fmt.Errorf("read stored ci_pipeline_runs finish days: %w", droppedFirstBlockError()),
		"read after a stored insert":  fmt.Errorf("read back stored rows: %w", droppedFirstBlockError()),
		"read beside an insert error": errors.Join(connerr.AtWrite(errors.New("code: 60, unknown table")), droppedFirstBlockError()),
	} {
		t.Run(name, func(t *testing.T) {
			err := commitWithFailingSink(t, context.Background(), "ci_pipeline_runs", EffectReadbackRequired, sinkErr, nil)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("premise: %v does not carry the drop", err)
			}
			requireSinkErrorUnchanged(t, err, sinkErr)
		})
	}
}

// (h) The INSERT mark survives the wrapping on the way to the committer: %w
// (the touched-days "send touched days ..." wrap) and errors.Join.
func TestEffectCommitterNamesAWrappedDroppedInsert(t *testing.T) {
	for name, sinkErr := range map[string]error{
		"%w":           fmt.Errorf("send touched days of moved ci_pipeline_runs: %w", droppedInsertError()),
		"errors.Join":  errors.Join(errors.New("abort batch"), droppedInsertError()),
		"%w over join": fmt.Errorf("write: %w", errors.Join(droppedInsertError(), errors.New("abort batch"))),
	} {
		t.Run(name, func(t *testing.T) {
			err := commitWithFailingSink(t, context.Background(), "ci_pipeline_runs", EffectReadbackRequired, sinkErr, nil)
			requireSinkDropWrapped(t, err, sinkErr, "ci_pipeline_runs")
		})
	}
}

// (i) The recovery readback of a Writing effect (commitPrepared, before any
// rewrite) is a read: its drop is returned unchanged, never named.
func TestEffectCommitterDoesNotNameADroppedRecoveryReadback(t *testing.T) {
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	claim := nativeTestClaim("github", "cicd")
	batch := effectBatchFixture(t, "test_suite_results", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"9001"}`)
	ledger := &memoryEffectLedger{}
	if _, err := (EffectCommitter{
		Ledger: ledger, Sink: failingEffectSink{err: droppedInsertError()},
		Readback: staticEffectReadback{}, Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, []EffectBatch{batch}, now); err == nil || ledger.state.Effects[0].Status != GenerationBlockWriting {
		t.Fatalf("premise: first write err=%v, want a failed write that leaves the effect Writing", err)
	}
	readErr := fmt.Errorf("inspect test_suite_results: %w", droppedFirstBlockError())
	_, err := (EffectCommitter{
		Ledger: ledger, Sink: failingEffectSink{err: errors.New("premise: no rewrite before the readback")},
		Readback: staticEffectReadback{err: readErr}, Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, []EffectBatch{batch}, now)
	requireSinkErrorUnchanged(t, err, readErr)
}
