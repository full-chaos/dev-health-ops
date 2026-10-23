package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func guardSentinel(t *testing.T) string {
	t.Helper()
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	return probes[0]
}

// countingTx records every database call. A producer or inserter that refuses
// an envelope must make none.
type countingTx struct {
	pgx.Tx
	calls int
}

func (tx *countingTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tx.calls++
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *countingTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	tx.calls++
	return nil, errors.New("unexpected query")
}

func (tx *countingTx) QueryRow(context.Context, string, ...any) pgx.Row {
	tx.calls++
	return nil
}

type countingRiver struct{ inserts int }

func (client *countingRiver) InsertTx(context.Context, pgx.Tx, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	client.inserts++
	return nil, errors.New("unexpected insert")
}

func poisonedEnvelope(sentinel string) jobcontract.Envelope {
	envelope := heartbeatEnvelope()
	envelope.CorrelationID = sentinel
	return envelope
}

func TestPublishRefusesACredentialShapedEnvelopeBeforeAnyWrite(t *testing.T) {
	sentinel := guardSentinel(t)
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindHeartbeat: testDescriptor()}}
	producer, err := NewTransactionProducer(registry)
	if err != nil {
		t.Fatal(err)
	}
	for name, publish := range map[string]func(*countingTx) error{
		"Publish": func(tx *countingTx) error {
			return producer.Publish(context.Background(), tx, jobcontract.KindHeartbeat, poisonedEnvelope(sentinel))
		},
		"PublishAfter": func(tx *countingTx) error {
			return producer.PublishAfter(context.Background(), tx, jobcontract.KindHeartbeat, poisonedEnvelope(sentinel), "")
		},
	} {
		t.Run(name, func(t *testing.T) {
			tx := &countingTx{}
			err := publish(tx)
			if !errors.Is(err, ErrContractRejected) || !errors.Is(err, jobcontract.ErrCredentialShapedValue) {
				t.Fatalf("error = %v, want a contract rejection carrying the value refusal", err)
			}
			if tx.calls != 0 {
				t.Fatalf("%d database calls (outbox rows) before the refusal", tx.calls)
			}
			if strings.Contains(err.Error(), sentinel) {
				t.Fatal("refusal echoes the value")
			}
		})
	}
}

func TestRelayRefusesAStoredCredentialShapedRowBeforeAnyRiverInsert(t *testing.T) {
	sentinel := guardSentinel(t)
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindHeartbeat: testDescriptor()}}
	row := testRow(t)
	row.Args = []byte(strings.Replace(string(row.Args), "relay-test-1", sentinel, 1))
	if !strings.Contains(string(row.Args), sentinel) {
		t.Fatal("test row was not poisoned")
	}
	river := &countingRiver{}
	tx := &countingTx{}
	inserter := &RiverInserter{client: river, registry: registry}
	_, err := inserter.Insert(context.Background(), tx, row)
	if !errors.Is(err, ErrContractRejected) {
		t.Fatalf("error = %v, want ErrContractRejected (the relay dead-letters it)", err)
	}
	if river.inserts != 0 || tx.calls != 0 {
		t.Fatalf("river inserts %d, database calls %d, want 0 and 0", river.inserts, tx.calls)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("refusal echoes the value")
	}
	if _, detail, terminal := failureEvidence(failureContract); strings.Contains(detail, sentinel) || !terminal {
		t.Fatalf("dead-letter evidence %q terminal=%v", detail, terminal)
	}
}

func logLoopResult(t *testing.T, result StepResult) string {
	t.Helper()
	var logs bytes.Buffer
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	loop, err := newReconcilerLoop(loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return result, nil
	}), ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	return logs.String()
}

// The retired-kind line and the provider-unit rearm line print fields read from
// queued rows; a credential-shaped value in one of them is omitted.
func TestReconcilerLoopLogsNoCredentialShapedQueueRowField(t *testing.T) {
	sentinel := guardSentinel(t)
	for name, result := range map[string]StepResult{
		"retired kind organization": {RetiredKindObservations: []RetiredKindObservation{
			{OutboxID: "00000000-0000-4000-8000-000000000010", JobKind: "workgraph.retired", OrganizationID: sentinel}}},
		"provider unit dedupe key": {ProviderUnitRearms: []ProviderUnitRearm{
			{OutboxID: "00000000-0000-4000-8000-000000000010", DedupeKey: sentinel, RiverJobID: 9}}},
	} {
		t.Run(name, func(t *testing.T) {
			text := logLoopResult(t, result)
			if text == "" {
				t.Fatal("nothing was logged")
			}
			if strings.Contains(text, sentinel) || strings.Contains(text, sentinel[len(sentinel)/2:]) {
				t.Fatalf("log carries the value: %s", text)
			}
			if !strings.Contains(text, "arguments_refused=true") || !strings.Contains(text, "00000000-0000-4000-8000-000000000010") {
				t.Fatalf("log lost the row identity or the refusal flag: %s", text)
			}
		})
	}
	benign := logLoopResult(t, StepResult{
		RetiredKindObservations: []RetiredKindObservation{{OutboxID: "00000000-0000-4000-8000-000000000010", JobKind: "workgraph.retired", OrganizationID: "00000000-0000-4000-8000-000000000002"}},
		ProviderUnitRearms:      []ProviderUnitRearm{{OutboxID: "00000000-0000-4000-8000-000000000010", DedupeKey: "sync.provider_unit:00000000-0000-4000-8000-000000000003", RiverJobID: 9}},
	})
	for _, want := range []string{"organization_id=00000000-0000-4000-8000-000000000002", "dedupe_key=sync.provider_unit:00000000-0000-4000-8000-000000000003"} {
		if !strings.Contains(benign, want) {
			t.Fatalf("a benign value was changed: %s", benign)
		}
	}
}
