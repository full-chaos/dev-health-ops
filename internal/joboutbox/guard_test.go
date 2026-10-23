package joboutbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
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
