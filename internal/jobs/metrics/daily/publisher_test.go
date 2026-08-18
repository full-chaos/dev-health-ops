package daily

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// loadContractRegistry builds a real *jobruntime.Registry from the checked-in
// job contracts, the same way the production wiring and the integration
// tests do. No database is involved: the registry is pure descriptor data.
func loadContractRegistry(t *testing.T) *jobruntime.Registry {
	t.Helper()
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func testPublisher(t *testing.T) *PostgresPublisher {
	t.Helper()
	registry := loadContractRegistry(t)
	producer, err := joboutbox.NewTransactionProducer(registry)
	if err != nil {
		t.Fatal(err)
	}
	return &PostgresPublisher{producer: producer, registry: registry}
}

// TestPublishDispatchTxWrapsNonContractProducerErrorCause is the CHAOS-3905
// regression test: a producer error that is NOT a contract/policy rejection
// (e.g. the outbox write itself failing) must still surface its cause,
// exactly like the CHAOS-3903 fix already does for the contract/policy
// branch. Before the fix this path discarded the local err and returned a
// bare ErrUnavailable.
func TestPublishDispatchTxWrapsNonContractProducerErrorCause(t *testing.T) {
	publisher := testPublisher(t)
	writeFailure := errors.New("simulated postgres write failure: connection reset by peer")
	tx := failingExecTx{execErr: writeFailure}

	run := Run{ID: "00000000-0000-4000-8000-000000000001", OrganizationID: "00000000-0000-4000-8000-000000000002"}

	err := publisher.PublishDispatchTx(context.Background(), tx, run, "")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("PublishDispatchTx() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if errors.Is(err, ErrInvalidState) {
		t.Fatalf("PublishDispatchTx() misclassified a non-contract producer error as ErrInvalidState: %v", err)
	}
	if !strings.Contains(err.Error(), joboutbox.ErrUnavailable.Error()) {
		t.Fatalf("PublishDispatchTx() dropped the producer error cause, got: %v", err)
	}
}

// TestPublishDispatchTxWrapsContractRejectedCause guards the existing
// CHAOS-3903 behavior: a contract-rejected producer error still classifies
// as ErrInvalidState and keeps its underlying joboutbox reason.
func TestPublishDispatchTxWrapsContractRejectedCause(t *testing.T) {
	publisher := testPublisher(t)
	// Exec is never reached: the envelope fails contract validation
	// (organization_id must be a lowercase UUID) before any write.
	tx := failingExecTx{execErr: errors.New("must not be called")}

	run := Run{ID: "00000000-0000-4000-8000-000000000001", OrganizationID: "not-a-uuid"}

	err := publisher.PublishDispatchTx(context.Background(), tx, run, "")
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("PublishDispatchTx() error = %v, want errors.Is(err, ErrInvalidState)", err)
	}
	if !errors.Is(err, joboutbox.ErrContractRejected) {
		t.Fatalf("PublishDispatchTx() lost the contract-rejected cause, got: %v", err)
	}
}

// TestPublishFinalizeTxWrapsNonContractProducerErrorCause covers the other
// tx-based bare-return site CHAOS-3905 asked to fix consistently.
func TestPublishFinalizeTxWrapsNonContractProducerErrorCause(t *testing.T) {
	publisher := testPublisher(t)
	writeFailure := errors.New("simulated postgres write failure: connection reset by peer")
	tx := failingExecTx{execErr: writeFailure}

	run := Run{ID: "00000000-0000-4000-8000-000000000001", OrganizationID: "00000000-0000-4000-8000-000000000002"}

	err := publisher.PublishFinalizeTx(context.Background(), tx, run)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("PublishFinalizeTx() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if !strings.Contains(err.Error(), joboutbox.ErrUnavailable.Error()) {
		t.Fatalf("PublishFinalizeTx() dropped the producer error cause, got: %v", err)
	}
}

// TestNewPostgresPublisherWrapsProducerConstructionErrorCause covers the
// constructor bare-return site CHAOS-3905 asked to fix consistently.
func TestNewPostgresPublisherWrapsProducerConstructionErrorCause(t *testing.T) {
	registry := loadContractRegistry(t)
	_, err := NewPostgresPublisher(nil, registry)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewPostgresPublisher() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if !strings.Contains(err.Error(), joboutbox.ErrInvalidConfiguration.Error()) {
		t.Fatalf("NewPostgresPublisher() dropped the producer construction cause, got: %v", err)
	}
}

// failingExecTx is a minimal pgx.Tx stub whose only observable behavior is
// Exec() returning a caller-supplied error. Every other method is unused by
// producer.publish() on the paths these tests exercise.
type failingExecTx struct {
	execErr error
}

func (failingExecTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("failingExecTx: Begin not implemented")
}
func (failingExecTx) Commit(context.Context) error   { return nil }
func (failingExecTx) Rollback(context.Context) error { return nil }
func (failingExecTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("failingExecTx: CopyFrom not implemented")
}
func (failingExecTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (failingExecTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (failingExecTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("failingExecTx: Prepare not implemented")
}
func (tx failingExecTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, tx.execErr
}
func (failingExecTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("failingExecTx: Query not implemented")
}
func (failingExecTx) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
func (failingExecTx) Conn() *pgx.Conn                                  { return nil }
