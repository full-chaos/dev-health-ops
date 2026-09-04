package remaining

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

// TestJobKindForFamilyCoversEveryRegisteredFamily is the CHAOS-4993/CHAOS-5007
// recurrence guard. familyJobKinds used to be a fourth hardcoded family->kind
// table, separate from families.json, registry.json, migration-state.json and
// the Python job_contracts mirror, and nothing enforced that a new family's
// entry landed here too -- its absence for "work_item_attribution" made
// NewPartitionHandler refuse with ErrUnavailable for every partition of that
// kind, which daily.go's registration loop turned into a full "daily" family
// composition failure (worker_family_composition_failed) instead of a
// graceful per-kind skip -- reproduced live against a migrated ClickHouse in
// TestMetricsAndSyncQueueSelectionBootsWithMigratedClickHouse.
//
// CHAOS-5007 removed that hardcoded map: JobKindForFamily now derives
// directly from families.json (see loadFamilyJobKinds), so this specific
// pairing can no longer drift by construction. This test stays as the
// vacuous-pass guard (a families.json that decoded to zero families would
// make every loop below trivially pass) and as a locked-in contract that
// JobKindForFamily's behavior actually matches the inventory it claims to be
// derived from. TestJobKindForFamilyResolvesToARegisteredContractDescriptor
// below covers the axis this test can no longer catch on its own: whether
// the resolved kind is a REAL registered job kind, not just an internally
// self-consistent one.
func TestJobKindForFamilyCoversEveryRegisteredFamily(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Families) == 0 {
		t.Fatal("families.json produced no families -- test would pass vacuously")
	}
	for _, family := range inventory.Families {
		kind, ok := JobKindForFamily(family.Name)
		if !ok {
			t.Errorf("familyJobKinds has no entry for family %q (route_key %q)", family.Name, family.RouteKey)
			continue
		}
		if kind != family.RouteKey {
			t.Errorf("familyJobKinds[%q] = %q, want %q (families.json's route_key)", family.Name, kind, family.RouteKey)
		}
	}
}

// TestJobKindForFamilyResolvesToARegisteredContractDescriptor is the
// CHAOS-5007 parity test between families.json and registry.json (the two
// of the four family->kind tables that were never cross-checked against each
// other outside a live-database integration test): every family's resolved
// job kind must actually exist in the checked-in contract registry.
// PublishPartitionTx calls exactly this lookup, via publisher.registry.
// Descriptor(kind), on the live publish path -- a route_key with no matching
// registry entry surfaces there as ErrUnavailable per-partition, the same
// swallowed-cause shape CHAOS-4993 is about, except for a mismatch this test
// now catches in `go test`, before it ever reaches a running worker.
func TestJobKindForFamilyResolvesToARegisteredContractDescriptor(t *testing.T) {
	registry := loadContractRegistry(t)
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Families) == 0 {
		t.Fatal("families.json produced no families -- test would pass vacuously")
	}
	for _, family := range inventory.Families {
		kind, ok := JobKindForFamily(family.Name)
		if !ok {
			t.Errorf("JobKindForFamily(%q) resolved to nothing", family.Name)
			continue
		}
		if _, ok := registry.Descriptor(kind); !ok {
			t.Errorf(
				"family %q resolves to kind %q, which registry.json has no descriptor for "+
					"(add it to contracts/jobs/v1/registry.json and migration-state.json)",
				family.Name, kind,
			)
		}
	}
}

// TestJobKindForFamilyUnknownFamilyReturnsFalseWithoutPanicking guards the
// sync.Once-cached derivation in loadFamilyJobKinds (CHAOS-5007): the ONLY
// panic path there is families.json itself failing to decode/validate (a
// build-time defect in the embedded artifact), never a per-call lookup miss.
// An unregistered family name -- typos, a retired family, a family that
// exists in some OTHER inventory -- must come back as ("", false) like an
// ordinary map miss, exactly as it did before the map was derived from
// families.json instead of hand-written.
func TestJobKindForFamilyUnknownFamilyReturnsFalseWithoutPanicking(t *testing.T) {
	for _, family := range []string{
		"",
		"not_a_real_family",
		"metrics.remaining.capacity", // a kind string, not a family name
		"CAPACITY",                   // wrong case
	} {
		kind, ok := JobKindForFamily(family)
		if ok {
			t.Errorf("JobKindForFamily(%q) = (%q, true), want (\"\", false)", family, kind)
		}
		if kind != "" {
			t.Errorf("JobKindForFamily(%q) kind = %q, want empty string on a miss", family, kind)
		}
	}
}

// TestPublishPartitionTxWrapsNonContractProducerErrorCause is the CHAOS-3905
// regression test: a producer error that is NOT a contract/policy rejection
// (e.g. the outbox write itself failing) must still surface its cause,
// exactly like the CHAOS-3903 fix already does for the contract/policy
// branch. Before the fix this path discarded the local err and returned a
// bare ErrUnavailable.
func TestPublishPartitionTxWrapsNonContractProducerErrorCause(t *testing.T) {
	publisher := testPublisher(t)
	writeFailure := errors.New("simulated postgres write failure: connection reset by peer")
	tx := failingExecTx{execErr: writeFailure}

	run := Run{
		ID:             "00000000-0000-4000-8000-000000000001",
		OrganizationID: "00000000-0000-4000-8000-000000000002",
		Family:         "capacity",
	}
	partition := Partition{ID: "00000000-0000-4000-8000-000000000003", RunID: run.ID, Ordinal: 1}

	err := publisher.PublishPartitionTx(context.Background(), tx, run, partition, "")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("PublishPartitionTx() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if errors.Is(err, ErrInvalidState) {
		t.Fatalf("PublishPartitionTx() misclassified a non-contract producer error as ErrInvalidState: %v", err)
	}
	if !strings.Contains(err.Error(), joboutbox.ErrUnavailable.Error()) {
		t.Fatalf("PublishPartitionTx() dropped the producer error cause, got: %v", err)
	}
}

// TestPublishPartitionTxWrapsContractRejectedCause guards the existing
// CHAOS-3903 behavior: a contract-rejected producer error still classifies
// as ErrInvalidState and keeps its underlying joboutbox reason.
func TestPublishPartitionTxWrapsContractRejectedCause(t *testing.T) {
	publisher := testPublisher(t)
	// Exec is never reached: the envelope fails contract validation
	// (organization_id must be a lowercase UUID) before any write.
	tx := failingExecTx{execErr: errors.New("must not be called")}

	run := Run{
		ID:             "00000000-0000-4000-8000-000000000001",
		OrganizationID: "not-a-uuid",
		Family:         "capacity",
	}
	partition := Partition{ID: "00000000-0000-4000-8000-000000000003", RunID: run.ID, Ordinal: 1}

	err := publisher.PublishPartitionTx(context.Background(), tx, run, partition, "")
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("PublishPartitionTx() error = %v, want errors.Is(err, ErrInvalidState)", err)
	}
	if !errors.Is(err, joboutbox.ErrContractRejected) {
		t.Fatalf("PublishPartitionTx() lost the contract-rejected cause, got: %v", err)
	}
}

// TestNewPostgresPublisherWrapsProducerConstructionErrorCause covers the
// other bare-return site CHAOS-3905 asked to fix consistently.
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
