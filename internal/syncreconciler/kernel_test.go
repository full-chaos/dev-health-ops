package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type kernelStepper struct {
	calls int
}

func (stepper *kernelStepper) Step(_ context.Context, _ time.Time, _ int) (Observation, error) {
	stepper.calls++
	return Observation{Limit: 1}, nil
}

type fakeKernelRows struct {
	pgx.Rows
	claims []TransportClaim
	index  int
	err    error
}

func (rows *fakeKernelRows) Next() bool { return rows.index < len(rows.claims) }

func (rows *fakeKernelRows) Scan(dest ...any) error {
	if rows.index >= len(rows.claims) {
		return errors.New("scan past rows")
	}
	claim := rows.claims[rows.index]
	rows.index++
	values := []any{
		claim.ID, claim.Kind, claim.ClaimToken, claim.RouteGeneration,
		claim.AvailableAt, claim.Attempts,
	}
	for index, destination := range dest {
		switch typed := destination.(type) {
		case *string:
			*typed = values[index].(string)
		case *int64:
			*typed = values[index].(int64)
		case *time.Time:
			*typed = values[index].(time.Time)
		default:
			return errors.New("unexpected scan destination")
		}
	}
	return nil
}

func (rows *fakeKernelRows) Err() error { return rows.err }
func (*fakeKernelRows) Close()          {}

type fakeKernelTx struct {
	pgx.Tx
	claims       []TransportClaim
	querySQL     string
	queryArgs    []any
	execSQL      []string
	commit       bool
	rollback     bool
	terminalRows int64
}

func (tx *fakeKernelTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.querySQL = sql
	tx.queryArgs = args
	return &fakeKernelRows{claims: append([]TransportClaim(nil), tx.claims...)}, nil
}

func (tx *fakeKernelTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(tx.terminalRows, 10)), nil
}

func (tx *fakeKernelTx) Commit(context.Context) error {
	tx.commit = true
	return nil
}

func (tx *fakeKernelTx) Rollback(context.Context) error {
	tx.rollback = true
	return nil
}

func riverRegistry(t *testing.T, riverKinds ...string) registryMap {
	t.Helper()
	registry := testRegistry(t, "")
	for _, kind := range riverKinds {
		descriptor := registry[kind]
		descriptor.Route = syncdispatchcontract.RouteRiver
		registry[kind] = descriptor
	}
	return registry
}

func kernelClaim(kind string, at time.Time, id string) TransportClaim {
	return TransportClaim{
		ID:              id,
		Kind:            kind,
		ClaimToken:      "10000000-0000-4000-8000-000000000001",
		RouteGeneration: 7,
		AvailableAt:     at,
		Attempts:        2,
	}
}

func TestNewKernelSeparatesDomainObservationFromQueueMutationPool(t *testing.T) {
	domainPool := &pgxpool.Pool{}
	queuePool := &pgxpool.Pool{}
	registry := riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun)

	shadow, err := NewKernel(domainPool, nil, registry, KernelModeShadow)
	if err != nil || shadow.begin != nil {
		t.Fatalf("NewKernel(shadow) = %#v, %v", shadow, err)
	}
	if _, err := NewKernel(domainPool, nil, registry, KernelModeMutation); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("NewKernel(mutation without queue pool) error = %v", err)
	}
	mutation, err := NewKernel(domainPool, queuePool, registry, KernelModeMutation)
	if err != nil || mutation.begin == nil {
		t.Fatalf("NewKernel(mutation) = %#v, %v", mutation, err)
	}
}

func TestKernelShadowIsReadOnlyAndNeverBeginsTransaction(t *testing.T) {
	stepper := &kernelStepper{}
	kernel, err := newKernel(testRegistry(t, ""), KernelModeShadow, stepper, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := kernel.Step(context.Background(), time.Now(), 1, time.Minute, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != KernelModeShadow || stepper.calls != 1 || result.Claimed != 0 || result.Dispatched != 0 {
		t.Fatalf("shadow result = %#v, calls = %d", result, stepper.calls)
	}
}

func TestKernelMutationWithCeleryOnlyContractNeverBeginsATransaction(t *testing.T) {
	called := false
	kernel, err := newKernel(
		testRegistry(t, ""), KernelModeMutation, &kernelStepper{},
		func(context.Context) (pgx.Tx, error) {
			called = true
			return nil, errors.New("must not begin")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := kernel.Step(context.Background(), time.Now(), 1, time.Minute, nil, nil)
	if err != nil || called || result.Mode != KernelModeMutation || result.Claimed != 0 {
		t.Fatalf("celery-only mutation result = %#v err=%v begin=%t", result, err, called)
	}
}

func TestKernelMutationClaimsOnlyBoundedPersistedRiverRoutesInDeterministicOrder(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 1, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindFinalizeSyncRun, now, candidateID2),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now.Add(-time.Second), candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.KindFinalizeSyncRun),
		KernelModeMutation,
		&kernelStepper{},
		func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	var published []string
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(_ context.Context, gotTx pgx.Tx, claim TransportClaim) (string, error) {
			if gotTx != tx {
				t.Fatal("publisher received a different transaction")
			}
			published = append(published, claim.ID)
			return "river-" + claim.ID, nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(published, []string{candidateID1, candidateID2}) ||
		result.Claimed != 2 || result.Dispatched != 2 || !tx.commit {
		t.Fatalf("result = %#v published = %v tx = %#v", result, published, tx)
	}
	upper := strings.ToUpper(tx.querySQL)
	for _, required := range []string{
		"ROUTE.TRANSPORT = 'RIVER'", "ROUTE.PAUSED = FALSE",
		"OUTBOX.KIND = ANY($4::TEXT[])", "ORDER BY OUTBOX.AVAILABLE_AT, OUTBOX.ID",
		"FOR UPDATE OF OUTBOX SKIP LOCKED", "LIMIT $2",
		"SET CLAIM_TOKEN = GEN_RANDOM_UUID()::TEXT",
	} {
		if !strings.Contains(upper, required) {
			t.Fatalf("claim SQL missing %q:\n%s", required, tx.querySQL)
		}
	}
	if strings.Contains(upper, "FOR UPDATE OF ROUTE") ||
		strings.Contains(upper, "FOR SHARE OF ROUTE") ||
		strings.Contains(upper, "FOR KEY SHARE OF ROUTE") {
		t.Fatalf("claim SQL requires forbidden route mutation privilege:\n%s", tx.querySQL)
	}
	if len(tx.queryArgs) != 4 || !reflect.DeepEqual(tx.queryArgs[3], []string{
		syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.KindFinalizeSyncRun,
	}) {
		t.Fatalf("claim arguments = %#v", tx.queryArgs)
	}
	if len(tx.execSQL) != 2 || !strings.Contains(strings.ToUpper(tx.execSQL[0]), "ROUTE.GENERATION = OUTBOX.CLAIM_ROUTE_GENERATION") {
		t.Fatalf("terminal SQL = %v", tx.execSQL)
	}
	if strings.Contains(strings.ToUpper(markRiverDispatchedSQL), "CLAIM_TOKEN = $2::UUID") {
		t.Fatalf("terminal SQL wrongly treats the text claim token as UUID:\n%s", markRiverDispatchedSQL)
	}
}

func TestKernelMutationPublisherFailureRollsBackClaimAndRiverInsertForRetry(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 1, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	publisherErr := errors.New("river insert failed")
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", publisherErr }, nil); !errors.Is(err, publisherErr) {
		t.Fatalf("Step() error = %v", err)
	}
	if tx.commit || !tx.rollback || len(tx.execSQL) != 0 {
		t.Fatalf("failed publish committed or terminalized: %#v", tx)
	}
}

func TestKernelMutationGenerationRecheckRejectsTerminalWriteAndRollsBack(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 0, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published = true
			return "river-job", nil
		}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Step() error = %v", err)
	}
	if !published || tx.commit || !tx.rollback {
		t.Fatalf("generation recheck result = published:%t tx:%#v", published, tx)
	}
}

func TestKernelPostSyncMarksBeforeItsSeparateTransactionLocalSeam(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 1, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	marked := false
	result, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published = true
			return "", nil
		}, func(_ context.Context, gotTx pgx.Tx, claim TransportClaim) error {
			if gotTx != tx || claim.Kind != syncdispatchcontract.KindPostSync || len(tx.execSQL) != 1 {
				t.Fatalf("post-sync seam order or transaction invalid: tx=%#v claim=%#v", tx, claim)
			}
			marked = true
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if published || !marked || !tx.commit || result.PostSyncMark != 1 || result.Dispatched != 1 {
		t.Fatalf("post-sync result = %#v published:%t marked:%t tx:%#v", result, published, marked, tx)
	}
}

func TestKernelPostSyncMarkerFailureRollsBackItsPriorTerminalMark(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 1, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	markerErr := errors.New("mark-before seam failed")
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute, nil,
		func(context.Context, pgx.Tx, TransportClaim) error { return markerErr }); !errors.Is(err, markerErr) {
		t.Fatalf("Step() error = %v", err)
	}
	if tx.commit || !tx.rollback || len(tx.execSQL) != 1 {
		t.Fatalf("post-sync marker failure leaked terminal write: %#v", tx)
	}
}

func TestKernelRejectsInvalidModesAndMissingMutationSeams(t *testing.T) {
	if _, err := newKernel(testRegistry(t, ""), "active", &kernelStepper{}, nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid mode error = %v", err)
	}
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeKernelTx{terminalRows: 1, claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
	}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) { return tx, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute, nil, nil); !errors.Is(err, ErrPublisherRequired) {
		t.Fatalf("missing publisher error = %v", err)
	}
	if tx.commit || !tx.rollback {
		t.Fatalf("missing publisher transaction = %#v", tx)
	}
}
