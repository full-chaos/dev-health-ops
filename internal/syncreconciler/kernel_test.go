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
	name         string
	events       *[]string
	claims       []TransportClaim
	querySQL     string
	queryArgs    []any
	execSQL      []string
	execArgs     [][]any
	execRows     []int64
	execErr      error
	execErrs     []error
	commitErr    error
	commit       bool
	rollback     int
	terminalRows int64
}

func (tx *fakeKernelTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.record("query")
	tx.querySQL = sql
	tx.queryArgs = args
	return &fakeKernelRows{claims: append([]TransportClaim(nil), tx.claims...)}, nil
}

func (tx *fakeKernelTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.record("exec")
	tx.execSQL = append(tx.execSQL, sql)
	tx.execArgs = append(tx.execArgs, append([]any(nil), args...))
	rows := tx.terminalRows
	if index := len(tx.execSQL) - 1; index < len(tx.execRows) {
		rows = tx.execRows[index]
	}
	err := tx.execErr
	if index := len(tx.execSQL) - 1; index < len(tx.execErrs) {
		err = tx.execErrs[index]
	}
	return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(rows, 10)), err
}

func (tx *fakeKernelTx) Commit(context.Context) error {
	tx.record("commit")
	tx.commit = tx.commitErr == nil
	return tx.commitErr
}

func (tx *fakeKernelTx) Rollback(context.Context) error {
	tx.record("rollback")
	tx.rollback++
	return nil
}

func (tx *fakeKernelTx) record(event string) {
	if tx.events != nil {
		*tx.events = append(*tx.events, tx.name+":"+event)
	}
}

func kernelBeginSequence(t *testing.T, events *[]string, transactions ...*fakeKernelTx) beginFunc {
	t.Helper()
	index := 0
	return func(context.Context) (pgx.Tx, error) {
		if index >= len(transactions) {
			t.Fatalf("unexpected transaction begin %d", index+1)
		}
		tx := transactions[index]
		index++
		tx.events = events
		if tx.name == "" {
			tx.name = "tx" + strconv.Itoa(index)
		}
		if events != nil {
			*events = append(*events, tx.name+":begin")
		}
		return tx, nil
	}
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
	events := []string{}
	claimTx := &fakeKernelTx{name: "claim", claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindFinalizeSyncRun, now, candidateID2),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now.Add(-time.Second), candidateID1),
	}}
	firstDelivery := &fakeKernelTx{name: "delivery-1", terminalRows: 1}
	secondDelivery := &fakeKernelTx{name: "delivery-2", terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.KindFinalizeSyncRun),
		KernelModeMutation,
		&kernelStepper{},
		kernelBeginSequence(t, &events, claimTx, firstDelivery, secondDelivery),
	)
	if err != nil {
		t.Fatal(err)
	}
	var published []string
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(_ context.Context, gotTx pgx.Tx, claim TransportClaim) (string, error) {
			if !claimTx.commit {
				t.Fatal("publisher ran before the bounded claims committed")
			}
			wantTx := pgx.Tx(firstDelivery)
			if len(published) == 1 {
				wantTx = secondDelivery
			}
			if gotTx != wantTx {
				t.Fatalf("publisher transaction = %T/%p, want %p", gotTx, gotTx, wantTx)
			}
			published = append(published, claim.ID)
			return "river-" + claim.ID, nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(published, []string{candidateID1, candidateID2}) ||
		result.Claimed != 2 || result.Dispatched != 2 ||
		!claimTx.commit || !firstDelivery.commit || !secondDelivery.commit {
		t.Fatalf("result = %#v published = %v events = %v", result, published, events)
	}
	if got, want := events[:4], []string{"claim:begin", "claim:query", "claim:commit", "claim:rollback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("claim ordering = %v, want prefix %v", events, want)
	}
	upper := strings.ToUpper(claimTx.querySQL)
	for _, required := range []string{
		"ROUTE.TRANSPORT = 'RIVER'", "ROUTE.PAUSED = FALSE",
		"OUTBOX.KIND = ANY($4::TEXT[])", "ORDER BY OUTBOX.AVAILABLE_AT, OUTBOX.ID",
		"FOR UPDATE OF OUTBOX SKIP LOCKED", "LIMIT $2",
		"SET CLAIM_TOKEN = GEN_RANDOM_UUID()::TEXT",
	} {
		if !strings.Contains(upper, required) {
			t.Fatalf("claim SQL missing %q:\n%s", required, claimTx.querySQL)
		}
	}
	if strings.Contains(upper, "FOR UPDATE OF ROUTE") ||
		strings.Contains(upper, "FOR SHARE OF ROUTE") ||
		strings.Contains(upper, "FOR KEY SHARE OF ROUTE") {
		t.Fatalf("claim SQL requires forbidden route mutation privilege:\n%s", claimTx.querySQL)
	}
	if len(claimTx.queryArgs) != 4 || !reflect.DeepEqual(claimTx.queryArgs[3], []string{
		syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.KindFinalizeSyncRun,
	}) {
		t.Fatalf("claim arguments = %#v", claimTx.queryArgs)
	}
	if len(firstDelivery.execSQL) != 2 || len(secondDelivery.execSQL) != 2 ||
		!strings.Contains(strings.ToUpper(firstDelivery.execSQL[0]), "FOR UPDATE OF OUTBOX") ||
		!strings.Contains(strings.ToUpper(firstDelivery.execSQL[1]), "ROUTE.GENERATION = OUTBOX.CLAIM_ROUTE_GENERATION") {
		t.Fatalf("terminal SQL = %v / %v", firstDelivery.execSQL, secondDelivery.execSQL)
	}
	if strings.Contains(strings.ToUpper(markRiverDispatchedSQL), "CLAIM_TOKEN = $2::UUID") {
		t.Fatalf("terminal SQL wrongly treats the text claim token as UUID:\n%s", markRiverDispatchedSQL)
	}
}

func TestKernelMutationPublisherFailureRecordsCommittedClaimBackoffAndContinues(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	events := []string{}
	claimTx := &fakeKernelTx{name: "claim", claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now.Add(time.Second), candidateID2),
	}}
	failedDelivery := &fakeKernelTx{name: "delivery-1", terminalRows: 1}
	backoffTx := &fakeKernelTx{name: "backoff", terminalRows: 1}
	successfulDelivery := &fakeKernelTx{name: "delivery-2", terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{},
		kernelBeginSequence(t, &events, claimTx, failedDelivery, backoffTx, successfulDelivery),
	)
	if err != nil {
		t.Fatal(err)
	}
	publisherErr := errors.New("river insert failed")
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(_ context.Context, _ pgx.Tx, claim TransportClaim) (string, error) {
			if claim.ID == candidateID1 {
				return "", publisherErr
			}
			return "river-" + claim.ID, nil
		}, nil)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	if result.Claimed != 2 || result.Retried != 1 || result.Dispatched != 1 ||
		!claimTx.commit || failedDelivery.commit || failedDelivery.rollback == 0 ||
		!backoffTx.commit || !successfulDelivery.commit {
		t.Fatalf("result = %#v events = %v", result, events)
	}
	if len(backoffTx.execArgs) != 1 ||
		backoffTx.execArgs[0][0] != candidateID1 ||
		backoffTx.execArgs[0][1] != kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1).ClaimToken {
		t.Fatalf("backoff CAS args = %#v", backoffTx.execArgs)
	}
}

func TestKernelMutationFailureRecorderOutageStopsRemainingDelivery(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID2),
	}}
	failedDelivery := &fakeKernelTx{terminalRows: 1}
	backoffTx := &fakeKernelTx{
		terminalRows: 1,
		execErr:      errors.New("failure recorder database outage"),
	}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{},
		kernelBeginSequence(t, nil, claimTx, failedDelivery, backoffTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := 0
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published++
			return "", errors.New("river insert failed")
		}, nil)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v, want ErrUnavailable", err)
	}
	if published != 1 || result.Dispatched != 0 || result.Retried != 0 {
		t.Fatalf("recorder outage continued delivery: result=%#v published=%d", result, published)
	}
}

func TestKernelMutationGenerationRecheckRejectsTerminalWriteAndRollsBack(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
	}}
	deliveryTx := &fakeKernelTx{terminalRows: 0}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, deliveryTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	result, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published = true
			return "river-job", nil
		}, nil)
	if err != nil || result.LeaseLost != 1 {
		t.Fatalf("Step() = %#v, %v", result, err)
	}
	if published || !claimTx.commit || deliveryTx.commit || deliveryTx.rollback == 0 {
		t.Fatalf("generation recheck result = published:%t claim:%#v delivery:%#v", published, claimTx, deliveryTx)
	}
}

func TestKernelMutationLeaseLossContinuesAlreadyClaimedAtLeastOnceBatch(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now.Add(time.Second), candidateID2),
	}}
	staleDelivery := &fakeKernelTx{execRows: []int64{1, 0}}
	successfulDelivery := &fakeKernelTx{terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, staleDelivery, successfulDelivery),
	)
	if err != nil {
		t.Fatal(err)
	}
	var published []string
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(_ context.Context, _ pgx.Tx, claim TransportClaim) (string, error) {
			published = append(published, claim.ID)
			return "river-" + claim.ID, nil
		}, nil)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	if result.Claimed != 2 || result.LeaseLost != 1 || result.Dispatched != 1 ||
		!reflect.DeepEqual(published, []string{candidateID1, candidateID2}) ||
		staleDelivery.commit || staleDelivery.rollback == 0 || !successfulDelivery.commit {
		t.Fatalf("stale lease did not continue batch: result=%#v published=%v stale=%#v successful=%#v", result, published, staleDelivery, successfulDelivery)
	}
}

func TestKernelMutationUnavailableDeliveryDoesNotCountLeaseLoss(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name          string
		delivery      *fakeKernelTx
		wantPublished int
	}{
		{
			name:          "lock query unavailable",
			delivery:      &fakeKernelTx{terminalRows: 1, execErrs: []error{errors.New("lock database unavailable")}},
			wantPublished: 0,
		},
		{
			name:          "terminal update unavailable",
			delivery:      &fakeKernelTx{terminalRows: 1, execErrs: []error{nil, errors.New("terminal database unavailable")}},
			wantPublished: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			claimTx := &fakeKernelTx{claims: []TransportClaim{
				kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
			}}
			kernel, err := newKernel(
				riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
				&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, test.delivery),
			)
			if err != nil {
				t.Fatal(err)
			}
			published := 0
			result, err := kernel.Step(context.Background(), now, 1, time.Minute,
				func(context.Context, pgx.Tx, TransportClaim) (string, error) {
					published++
					return "river-job", nil
				}, nil)
			if !errors.Is(err, ErrUnavailable) || result.LeaseLost != 0 ||
				result.Dispatched != 0 || published != test.wantPublished {
				t.Fatalf("Step() = %#v, %v; published=%d", result, err, published)
			}
		})
	}
}

func TestKernelMutationClaimCommitFailurePublishesNothing(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{
		claims: []TransportClaim{
			kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
		},
		commitErr: errors.New("claim commit outcome unknown"),
	}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published = true
			return "", nil
		}, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v, want ErrUnavailable", err)
	}
	if published {
		t.Fatal("publisher ran after the bounded claim commit failed")
	}
}

func TestKernelMutationDeliveryCommitAmbiguityFailsClosedAfterCASProbe(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name            string
		backoffRows     int64
		wantLeaseLost   bool
		wantBackoffDone bool
	}{
		{
			name:            "CAS proves terminal transaction did not commit",
			backoffRows:     1,
			wantBackoffDone: true,
		},
		{
			name:          "CAS miss leaves terminal outcome ambiguous",
			backoffRows:   0,
			wantLeaseLost: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			claimTx := &fakeKernelTx{claims: []TransportClaim{
				kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1),
				kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID2),
			}}
			deliveryTx := &fakeKernelTx{
				terminalRows: 1,
				commitErr:    errors.New("delivery commit outcome unknown"),
			}
			backoffTx := &fakeKernelTx{terminalRows: test.backoffRows}
			kernel, err := newKernel(
				riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
				&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, deliveryTx, backoffTx),
			)
			if err != nil {
				t.Fatal(err)
			}
			published := 0
			result, err := kernel.Step(context.Background(), now, 2, time.Minute,
				func(context.Context, pgx.Tx, TransportClaim) (string, error) {
					published++
					return "river-job", nil
				}, nil)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Step() error = %v, want ErrUnavailable", err)
			}
			if errors.Is(err, ErrLeaseLost) != test.wantLeaseLost {
				t.Fatalf("Step() lease-lost classification = %v, want %t", err, test.wantLeaseLost)
			}
			if published != 1 || result.Dispatched != 0 || result.Retried != 0 {
				t.Fatalf("ambiguous commit continued delivery: result=%#v published=%d", result, published)
			}
			if backoffTx.commit != test.wantBackoffDone {
				t.Fatalf("backoff commit = %t, want %t", backoffTx.commit, test.wantBackoffDone)
			}
		})
	}
}

func TestKernelMutationUnknownClaimDescriptorFailsClosedAfterClaimCommit(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claim := kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now, candidateID1)
	claim.Kind = "unknown_kind"
	claimTx := &fakeKernelTx{claims: []TransportClaim{claim}}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			t.Fatal("unknown descriptor reached publisher")
			return "", nil
		}, nil); !errors.Is(err, ErrLeaseLost) || result.Claimed != 1 {
		t.Fatalf("Step() = %#v, %v", result, err)
	}
	if !claimTx.commit {
		t.Fatal("unknown descriptor test did not exercise a durable claim")
	}
}

func TestKernelPostSyncUsesGuardedAtLeastOncePublisher(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{name: "claim", claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
	}}
	deliveryTx := &fakeKernelTx{name: "delivery", terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, deliveryTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	result, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published = true
			return "river-post-sync", nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !published || !claimTx.commit || !deliveryTx.commit || result.Dispatched != 1 {
		t.Fatalf("post-sync result = %#v published:%t claim:%#v delivery:%#v", result, published, claimTx, deliveryTx)
	}
}

func TestKernelPostSyncPublisherFailureRearmsCommittedClaim(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
	}}
	failedDelivery := &fakeKernelTx{terminalRows: 1}
	backoffTx := &fakeKernelTx{terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, failedDelivery, backoffTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := kernel.Step(context.Background(), now, 1, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			return "", errors.New("river insert failed")
		}, nil)
	if err != nil || result.Claimed != 1 || result.Retried != 1 || result.Dispatched != 0 {
		t.Fatalf("Step() error/result = %v / %#v", err, result)
	}
	if !claimTx.commit || failedDelivery.commit || failedDelivery.rollback == 0 || !backoffTx.commit {
		t.Fatalf("post-sync failure did not rearm guarded claim: claim=%#v delivery=%#v backoff=%#v", claimTx, failedDelivery, backoffTx)
	}
}

func TestKernelPostSyncLeaseLossContinuesAlreadyClaimedBatch(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
		kernelClaim(syncdispatchcontract.KindPostSync, now.Add(time.Second), candidateID2),
		kernelClaim(syncdispatchcontract.KindPostSync, now.Add(2*time.Second), candidateID3),
	}}
	staleDelivery := &fakeKernelTx{execRows: []int64{0}}
	firstDelivery := &fakeKernelTx{terminalRows: 1}
	secondDelivery := &fakeKernelTx{terminalRows: 1}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, kernelBeginSequence(t, nil, claimTx, staleDelivery, firstDelivery, secondDelivery),
	)
	if err != nil {
		t.Fatal(err)
	}
	var published []string
	result, err := kernel.Step(context.Background(), now, 3, time.Minute,
		func(_ context.Context, _ pgx.Tx, claim TransportClaim) (string, error) {
			published = append(published, claim.ID)
			return "river-" + claim.ID, nil
		}, nil)
	if err != nil || result.Claimed != 3 || result.LeaseLost != 1 || result.Dispatched != 2 ||
		!reflect.DeepEqual(published, []string{candidateID2, candidateID3}) ||
		staleDelivery.commit || staleDelivery.rollback == 0 || !firstDelivery.commit || !secondDelivery.commit {
		t.Fatalf("post-sync batch result=%#v error=%v published=%v stale=%#v first=%#v second=%#v", result, err, published, staleDelivery, firstDelivery, secondDelivery)
	}
}

func TestKernelPostSyncFailureRecorderOutageStopsBatch(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claimTx := &fakeKernelTx{claims: []TransportClaim{
		kernelClaim(syncdispatchcontract.KindPostSync, now, candidateID1),
		kernelClaim(syncdispatchcontract.KindDispatchSyncRun, now.Add(time.Second), candidateID2),
	}}
	failedDelivery := &fakeKernelTx{terminalRows: 1}
	backoffTx := &fakeKernelTx{
		terminalRows: 1,
		execErr:      errors.New("failure recorder database unavailable"),
	}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync, syncdispatchcontract.KindDispatchSyncRun),
		KernelModeMutation,
		&kernelStepper{},
		kernelBeginSequence(t, nil, claimTx, failedDelivery, backoffTx),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := 0
	result, err := kernel.Step(context.Background(), now, 2, time.Minute,
		func(context.Context, pgx.Tx, TransportClaim) (string, error) {
			published++
			return "", errors.New("river insert failed")
		},
		nil)
	if !errors.Is(err, ErrUnavailable) || published != 1 || result.Retried != 0 || result.Dispatched != 0 {
		t.Fatalf("Step() = %#v, %v", result, err)
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
	began := false
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) {
			began = true
			return tx, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.Step(context.Background(), now, 1, time.Minute, nil, nil); !errors.Is(err, ErrPublisherRequired) {
		t.Fatalf("missing publisher error = %v", err)
	}
	if began || tx.commit || tx.rollback != 0 {
		t.Fatalf("missing publisher transaction = %#v", tx)
	}
	postSync, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation,
		&kernelStepper{}, func(context.Context) (pgx.Tx, error) {
			began = true
			return tx, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postSync.Step(context.Background(), now, 1, time.Minute, nil, nil); !errors.Is(err, ErrPublisherRequired) {
		t.Fatalf("missing post-sync publisher error = %v", err)
	}
	if began {
		t.Fatal("missing post-sync publisher began a claim transaction")
	}
}
