package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestStartRunWrapsUnavailableWithOperationAndUnderlyingError is CHAOS's
// regression for the boot-storm diagnosis gap: "fixed schedule failed /
// work_item_attribution_daily_fanout / remaining metrics durable state is
// unavailable" named neither the failing step nor the pgx error underneath,
// so diagnosing it needed ~15 minutes of grant/SQL replay on a live host
// instead of reading the log line. No live Postgres is needed to trigger
// this: a pool closed before use fails Begin() the same way a connection
// timeout under WORKER_COORDINATOR_DATABASE_MAX_CONNS contention would --
// synchronously, with no network round trip.
func TestStartRunWrapsUnavailableWithOperationAndUnderlyingError(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://user:pass@127.0.0.1:5432/db")
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	pool.Close()

	_, wantErr := pool.Begin(ctx)
	if wantErr == nil {
		t.Fatal("Begin() on a closed pool unexpectedly succeeded")
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	seed := int64(1)
	_, err = store.StartRun(ctx, StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000001",
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "all-teams",
		GenerationSeed: &seed,
		Scopes:         []json.RawMessage{json.RawMessage(`{}`)},
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("StartRun() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if !strings.Contains(err.Error(), "begin run tx") {
		t.Fatalf("StartRun() error = %v, want it to name the failing operation (\"begin run tx\")", err)
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("StartRun() error = %v, want it to carry the underlying pgx error (%v)", err, wantErr)
	}
}

func TestDeterministicRunIDUnambiguouslyEncodesGenerationAndScope(t *testing.T) {
	base := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000119",
		Family:         "capacity",
	}
	left := base
	left.Generation = "a/b"
	left.ScopeKey = "c"
	right := base
	right.Generation = "a"
	right.ScopeKey = "b/c"
	if deterministicRunID(left) == deterministicRunID(right) {
		t.Fatalf("distinct generation/scope tuples collided: left=%#v right=%#v", left, right)
	}
}

// fakeScopeRefusalObserver records every CHAOS-5395 scope-refusal
// observation it receives, so a test can assert the EXACT family/reason
// pair the store reports -- not just that some error was returned.
type fakeScopeRefusalObserver struct {
	calls []struct{ family, reason string }
}

func (f *fakeScopeRefusalObserver) ObserveRemainingMetricsScopeRefused(family, reason string) error {
	f.calls = append(f.calls, struct{ family, reason string }{family, reason})
	return nil
}

// TestScopeRefusalReasonMapsUnknownDORAMetricDistinctlyFromOtherRefusals
// pins scopeRefusalReason's bounded vocabulary: a refusal traceable to
// ErrUnknownDORAMetricName gets its own reason label so an operator can
// distinguish "someone typo'd a metric name" from every other invalid-scope
// shape at the telemetry level, not just via a generic error.
func TestScopeRefusalReasonMapsUnknownDORAMetricDistinctlyFromOtherRefusals(t *testing.T) {
	_, scopeErr := validateFamilyScope("dora", json.RawMessage(
		`{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily","metrics":"lead_time"}`,
	))
	if !errors.Is(scopeErr, ErrUnknownDORAMetricName) {
		t.Fatalf("validateFamilyScope() error = %v, want errors.Is(_, ErrUnknownDORAMetricName)", scopeErr)
	}
	if got := scopeRefusalReason(scopeErr); got != "unknown_dora_metric" {
		t.Fatalf("scopeRefusalReason(%v) = %q, want unknown_dora_metric", scopeErr, got)
	}
	if got := scopeRefusalReason(errors.New("some other invalid scope shape")); got != "invalid_scope" {
		t.Fatalf("scopeRefusalReason(other) = %q, want invalid_scope (the bounded fallback)", got)
	}
}

// TestObserveScopeRefusedReachesTheWiredObserverWithTheExactPair pins the
// PLUMBING CHAOS-5395's telemetry depends on end to end (short of a real
// StartRunTx, which needs a live pool): a typo'd "dora" scope's error,
// mapped through scopeRefusalReason, must reach a wired
// ScopeRefusalObserver as EXACTLY (family, reason) -- not the zeroed-out
// request.Family a naive read of normalizeStartRunRequest's error path
// would produce (see StartRunTx's own "requestedFamily, not request.Family"
// comment for why that distinction is load-bearing).
func TestObserveScopeRefusedReachesTheWiredObserverWithTheExactPair(t *testing.T) {
	_, scopeErr := validateFamilyScope("dora", json.RawMessage(
		`{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily","metrics":"lead_time"}`,
	))
	if scopeErr == nil {
		t.Fatal("expected validateFamilyScope to refuse the typo'd metric name")
	}

	observer := &fakeScopeRefusalObserver{}
	store := &PostgresStore{}
	store.SetScopeRefusalObserver(observer)
	store.observeScopeRefused("dora", scopeRefusalReason(scopeErr))

	if len(observer.calls) != 1 {
		t.Fatalf("observer.calls = %v, want exactly 1 call", observer.calls)
	}
	if observer.calls[0].family != "dora" || observer.calls[0].reason != "unknown_dora_metric" {
		t.Fatalf("observer.calls[0] = %+v, want {family: dora, reason: unknown_dora_metric}", observer.calls[0])
	}
}

// TestNormalizeStartRunRequestRefusesAnUnknownDORAMetricNameEndToEnd is the
// red-first regression at the actual call boundary: a StartRunRequest whose
// "dora" scope names an unregistered metric must be refused by
// normalizeStartRunRequest itself (the function StartRunTx and
// startManualTriggerRun both call), with the refusal traceable to
// ErrUnknownDORAMetricName through the wrapped error chain.
func TestNormalizeStartRunRequestRefusesAnUnknownDORAMetricNameEndToEnd(t *testing.T) {
	orgID := "00000000-0000-4000-8000-000000000120"
	request := StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "test",
		ScopeKey:       "2026-07-23",
		Scopes: []json.RawMessage{json.RawMessage(
			`{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily","metrics":"lead_time"}`,
		)},
	}
	_, err := normalizeStartRunRequest(request)
	if err == nil {
		t.Fatal("normalizeStartRunRequest() = nil error, want a refusal for the unregistered metric name")
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("normalizeStartRunRequest() error = %v, want errors.Is(_, ErrInvalidState) (every existing caller's check)", err)
	}
	if !errors.Is(err, ErrUnknownDORAMetricName) {
		t.Fatalf("normalizeStartRunRequest() error = %v, want errors.Is(_, ErrUnknownDORAMetricName) (the specific reason preserved through the wrap)", err)
	}
	if got := scopeRefusalReason(err); got != "unknown_dora_metric" {
		t.Fatalf("scopeRefusalReason(normalizeStartRunRequest error) = %q, want unknown_dora_metric", got)
	}
}
