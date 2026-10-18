//go:build integration

package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// revisionConsumer is the reference idempotent consumer the Sink contract
// describes: it applies an event only when its revision is newer than the one
// it holds for the aggregate, and de-duplicates by idempotency key.
type revisionConsumer struct {
	mu       sync.Mutex
	state    map[string]int64 // aggregate -> applied revision
	seen     map[string]int
	received []string
	failOnce map[string]bool
}

func newConsumer() *revisionConsumer {
	return &revisionConsumer{state: map[string]int64{}, seen: map[string]int{}, failOnce: map[string]bool{}}
}

func (c *revisionConsumer) Deliver(_ context.Context, e Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failOnce[e.IdempotencyKey] {
		delete(c.failOnce, e.IdempotencyKey)
		return reasonErr{"consumer_unavailable"}
	}
	c.received = append(c.received, e.IdempotencyKey)
	c.seen[e.IdempotencyKey]++
	var p struct{ Revision int64 }
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}
	if p.Revision > c.state[e.AggregateID] {
		c.state[e.AggregateID] = p.Revision
	}
	return nil
}

func commitEvent(t *testing.T, ctx context.Context, env *auditFixture, aggregate string, revision int64) string {
	t.Helper()
	key := fmt.Sprintf("%s-r%d", aggregate, revision)
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $1)`, key)
			return err
		},
		Audit: AuditEvent{EventType: "organization.updated", Outcome: OutcomeAllowed},
		Event: OutboxEvent{
			AggregateType: "organization", AggregateID: aggregate, EventType: "organization.updated",
			Payload: map[string]any{"revision": revision}, IdempotencyKey: key,
		},
	})
	if err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
	return key
}

func newTestPublisher(t *testing.T, env *auditFixture, sink Sink) *Publisher {
	t.Helper()
	p, err := NewPublisher(env.pool, sink, PublisherConfig{
		Schema: env.schema, BaseBackoff: time.Second, MaxBackoff: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

type outboxRow struct {
	published bool
	attempts  int
	lastError *string
	available time.Time
}

func readRow(t *testing.T, ctx context.Context, env *auditFixture, key string) outboxRow {
	t.Helper()
	var r outboxRow
	err := env.pool.QueryRow(ctx, `SELECT published_at IS NOT NULL, attempts, last_error, available_at
		FROM auth.auth_outbox_events WHERE idempotency_key = $1`, key).Scan(&r.published, &r.attempts, &r.lastError, &r.available)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return r
}

// armedExec runs one statement against the guarded outbox the way this
// package's own writers do.
func armedExec(t *testing.T, ctx context.Context, env *auditFixture, sql string, args ...any) {
	t.Helper()
	tx, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := armTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublisherDeliversOldestFirstAndMarksPublishedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	k1 := commitEvent(t, ctx, env, "org-a", 1)
	k2 := commitEvent(t, ctx, env, "org-a", 2)
	k3 := commitEvent(t, ctx, env, "org-b", 1)

	consumer := newConsumer()
	p := newTestPublisher(t, env, consumer)
	res, err := p.Step(ctx, time.Now().Add(time.Second), 10)
	if err != nil || res.Delivered != 3 || res.Failed != 0 {
		t.Fatalf("Step = %+v, %v", res, err)
	}
	if got := fmt.Sprint(consumer.received); got != fmt.Sprint([]string{k1, k2, k3}) {
		t.Errorf("delivery order = %v", consumer.received)
	}
	for _, k := range []string{k1, k2, k3} {
		if r := readRow(t, ctx, env, k); !r.published || r.attempts != 1 || r.lastError != nil {
			t.Errorf("%s row = %+v", k, r)
		}
	}
	res, err = p.Step(ctx, time.Now().Add(time.Second), 10)
	if err != nil || res.Delivered != 0 {
		t.Fatalf("second Step = %+v, %v; a published event was delivered again", res, err)
	}
}

func TestPublisherFailureBacksOffAndStoresNoErrorText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	k := commitEvent(t, ctx, env, "org-a", 1)

	consumer := newConsumer()
	consumer.failOnce[k] = true
	p := newTestPublisher(t, env, consumer)
	now := time.Now().Add(time.Second)
	res, err := p.Step(ctx, now, 10)
	if err != nil || res.Failed != 1 || res.Delivered != 0 {
		t.Fatalf("Step = %+v, %v", res, err)
	}
	r := readRow(t, ctx, env, k)
	if r.published || r.attempts != 1 || r.lastError == nil || *r.lastError != "consumer_unavailable" {
		t.Fatalf("row after failure = %+v", r)
	}
	if !r.available.After(now) {
		t.Errorf("available_at %v is not after now %v: no backoff", r.available, now)
	}
	// Not yet due: nothing is delivered.
	if res, _ := p.Step(ctx, now, 10); res.Delivered+res.Failed != 0 {
		t.Errorf("a not-yet-due event was attempted: %+v", res)
	}
	// Due after the backoff: delivered, error cleared.
	res, err = p.Step(ctx, now.Add(time.Minute), 10)
	if err != nil || res.Delivered != 1 {
		t.Fatalf("retry Step = %+v, %v", res, err)
	}
	if r := readRow(t, ctx, env, k); !r.published || r.attempts != 2 || r.lastError != nil {
		t.Errorf("row after retry = %+v", r)
	}
}

// A crash after the sink accepted the event and before the mark commits must
// leave the event unpublished, so it is delivered again.
func TestPublisherRedeliversWhenTheMarkNeverCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	k := commitEvent(t, ctx, env, "org-a", 1)

	consumer := newConsumer()
	crashCtx, crash := context.WithCancel(ctx)
	crashing := sinkFunc(func(c context.Context, e Event) error {
		err := consumer.Deliver(c, e)
		crash() // the process dies after the consumer accepted the event
		return err
	})
	p := newTestPublisher(t, env, crashing)
	if _, err := p.Step(crashCtx, time.Now().Add(time.Second), 10); err == nil {
		t.Fatal("Step reported success although its mark could not commit")
	}
	if r := readRow(t, ctx, env, k); r.published {
		t.Fatalf("the event was marked published without a committed mark")
	}

	p2 := newTestPublisher(t, env, consumer)
	if res, err := p2.Step(ctx, time.Now().Add(time.Second), 10); err != nil || res.Delivered != 1 {
		t.Fatalf("redelivery Step = %+v, %v", res, err)
	}
	if consumer.seen[k] != 2 {
		t.Fatalf("consumer saw the event %d time(s), want 2 (at least once)", consumer.seen[k])
	}
	if consumer.state["org-a"] != 1 {
		t.Errorf("replay changed consumer state: %v", consumer.state)
	}
}

type sinkFunc func(context.Context, Event) error

func (f sinkFunc) Deliver(ctx context.Context, e Event) error { return f(ctx, e) }

// Out of order: r2 fails first, so r3 is delivered before r2. The consumer
// keeps the newer state, and a replay of r3 changes nothing.
func TestOutOfOrderAndReplayedDeliveryNeverRevertsConsumerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	commitEvent(t, ctx, env, "org-a", 1)
	k2 := commitEvent(t, ctx, env, "org-a", 2)
	k3 := commitEvent(t, ctx, env, "org-a", 3)

	consumer := newConsumer()
	consumer.failOnce[k2] = true
	p := newTestPublisher(t, env, consumer)
	now := time.Now().Add(time.Second)
	if _, err := p.Step(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if got := consumer.state["org-a"]; got != 3 {
		t.Fatalf("state after r1,r3 = %d, want 3", got)
	}
	if _, err := p.Step(ctx, now.Add(time.Minute), 10); err != nil { // r2 arrives late
		t.Fatal(err)
	}
	t.Logf("delivery order: %v; final state: %v", consumer.received, consumer.state)
	if got := consumer.state["org-a"]; got != 3 {
		t.Fatalf("a delayed r2 reverted the state to %d", got)
	}
	order := consumer.received
	if !(indexOf(order, k3) < indexOf(order, k2)) {
		t.Fatalf("test did not produce an out-of-order delivery: %v", order)
	}

	// Replay r3 and r2 again (published_at cleared as a redelivery would leave it).
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET published_at = NULL, available_at = now() WHERE idempotency_key = ANY($1)`, []string{k2, k3})
	if _, err := p.Step(ctx, time.Now().Add(time.Second), 10); err != nil {
		t.Fatal(err)
	}
	if got := consumer.state["org-a"]; got != 3 {
		t.Fatalf("a replay changed the state to %d", got)
	}
	if consumer.seen[k3] != 2 || consumer.seen[k2] != 2 {
		t.Errorf("replay was not delivered: %v", consumer.seen)
	}
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func TestTwoPublishersNeverDeliverTheSameEventConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	const n = 24
	for i := 0; i < n; i++ {
		commitEvent(t, ctx, env, fmt.Sprintf("org-%d", i), 1)
	}
	var mu sync.Mutex
	counts := map[string]int{}
	sink := sinkFunc(func(_ context.Context, e Event) error {
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		counts[e.IdempotencyKey]++
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		p := newTestPublisher(t, env, sink)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Step(ctx, time.Now().Add(time.Second), 100); err != nil {
				t.Errorf("Step: %v", err)
			}
		}()
	}
	wg.Wait()
	keys := make([]string, 0, len(counts))
	for k, c := range counts {
		keys = append(keys, k)
		if c != 1 {
			t.Errorf("%s delivered %d times", k, c)
		}
	}
	sort.Strings(keys)
	if len(keys) != n {
		t.Fatalf("delivered %d distinct events, want %d", len(keys), n)
	}
}

func TestReapDeletesOnlyPublishedEventsOlderThanTheCutoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	oldPublished := commitEvent(t, ctx, env, "a", 1)
	oldUnpublished := commitEvent(t, ctx, env, "b", 1)
	recentPublished := commitEvent(t, ctx, env, "c", 1)
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET published_at = now() - interval '30 days' WHERE idempotency_key = $1`, oldPublished)
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET created_at = now() - interval '90 days', available_at = now() - interval '90 days' WHERE idempotency_key = $1`, oldUnpublished)
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET published_at = now() WHERE idempotency_key = $1`, recentPublished)

	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	n, err := Reap(ctx, env.pool, env.schema, cutoff, 100)
	if err != nil || n != 1 {
		t.Fatalf("Reap = %d, %v; want 1", n, err)
	}
	exists := func(k string) bool {
		var c int
		if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE idempotency_key = $1`, k).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c == 1
	}
	if exists(oldPublished) {
		t.Error("the old published event survived")
	}
	if !exists(oldUnpublished) {
		t.Error("an UNPUBLISHED event was reaped")
	}
	if !exists(recentPublished) {
		t.Error("a recently published event was reaped")
	}
	for _, bad := range []int{0, -1, maxReapLimit + 1} {
		if _, err := Reap(ctx, env.pool, env.schema, cutoff, bad); !errors.Is(err, ErrReapFailed) {
			t.Errorf("limit %d: err = %v", bad, err)
		}
	}
}

// The loop's production stepper reaps. Removing the Reap call from
// outboxStepper.Step turns this test red.
func TestOutboxStepperPublishesThenReaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	stale := commitEvent(t, ctx, env, "a", 1)
	fresh := commitEvent(t, ctx, env, "b", 1)
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET published_at = now() - interval '30 days' WHERE idempotency_key = $1`, stale)

	consumer := newConsumer()
	cfg := DefaultLoopConfig(env.schema, nil)
	s := outboxStepper{pool: env.pool, publisher: newTestPublisher(t, env, consumer), config: cfg}
	if err := s.Step(ctx, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if consumer.seen[fresh] != 1 {
		t.Errorf("the due event was not published: %v", consumer.seen)
	}
	var left int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE idempotency_key = $1`, stale).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Error("the stale published event was not reaped by the loop's stepper")
	}

	// With no publisher the reaper still runs and nothing is published.
	armedExec(t, ctx, env, `UPDATE auth.auth_outbox_events SET published_at = now() - interval '30 days' WHERE idempotency_key = $1`, fresh)
	nopub := outboxStepper{pool: env.pool, config: cfg}
	if err := nopub.Step(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE idempotency_key = $1`, fresh).Scan(&left); err != nil || left != 0 {
		t.Errorf("sink-less stepper did not reap: left=%d err=%v", left, err)
	}
}

// Audit rows written through Commit carry only allowlisted attributes: a
// refused attribute writes nothing at all.
func TestCommitRefusesASecretAttributeAndWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), 'leak', 'leak')`)
			return err
		},
		Audit: AuditEvent{EventType: "leak.probe", Outcome: OutcomeAllowed, Attributes: map[string]any{"refresh_token": "rt_" + strings.Repeat("ab12", 5)}},
		Event: OutboxEvent{AggregateType: "a", AggregateID: "leak", EventType: "leak.probe", IdempotencyKey: "leak-1"},
	})
	if !errors.Is(err, ErrAttributeNotAllowed) {
		t.Fatalf("Commit = %v, want ErrAttributeNotAllowed", err)
	}
	var orgs, audits int
	if err := env.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.organizations WHERE slug = 'leak'), (SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'leak.probe')`).Scan(&orgs, &audits); err != nil {
		t.Fatal(err)
	}
	if orgs != 0 || audits != 0 {
		t.Errorf("rows survived a refused attribute: orgs=%d audits=%d", orgs, audits)
	}
}

func newMeterReader(t *testing.T) (metric.Meter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider.Meter("test"), reader
}

func collected(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// With no sink (the deployed mode) the pending-age gauge must still be
// exported, or a growing backlog is invisible.
func TestNilSinkLoopStillExportsPendingLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	commitEvent(t, ctx, env, "org-a", 1)

	meter, reader := newMeterReader(t)
	cfg := DefaultLoopConfig(env.schema, health.NewRegistry(time.Second))
	cfg.Meter = meter
	loop, err := NewOutboxLoop(env.pool, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.stepper.Step(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Step: %v", err)
	}
	m, ok := collected(t, reader)["devhealth_auth_outbox_oldest_pending_age_seconds"]
	if !ok {
		t.Fatal("the pending-age gauge is absent in nil-sink mode")
	}
	g := m.Data.(metricdata.Gauge[int64])
	if len(g.DataPoints) != 1 || g.DataPoints[0].Value < 3600 {
		t.Fatalf("gauge = %+v, want >= 3600 s for a pending event an hour old", g.DataPoints)
	}
}

// A sink that ignores its context must not hold the row lock, the step, or
// shutdown past DeliverTimeout.
func TestDeliverTimeoutBoundsASinkThatIgnoresItsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	k := commitEvent(t, ctx, env, "org-a", 1)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	blocked := sinkFunc(func(context.Context, Event) error { <-release; return nil })
	p, err := NewPublisher(env.pool, blocked, PublisherConfig{
		Schema: env.schema, BaseBackoff: time.Second, MaxBackoff: time.Minute, DeliverTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := p.Step(ctx, time.Now().Add(time.Second), 10); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Step: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Step stayed blocked past DeliverTimeout on a sink that ignores its context")
	}
	r := readRow(t, ctx, env, k)
	if r.published || r.lastError == nil || *r.lastError != "delivery_timeout" || r.attempts != 1 {
		t.Fatalf("row after a timed-out delivery = %+v (last_error=%v)", r, r.lastError)
	}
}

// Every metric attribute value comes from a closed set.
func TestFailureMetricAttributesAreAClosedSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	const n = 30
	for i := 0; i < n; i++ {
		commitEvent(t, ctx, env, fmt.Sprintf("org-%d", i), 1)
	}
	meter, reader := newMeterReader(t)
	var mu sync.Mutex
	call := 0
	sink := sinkFunc(func(context.Context, Event) error {
		mu.Lock()
		call++
		c := call
		mu.Unlock()
		return reasonErr{fmt.Sprintf("distinct_reason_%d", c)}
	})
	p, err := NewPublisher(env.pool, sink, PublisherConfig{Schema: env.schema, Meter: meter})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Step(ctx, time.Now().Add(time.Second), 100); err != nil {
		t.Fatal(err)
	}
	m, ok := collected(t, reader)["devhealth_auth_outbox_delivery_failures_total"]
	if !ok {
		t.Fatal("no failure series was exported; this control proves nothing")
	}
	enum := map[string]bool{}
	for _, r := range AllFailureReasons() {
		enum[r] = true
	}
	sum := m.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) == 0 {
		t.Fatal("no failure data points")
	}
	for _, dp := range sum.DataPoints {
		v, _ := dp.Attributes.Value("reason")
		if !enum[v.AsString()] {
			t.Errorf("reason attribute %q is outside the closed enum (%d series)", v.AsString(), len(sum.DataPoints))
		}
	}
}

// The retry time is measured from when the failure happened, not from when the
// step started: a slow sink must not turn a one-second backoff into "due now".
func TestBackoffIsMeasuredFromTheFailureNotTheStepStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	k := commitEvent(t, ctx, env, "org-a", 1)

	slow := sinkFunc(func(context.Context, Event) error {
		time.Sleep(1500 * time.Millisecond)
		return reasonErr{"consumer_unavailable"}
	})
	p, err := NewPublisher(env.pool, slow, PublisherConfig{
		Schema: env.schema, BaseBackoff: 2 * time.Second, MaxBackoff: time.Minute, DeliverTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	stepStart := time.Now().Add(time.Second) // due, and the step's notion of "now"
	if _, err := p.Step(ctx, stepStart, 10); err != nil {
		t.Fatal(err)
	}
	failedAt := time.Now()
	r := readRow(t, ctx, env, k)
	if got := r.available.Sub(failedAt); got < time.Second {
		t.Fatalf("available_at is only %v after the failure was recorded; a 2s backoff was measured from the step start", got)
	}
}
