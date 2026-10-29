//go:build integration

package revisionfeed

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Real Valkey and PostgreSQL cells of the revision feed (CHAOS-10983 PR B plan section 5). The lane vets them; the lead
// runs them.

func startValkeyClient(t *testing.T, ctx context.Context) (Client, valkeygo.Client, *containers.Instance) {
	t.Helper()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	raw, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	return NewClient(raw), raw, instance
}

func outboxEventAt(aggregateID string, revision int64) audit.Event {
	key := fmt.Sprintf("organization:%s:%d", aggregateID, revision)
	return audit.Event{
		ID: revision, AggregateType: "organization", AggregateID: aggregateID, EventType: "organization.updated",
		Payload:        []byte(fmt.Sprintf(`{"revision":%d,"change":{}}`, revision)),
		IdempotencyKey: key, CreatedAt: time.Now().UTC(),
	}
}

type collector struct {
	mu       sync.Mutex
	messages []string
	subbed   chan struct{}
}

func subscribeCollect(t *testing.T, ctx context.Context, client Client) *collector {
	t.Helper()
	c := &collector{subbed: make(chan struct{}, 1)}
	go func() {
		_ = client.Subscribe(ctx, Channel, SubscribeHooks{
			OnSubscribed: func() {
				select {
				case c.subbed <- struct{}{}:
				default:
				}
			},
			OnMessage: func(m string) {
				c.mu.Lock()
				c.messages = append(c.messages, m)
				c.mu.Unlock()
			},
		})
	}()
	select {
	case <-c.subbed:
	case <-time.After(15 * time.Second):
		t.Fatal("the subscription was not confirmed")
	}
	return c
}

func (c *collector) waitFor(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.messages) >= n {
			out := append([]string(nil), c.messages...)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("%d messages, want %d: %v", len(c.messages), n, c.messages)
	return nil
}

// The script on a real Valkey: a lower or repeated revision never lowers the hash, and every delivery is published.
func TestTheScriptNeverLowersTheHighWaterAndPublishesEveryDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, _, _ := startValkeyClient(t, ctx)
	sink, err := NewSink(client)
	if err != nil {
		t.Fatal(err)
	}
	subCtx, stop := context.WithCancel(ctx)
	defer stop()
	got := subscribeCollect(t, subCtx, client)
	const id = "org_AAAAAAAAAAAAAAAAAAAAAA"
	for _, r := range []int64{1, 5, 3, 5, 6} {
		if err := sink.Deliver(ctx, outboxEventAt(id, r)); err != nil {
			t.Fatalf("Deliver r%d: %v", r, err)
		}
	}
	floors, err := PublishedFloors(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if floors["organization:"+id] != 6 || len(floors) != 1 {
		t.Fatalf("floors %v, want organization:%s = 6", floors, id)
	}
	messages := got.waitFor(t, 5)
	want := []string{"1", "5", "3", "5", "6"}
	for i, m := range messages[:5] {
		if m != "organization:"+id+":"+want[i] {
			t.Fatalf("message %d = %q", i, m)
		}
	}
	ttl, err := client.TTL(ctx, HighWaterKey)
	if err != nil || ttl != -1 {
		t.Fatalf("ttl of the hash = %d, %v, want -1 (no expiry)", ttl, err)
	}
}

// Every replica subscribes: two subscribers each see every message (fan-out), and a Consumer on each ends at the same marks.
func TestTwoSubscribersEachSeeEveryEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, _, _ := startValkeyClient(t, ctx)
	sink, _ := NewSink(client)
	type replica struct {
		consumer *Consumer
		rec      *recorder
	}
	replicas := make([]replica, 2)
	for i := range replicas {
		rec := &recorder{}
		consumer, err := NewConsumer(Config{Invalidator: rec})
		if err != nil {
			t.Fatal(err)
		}
		sub, err := NewSubscriber(SubscriberConfig{Client: client, Consumer: consumer, Backoff: func(int) time.Duration { return 10 * time.Millisecond }})
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		go sub.Run(runCtx)
		replicas[i] = replica{consumer: consumer, rec: rec}
	}
	deadline := time.Now().Add(15 * time.Second)
	for _, r := range replicas {
		for countAll(r.rec) < 1 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if countAll(r.rec) < 1 {
			t.Fatal("a replica never confirmed its subscription")
		}
	}
	const id = "org_BBBBBBBBBBBBBBBBBBBBBB"
	for r := int64(1); r <= 4; r++ {
		if err := sink.Deliver(ctx, outboxEventAt(id, r)); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range replicas {
		for time.Now().Before(deadline) {
			if got, _ := r.consumer.Watermarks().Latest("organization", id); got == 4 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if got, _ := r.consumer.Watermarks().Latest("organization", id); got != 4 {
			t.Fatalf("replica %d ends at revision %d, want 4", i, got)
		}
	}
}

// The login the chart's ACL file defines can run the sink and the subscriber, and reaches nothing outside the prefixes.
func TestTheAuthAclLoginRunsTheFeedAndNothingElse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const password = "feed-acl-test-password"
	instance, err := containers.StartValkeyWithACLFile(ctx, ratelimitvalkey.ACLFile(password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = instance.Close(closeCtx)
	})
	login, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	login.User = url.UserPassword(ratelimitvalkey.ACLUser, password)
	raw, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(login.String()))
	if err != nil {
		t.Fatalf("the ACL login cannot connect: %v", err)
	}
	t.Cleanup(raw.Close)
	client := NewClient(raw)
	sink, _ := NewSink(client)
	subCtx, stop := context.WithCancel(ctx)
	defer stop()
	got := subscribeCollect(t, subCtx, client)
	const id = "org_CCCCCCCCCCCCCCCCCCCCCC"
	if err := sink.Deliver(ctx, outboxEventAt(id, 1)); err != nil {
		t.Fatalf("the sink under the ACL login: %v", err)
	}
	got.waitFor(t, 1)
	// outside the channel pattern and outside the key prefix: refused
	if err := raw.Do(ctx, raw.B().Publish().Channel("other_channel").Message("x").Build()).Error(); err == nil {
		t.Fatal("the login published outside &auth_svc_*")
	}
	if err := raw.Do(ctx, raw.B().Hset().Key("some:other:hash").FieldValue().FieldValue("a", "1").Build()).Error(); err == nil {
		t.Fatal("the login wrote a key outside ~dho:rl:auth_svc_*")
	}
	if err := raw.Do(ctx, raw.B().Del().Key(HighWaterKey).Build()).Error(); err == nil {
		t.Fatal("the login deleted the hash")
	}
}

// --- PostgreSQL + Valkey: the outbox publisher through the sink ---

type feedFixture struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
}

func newFeedFixture(t *testing.T, ctx context.Context) *feedFixture {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	role, err := containers.RoleName("revisionfeed_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %q", role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(pool, role, t.Logf) })
	if _, err := authschema.Apply(ctx, pool, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatal(err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return &feedFixture{pool: pool, schema: schema}
}

func (f *feedFixture) commit(t *testing.T, ctx context.Context, aggregate string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		slug := fmt.Sprintf("%s-%s", aggregate, uuid.NewString())
		err := audit.Commit(ctx, f.pool, f.schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $1)`, slug)
				return err
			},
			Audit:  audit.AuditEvent{EventType: "organization.updated", Outcome: audit.OutcomeAllowed},
			Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: aggregate, EventType: "organization.updated"}},
		})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
}

func (f *feedFixture) publisher(t *testing.T, sink audit.Sink) *audit.Publisher {
	t.Helper()
	p, err := audit.NewPublisher(f.pool, sink, audit.PublisherConfig{Schema: f.schema, BaseBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (f *feedFixture) pending(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// countingSink wraps a sink and counts deliveries per idempotency key.
type countingSink struct {
	inner audit.Sink
	mu    sync.Mutex
	seen  map[string]int
	twice bool
}

func (c *countingSink) Deliver(ctx context.Context, e audit.Event) error {
	if err := c.inner.Deliver(ctx, e); err != nil {
		return err
	}
	if c.twice {
		if err := c.inner.Deliver(ctx, e); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.seen[e.IdempotencyKey]++
	c.mu.Unlock()
	return nil
}

func orgWire() string { return "org_" + uuid.NewString() }

// Valkey down, then up: events stay pending (nothing is marked published), the reaper never deletes them, and after Valkey
// answers they drain and the hash holds the max.
func TestEventsStayPendingWhileValkeyIsDownThenDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := newFeedFixture(t, ctx)
	client, _, _ := startValkeyClient(t, ctx)
	down := &switchClient{Client: client, down: true}
	sink, _ := NewSink(down)
	publisher := fixture.publisher(t, sink)
	aggregate := orgWire()
	fixture.commit(t, ctx, aggregate, 3)

	now := time.Now()
	result, err := publisher.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("Step with Valkey down: %v (a down consumer must not fail the step)", err)
	}
	if result.Delivered != 0 || result.Failed == 0 {
		t.Fatalf("step %+v, want failures and no delivery", result)
	}
	if n := fixture.pending(t, ctx); n != 3 {
		t.Fatalf("%d pending, want 3", n)
	}
	var lastError *string
	if err := fixture.pool.QueryRow(ctx, `SELECT last_error FROM auth.auth_outbox_events ORDER BY id LIMIT 1`).Scan(&lastError); err != nil || lastError == nil || *lastError != "consumer_unavailable" {
		t.Fatalf("last_error = %v, %v, want consumer_unavailable", lastError, err)
	}
	if deleted, err := audit.Reap(ctx, fixture.pool, fixture.schema, now.Add(24*time.Hour), 100); err != nil || deleted != 0 {
		t.Fatalf("the reaper deleted %d unpublished rows (%v)", deleted, err)
	}
	if n := fixture.pending(t, ctx); n != 3 {
		t.Fatalf("%d pending after the reaper, want 3", n)
	}

	down.setDown(false)
	later := now.Add(time.Hour)
	for i := 0; i < 5 && fixture.pending(t, ctx) > 0; i++ {
		if _, err := publisher.Step(ctx, later, 10); err != nil {
			t.Fatal(err)
		}
	}
	if n := fixture.pending(t, ctx); n != 0 {
		t.Fatalf("%d still pending after Valkey came back", n)
	}
	floors, err := PublishedFloors(ctx, client)
	if err != nil || floors["organization:"+aggregate] != 3 {
		t.Fatalf("floors %v, %v, want organization:%s = 3", floors, err, aggregate)
	}
}

type switchClient struct {
	Client
	mu   sync.Mutex
	down bool
}

func (s *switchClient) setDown(v bool) {
	s.mu.Lock()
	s.down = v
	s.mu.Unlock()
}

func (s *switchClient) EvalInt(ctx context.Context, script string, keys, args []string) (int64, error) {
	s.mu.Lock()
	down := s.down
	s.mu.Unlock()
	if down {
		return 0, ErrUnavailable
	}
	return s.Client.EvalInt(ctx, script, keys, args)
}

// At-least-once: a sink that delivers every event twice (the redelivery after a mark that never committed) leaves the
// hash at the max and a Consumer that ignores the second copy.
func TestARedeliveredEventIsIgnoredByTheConsumerAndLeavesTheHashAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := newFeedFixture(t, ctx)
	client, _, _ := startValkeyClient(t, ctx)
	sink, _ := NewSink(client)
	counting := &countingSink{inner: sink, seen: map[string]int{}, twice: true}
	publisher := fixture.publisher(t, counting)
	aggregate := orgWire()
	fixture.commit(t, ctx, aggregate, 4)

	rec := &recorder{}
	consumer, _ := NewConsumer(Config{Invalidator: rec})
	subCtx, stop := context.WithCancel(ctx)
	defer stop()
	got := subscribeCollect(t, subCtx, client)
	if _, err := publisher.Step(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	messages := got.waitFor(t, 8)
	for _, m := range messages {
		typ, id, revision, ok := parseMessage(m)
		if !ok {
			t.Fatalf("unparseable message %q", m)
		}
		consumer.Handle(ctx, contracts.ProvisioningEvent{AggregateType: typ, AggregateID: id, Payload: contracts.ProvisioningPayload{Revision: contracts.Revision(revision)}})
	}
	invalidations := 0
	for _, c := range rec.snapshot() {
		if !c.all {
			invalidations++
		}
	}
	if invalidations != 4 {
		t.Fatalf("%d invalidations for 4 events delivered twice, want 4 (the second copies are ignored)", invalidations)
	}
	floors, _ := PublishedFloors(ctx, client)
	if floors["organization:"+aggregate] != 4 {
		t.Fatalf("floors %v", floors)
	}
}

// Restart mid-batch: a first publisher delivers part of the batch and stops; a second finishes. No event is lost, none is
// delivered twice, and the hash holds the max.
func TestARestartMidBatchLosesNothingAndDeliversNothingTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := newFeedFixture(t, ctx)
	client, _, _ := startValkeyClient(t, ctx)
	sink, _ := NewSink(client)
	counting := &countingSink{inner: sink, seen: map[string]int{}}
	aggregate := orgWire()
	fixture.commit(t, ctx, aggregate, 6)

	first := fixture.publisher(t, counting)
	if _, err := first.Step(ctx, time.Now(), 2); err != nil {
		t.Fatal(err)
	}
	if n := fixture.pending(t, ctx); n != 4 {
		t.Fatalf("%d pending after the first publisher, want 4", n)
	}
	second := fixture.publisher(t, counting)
	if _, err := second.Step(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	if n := fixture.pending(t, ctx); n != 0 {
		t.Fatalf("%d pending after the second publisher", n)
	}
	counting.mu.Lock()
	defer counting.mu.Unlock()
	if len(counting.seen) != 6 {
		t.Fatalf("%d distinct events delivered, want 6", len(counting.seen))
	}
	for key, n := range counting.seen {
		if n != 1 {
			t.Fatalf("%s delivered %d times", key, n)
		}
	}
	floors, _ := PublishedFloors(ctx, client)
	if floors["organization:"+aggregate] != 6 {
		t.Fatalf("floors %v", floors)
	}
}

// Two publishers (two replicas) drain one outbox concurrently (FOR UPDATE SKIP LOCKED) and two consumers each see every
// event through the channel.
func TestTwoPublishersAndTwoConsumersSeeEveryEventOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := newFeedFixture(t, ctx)
	client, _, _ := startValkeyClient(t, ctx)
	sink, _ := NewSink(client)
	counting := &countingSink{inner: sink, seen: map[string]int{}}
	aggregates := []string{orgWire(), orgWire(), orgWire()}
	for _, a := range aggregates {
		fixture.commit(t, ctx, a, 4)
	}
	subCtx, stop := context.WithCancel(ctx)
	defer stop()
	one := subscribeCollect(t, subCtx, client)
	two := subscribeCollect(t, subCtx, client)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := fixture.publisher(t, counting)
			for fixture.pending(t, ctx) > 0 {
				if _, err := p.Step(ctx, time.Now(), 5); err != nil {
					t.Errorf("Step: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	counting.mu.Lock()
	for key, n := range counting.seen {
		if n != 1 {
			t.Errorf("%s delivered %d times by the two publishers", key, n)
		}
	}
	if len(counting.seen) != 12 {
		t.Errorf("%d events delivered, want 12", len(counting.seen))
	}
	counting.mu.Unlock()
	for name, c := range map[string]*collector{"first": one, "second": two} {
		if got := len(c.waitFor(t, 12)); got < 12 {
			t.Errorf("the %s consumer saw %d messages, want 12", name, got)
		}
	}
	floors, _ := PublishedFloors(ctx, client)
	for _, a := range aggregates {
		if floors["organization:"+a] != 4 {
			t.Errorf("floors %v, want organization:%s = 4", floors, a)
		}
	}
}
