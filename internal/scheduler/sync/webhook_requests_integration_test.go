//go:build integration

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// webhookRequestFixture is the real schema with one org, integration, source
// and configuration a webhook request can point at.
type webhookRequestFixture struct {
	pool                                   *pgxpool.Pool
	org, integrationID, sourceID, configID string
}

func startWebhookRequestFixture(t *testing.T) webhookRequestFixture {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
	fixture := webhookRequestFixture{pool: pool, org: "00000000-0000-4000-8000-00000000a695"}
	now := time.Now().UTC()
	if err := pool.QueryRow(ctx, `
INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'github', 'wsr', '{}', true, $2, $2) RETURNING id::text`, fixture.org, now).Scan(&fixture.integrationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
	is_enabled, discovered_at, last_seen_at)
VALUES (gen_random_uuid(), $1, $2, 'github', 'repository', '42', 'r', 'o/r', '{}', true, $3, $3) RETURNING id::text`,
		fixture.org, fixture.integrationID, now).Scan(&fixture.sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, source_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'wsr', 'github', $2, $3, '[]', '{}', true, false, $4, $4) RETURNING id::text`,
		fixture.org, fixture.integrationID, fixture.sourceID, now).Scan(&fixture.configID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// request writes one pending request and returns its delivery id.
func (fixture webhookRequestFixture) request(t *testing.T, org, mode string, scheduledFor, createdAt time.Time) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO webhook_sync_requests (delivery_id, org_id, sync_config_id, mode, source_ids, scheduled_for, created_at)
VALUES ($1, $2, $3, $4, ARRAY[$5], $6, $7)`, id, org, fixture.configID, mode, fixture.sourceID, scheduledFor, createdAt); err != nil {
		t.Fatal(err)
	}
	return id
}

type requestRow struct {
	exists                 bool
	attempts               int
	nextAttempt, refusedAt *time.Time
	mintedAt               *time.Time
	lastError, refused     *string
	occurrenceID           *string
}

func (fixture webhookRequestFixture) row(t *testing.T, id string) requestRow {
	t.Helper()
	var row requestRow
	err := fixture.pool.QueryRow(context.Background(), `
SELECT attempts, next_attempt_at, last_error, refused_at, refused_reason, minted_at, occurrence_id FROM webhook_sync_requests WHERE delivery_id = $1`, id).
		Scan(&row.attempts, &row.nextAttempt, &row.lastError, &row.refusedAt, &row.refused, &row.mintedAt, &row.occurrenceID)
	if err == pgx.ErrNoRows {
		return row
	}
	if err != nil {
		t.Fatal(err)
	}
	row.exists = true
	return row
}

func (fixture webhookRequestFixture) count(t *testing.T, table string) int {
	t.Helper()
	var count int
	if err := fixture.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestWebhookRequestMinterSettlesEachRequestByItsOwnOutcome drives the minter
// through every settlement: a due request mints its occurrence and trigger
// and is deleted; a replay of the same delivery time mints nothing new; a
// request past the age bound, and one naming another org's configuration,
// are refused with their reason and never minted; a request whose mint fails
// records a stage-named SQLSTATE and backs off, and is refused once its
// attempts are spent -- and none of them stops the others.
func TestWebhookRequestMinterSettlesEachRequestByItsOwnOutcome(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	delivered := now.Add(-time.Minute)

	minted := fixture.request(t, fixture.org, "incremental", delivered, now.Add(-time.Minute))
	replay := fixture.request(t, fixture.org, "incremental", delivered, now.Add(-50*time.Second))
	stale := fixture.request(t, fixture.org, "incremental", now.Add(-25*time.Hour), now.Add(-25*time.Hour))
	otherOrg := fixture.request(t, "00000000-0000-4000-8000-0000000000ff", "incremental", delivered, now.Add(-40*time.Second))
	// "bogus" violates sync_manual_triggers' mode CHECK: the mint fails.
	failing := fixture.request(t, fixture.org, "bogus", now.Add(-30*time.Second), now.Add(-30*time.Second))

	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	if err := minter.mintPending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}

	// A minted request is KEPT (marked minted, with its occurrence id) so a
	// retried delivery is a no-op; the two deliveries share an instant, so the
	// second one's occurrence moved one microsecond and both exist.
	first, second := fixture.row(t, minted), fixture.row(t, replay)
	if !first.exists || first.mintedAt == nil || first.occurrenceID == nil || !second.exists || second.mintedAt == nil || second.occurrenceID == nil || *first.occurrenceID == *second.occurrenceID {
		t.Fatalf("minted rows = %+v / %+v, want both kept, marked minted, with distinct occurrence ids", first, second)
	}
	if occurrences, triggers := fixture.count(t, "scheduled_sync_occurrences"), fixture.count(t, "sync_manual_triggers"); occurrences != 2 || triggers != 2 {
		t.Fatalf("occurrences=%d triggers=%d, want one of each per distinct delivery (2)", occurrences, triggers)
	}
	var instants []time.Time
	rows, err := fixture.pool.Query(ctx, `SELECT scheduled_for FROM scheduled_sync_occurrences ORDER BY scheduled_for`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var instant time.Time
		if err := rows.Scan(&instant); err != nil {
			t.Fatal(err)
		}
		instants = append(instants, instant)
	}
	rows.Close()
	if len(instants) != 2 || !instants[0].Equal(delivered) || !instants[1].Equal(delivered.Add(time.Microsecond)) {
		t.Fatalf("occurrence instants = %v, want %s and one microsecond later", instants, delivered)
	}
	var mode, triggeredBy string
	var sourceIDs []string
	var scheduledFor time.Time
	if err := fixture.pool.QueryRow(ctx, `
SELECT t.mode, t.triggered_by, t.source_ids, o.scheduled_for
FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o USING (occurrence_id) ORDER BY o.scheduled_for LIMIT 1`).Scan(&mode, &triggeredBy, &sourceIDs, &scheduledFor); err != nil {
		t.Fatal(err)
	}
	if mode != "incremental" || triggeredBy != "manual" || len(sourceIDs) != 1 || sourceIDs[0] != fixture.sourceID || !scheduledFor.Equal(delivered) {
		t.Fatalf("minted trigger mode=%q by=%q sources=%v scheduled_for=%s; want incremental/manual/[source]/%s",
			mode, triggeredBy, sourceIDs, scheduledFor, delivered)
	}

	if row := fixture.row(t, stale); !row.exists || row.refusedAt == nil || row.refused == nil || *row.refused != "stale: older than 24h0m0s" {
		t.Fatalf("stale request = %+v, want refused as stale", row)
	}
	if row := fixture.row(t, otherOrg); !row.exists || row.refused == nil || *row.refused != "sync configuration belongs to another organization" {
		t.Fatalf("another org's request = %+v, want refused", row)
	}
	row := fixture.row(t, failing)
	if !row.exists || row.refusedAt != nil || row.attempts != 1 || row.lastError == nil || *row.lastError != "mint: sqlstate 23514" ||
		row.nextAttempt == nil || !row.nextAttempt.Equal(now.Add(webhookRequestBaseBackoff)) {
		t.Fatalf("failing request = %+v (last_error %v), want attempt 1, 'mint: sqlstate 23514', next attempt after the base backoff",
			row, deref(row.lastError))
	}

	// Not due before its backoff: a window now leaves it untouched.
	if err := minter.mintPending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if again := fixture.row(t, failing); again.attempts != 1 {
		t.Fatalf("a request inside its backoff was retried: attempts=%d", again.attempts)
	}
	// Spend the attempts: each due window adds one, the last refuses.
	at := now
	for attempt := 2; attempt <= webhookRequestMaxAttempts; attempt++ {
		at = at.Add(webhookRequestMaxBackoff)
		if err := minter.mintPending(ctx, at, 10); err != nil {
			t.Fatal(err)
		}
	}
	final := fixture.row(t, failing)
	if final.refusedAt == nil || final.refused == nil || *final.refused != "attempts exhausted after 10; last error mint: sqlstate 23514" {
		t.Fatalf("exhausted request = %+v (refused %v), want refused after 10 attempts", final, deref(final.refused))
	}
	if occurrences := fixture.count(t, "scheduled_sync_occurrences"); occurrences != 2 {
		t.Fatalf("a failing or refused request minted an occurrence: %d, want only the two distinct deliveries", occurrences)
	}
}

// TestWebhookRequestMinterRefusesARequestWhoseConfigurationIsGone (r1): a
// pending request OUTLIVES its configuration (no cascade), so deleting the
// configuration leaves the request to be refused with a reason, never dropped.
func TestWebhookRequestMinterRefusesARequestWhoseConfigurationIsGone(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := fixture.request(t, fixture.org, "incremental", now.Add(-time.Minute), now.Add(-time.Minute))
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM sync_configurations WHERE id = $1::uuid`, fixture.configID); err != nil {
		t.Fatalf("delete the configuration: %v", err)
	}
	if row := fixture.row(t, id); !row.exists {
		t.Fatal("deleting the configuration deleted its pending request (a cascade)")
	}
	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	if err := minter.mintPending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if row := fixture.row(t, id); !row.exists || row.refused == nil || *row.refused != "sync configuration no longer exists" {
		t.Fatalf("request = %+v (refused %v), want refused: sync configuration no longer exists", row, deref(row.refused))
	}
	if occurrences := fixture.count(t, "scheduled_sync_occurrences"); occurrences != 0 {
		t.Fatalf("occurrences = %d, want none", occurrences)
	}
}

// TestWebhookRequestMinterPrunesMintedRequestsPastRetention: a minted request
// is kept for its retention window and then removed; pending and refused rows
// are never pruned.
func TestWebhookRequestMinterPrunesMintedRequestsPastRetention(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := fixture.request(t, fixture.org, "incremental", now.Add(-time.Minute), now.Add(-time.Minute))
	recent := fixture.request(t, fixture.org, "incremental", now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	refused := fixture.request(t, fixture.org, "incremental", now.Add(-3*time.Minute), now.Add(-3*time.Minute))
	pending := fixture.request(t, fixture.org, "incremental", now.Add(-4*time.Minute), now.Add(-4*time.Minute))
	for id, age := range map[string]time.Duration{old: 8 * 24 * time.Hour, recent: 6 * 24 * time.Hour} {
		if _, err := fixture.pool.Exec(ctx, `UPDATE webhook_sync_requests SET minted_at = $2, occurrence_id = 'x' WHERE delivery_id = $1`, id, now.Add(-age)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE webhook_sync_requests SET refused_at = $2, refused_reason = 'r' WHERE delivery_id = $1`, refused, now.Add(-30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// A pending request must not be swept by the window either: park it.
	if _, err := fixture.pool.Exec(ctx, `UPDATE webhook_sync_requests SET next_attempt_at = $2 WHERE delivery_id = $1`, pending, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	if err := minter.mintPending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if row := fixture.row(t, old); row.exists {
		t.Fatalf("a minted request past its retention was kept: %+v", row)
	}
	for name, id := range map[string]string{"recent minted": recent, "refused": refused, "pending": pending} {
		if row := fixture.row(t, id); !row.exists {
			t.Errorf("%s request was pruned", name)
		}
	}
	if minter.pruned.Load() != 1 {
		t.Errorf("pruned counter = %d, want 1", minter.pruned.Load())
	}
}

// TestWebhookRequestMinterSkipsARequestAnotherSchedulerHasClaimed: two
// scheduler replicas must not wait on each other. While one transaction holds
// the oldest request, a claim settles the next one instead of blocking.
func TestWebhookRequestMinterSkipsARequestAnotherSchedulerHasClaimed(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	held := fixture.request(t, fixture.org, "incremental", now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	free := fixture.request(t, fixture.org, "incremental", now.Add(-time.Minute), now.Add(-time.Minute))

	holder, err := fixture.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(context.Background(), `SELECT 1 FROM webhook_sync_requests WHERE delivery_id = $1 FOR UPDATE`, held); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	claimed, err := minter.mintOne(ctx, now)
	if err != nil || !claimed {
		t.Fatalf("mintOne = %v, %v; want it to settle the unlocked request without waiting", claimed, err)
	}
	if row := fixture.row(t, free); row.mintedAt == nil {
		t.Fatalf("the unlocked request was not minted: %+v", row)
	}
	if row := fixture.row(t, held); !row.exists || row.mintedAt != nil {
		t.Fatalf("the request another transaction holds was minted anyway: %+v", row)
	}
}

// TestWebhookRequestMinterFailsWhenEveryCandidateInstantIsTaken: a request
// moves at most webhookRequestMaxInstantBumps microseconds; past that it fails
// (backoff, stage-named), it is never merged into another delivery's occurrence.
func TestWebhookRequestMinterFailsWhenEveryCandidateInstantIsTaken(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	delivered := now.Add(-time.Minute)
	var ids []string
	for index := 0; index < webhookRequestMaxInstantBumps+2; index++ {
		ids = append(ids, fixture.request(t, fixture.org, "incremental", delivered, now.Add(-time.Minute+time.Duration(index)*time.Second)))
	}
	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	if err := minter.mintPending(ctx, now, 20); err != nil {
		t.Fatal(err)
	}
	minted := 0
	for _, id := range ids {
		if row := fixture.row(t, id); row.mintedAt != nil {
			minted++
		}
	}
	// One at the instant plus MaxInstantBumps moved ones; the rest fail.
	if want := webhookRequestMaxInstantBumps + 1; minted != want {
		t.Fatalf("minted %d requests at one instant, want %d", minted, want)
	}
	last := fixture.row(t, ids[len(ids)-1])
	if last.mintedAt != nil || last.attempts != 1 || last.lastError == nil || *last.lastError != "mint: sync.instantTakenError" {
		t.Fatalf("the request past the bound = %+v (last_error %v), want a stage-named failure and no occurrence", last, deref(last.lastError))
	}
	if occurrences := fixture.count(t, "scheduled_sync_occurrences"); occurrences != webhookRequestMaxInstantBumps+1 {
		t.Fatalf("occurrences = %d, want %d", occurrences, webhookRequestMaxInstantBumps+1)
	}
}

// TestWebhookRequestMinterRefusesADeliveryOlderThanTheAgeBound (r2): the age
// bound applies to the DELIVERY time, not only to the request row's age, so a
// replay of an old delivery (whose minted row was pruned) is refused.
func TestWebhookRequestMinterRefusesADeliveryOlderThanTheAgeBound(t *testing.T) {
	fixture := startWebhookRequestFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	// The row is brand new; the delivery it stands for is 25 hours old.
	old := fixture.request(t, fixture.org, "incremental", now.Add(-25*time.Hour), now)
	inside := fixture.request(t, fixture.org, "incremental", now.Add(-23*time.Hour), now)
	minter := &webhookRequestMinter{pool: fixture.pool, maxAge: webhookRequestMaxAge}
	if err := minter.mintPending(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if row := fixture.row(t, old); row.mintedAt != nil || row.refused == nil || *row.refused != "stale: delivery older than 24h0m0s" {
		t.Fatalf("a 25h-old delivery = %+v (refused %v), want refused as a stale delivery", row, deref(row.refused))
	}
	if row := fixture.row(t, inside); row.mintedAt == nil {
		t.Fatalf("a 23h-old delivery was not minted: %+v", row)
	}
	if occurrences := fixture.count(t, "scheduled_sync_occurrences"); occurrences != 1 {
		t.Fatalf("occurrences = %d, want only the fresh delivery's", occurrences)
	}
}
