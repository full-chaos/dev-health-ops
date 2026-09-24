//go:build integration

// CHAOS-5523 integration proof for ResolveEvents, against a REAL
// ClickHouse engine -- same testcontainers-backed pattern
// hotspots_argmax_tiebreak_integration_test.go uses in this repo
// (containers.StartClickHouse, a raw driver connection for DDL/INSERT
// since dev-health-go's Client rejects anything but a literal leading
// SELECT, and clickhouse.NewClickHouseQueryClientWithOptions for the
// read-only client ResolveEvents actually takes). Never the bare-host
// compose ClickHouse -- that stack is read-only reference data for this
// lane, not a test fixture host.
package featureflags

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// featureFlagEventDDL mirrors the production schema exactly, confirmed
// via `DESCRIBE TABLE feature_flag_event` against the local compose
// ClickHouse before writing this file: all columns are plain
// non-nullable String/DateTime64(3, 'UTC'), no Nullable() wrapper
// anywhere. Only the columns ResolveEvents' queries actually touch are
// declared here (org_id, flag_key, event_type, prev_state, next_state,
// actor_type, environment, event_ts) -- repo_id/ingested_at/
// source_event_id/dedupe_key are never read by this operation.
const featureFlagEventDDL = `
CREATE TABLE feature_flag_event
(
    org_id String,
    event_type String,
    flag_key String,
    environment String,
    actor_type String,
    prev_state String,
    next_state String,
    event_ts DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (org_id, flag_key, event_ts)
`

func openRawClickHouseForEvents(t *testing.T, dsn string) stdclickhouse.Conn {
	t.Helper()
	opts, err := stdclickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type eventSeedRow struct {
	eventType   string
	flagKey     string
	environment string
	actorType   string
	prevState   string
	nextState   string
	eventTs     time.Time
}

func seedFeatureFlagEvents(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, rows []eventSeedRow) {
	t.Helper()
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf(
			"('%s', '%s', '%s', '%s', '%s', '%s', '%s', toDateTime64('%s', 3, 'UTC'))",
			orgID, r.eventType, r.flagKey, r.environment, r.actorType, r.prevState, r.nextState,
			r.eventTs.UTC().Format("2006-01-02 15:04:05.000"),
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO feature_flag_event (org_id, event_type, flag_key, environment, actor_type, prev_state, next_state, event_ts) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed feature_flag_event: %v", err)
	}
}

// TestResolveEvents_RealClickHouse_HappyPath proves ResolveEvents' actual
// SQL (org-scoped WHERE, ORDER BY event_ts ASC, LIMIT clamp, and the
// count query's independent WHERE-only-no-limit form) against a real
// ClickHouse engine, not the fakeClient doubles events_test.go uses.
func TestResolveEvents_RealClickHouse_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	conn := openRawClickHouseForEvents(t, inst.URI)
	if err := conn.Exec(ctx, featureFlagEventDDL); err != nil {
		t.Fatalf("create feature_flag_event: %v", err)
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "chaos-5523-events-happy-path"
	t0 := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	rows := []eventSeedRow{
		{eventType: "flag_toggled", flagKey: "dark-mode", environment: "staging", actorType: "user", prevState: "off", nextState: "on", eventTs: t0.Add(2 * time.Minute)},
		{eventType: "flag_created", flagKey: "dark-mode", environment: "staging", actorType: "system", prevState: "", nextState: "off", eventTs: t0},
		{eventType: "flag_toggled", flagKey: "other-flag", environment: "production", actorType: "user", prevState: "on", nextState: "off", eventTs: t0.Add(time.Minute)},
	}
	seedFeatureFlagEvents(t, ctx, conn, orgID, rows)

	// A second org's rows must never leak into org-1's result.
	seedFeatureFlagEvents(t, ctx, conn, "chaos-5523-events-other-org", []eventSeedRow{
		{eventType: "flag_toggled", flagKey: "dark-mode", environment: "staging", actorType: "user", prevState: "off", nextState: "on", eventTs: t0},
	})

	result, err := ResolveEvents(ctx, client, orgID, nil, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents: %v", err)
	}
	if result.DegradedReason != nil {
		t.Fatalf("DegradedReason = %v, want nil", *result.DegradedReason)
	}
	// org-scoped (excludes the other-org seed row below), unfiltered by
	// flagKey/environment: all 3 of this org's rows (dark-mode's two,
	// other-flag's one).
	if result.TotalCount != 3 {
		t.Fatalf("TotalCount = %d, want 3 (org-scoped, all 3 seeded rows)", result.TotalCount)
	}
	if len(result.Events) != 3 {
		t.Fatalf("len(Events) = %d, want 3", len(result.Events))
	}
	// ORDER BY event_ts ASC: t0 (flag_created/dark-mode), t0+1m
	// (flag_toggled/other-flag), t0+2m (flag_toggled/dark-mode).
	if result.Events[0].EventType != "flag_created" || result.Events[0].FlagKey != "dark-mode" {
		t.Fatalf("events[0] not in event_ts ASC order: %+v", result.Events[0])
	}
	if result.Events[1].FlagKey != "other-flag" {
		t.Fatalf("events[1] not in event_ts ASC order: %+v", result.Events[1])
	}
	if result.Events[2].FlagKey != "dark-mode" || result.Events[2].NextState != "on" {
		t.Fatalf("events[2] not in event_ts ASC order: %+v", result.Events[2])
	}

	// flagKey filter narrows to the requested flag only.
	flagKey := "dark-mode"
	filtered, err := ResolveEvents(ctx, client, orgID, &flagKey, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents (flagKey filter): %v", err)
	}
	if filtered.TotalCount != 2 {
		t.Fatalf("flagKey-filtered TotalCount = %d, want 2", filtered.TotalCount)
	}

	otherFlagKey := "other-flag"
	filteredOther, err := ResolveEvents(ctx, client, orgID, &otherFlagKey, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents (other flagKey filter): %v", err)
	}
	if filteredOther.TotalCount != 1 {
		t.Fatalf("other-flag TotalCount = %d, want 1", filteredOther.TotalCount)
	}

	// count() must NOT be clamped by a small limit -- true total count.
	limited, err := ResolveEvents(ctx, client, orgID, nil, nil, 1)
	if err != nil {
		t.Fatalf("ResolveEvents (limit=1): %v", err)
	}
	if len(limited.Events) != 1 {
		t.Fatalf("len(Events) with limit=1 = %d, want 1", len(limited.Events))
	}
	if limited.TotalCount != 3 {
		t.Fatalf("TotalCount with limit=1 = %d, want 3 (count query is not limit-bound)", limited.TotalCount)
	}
}

// TestResolveEvents_RealClickHouse_MissingTableDegrades proves the
// degraded path against a real ClickHouse UNKNOWN_TABLE (code 60) error,
// not a scripted fakeClient double -- the table is never created at all
// in this test's instance.
func TestResolveEvents_RealClickHouse_MissingTableDegrades(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	result, err := ResolveEvents(ctx, client, "org-1", nil, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents returned an error instead of degrading: %v", err)
	}
	if result.DegradedReason == nil || *result.DegradedReason != EventNotMaterializedReason {
		t.Fatalf("DegradedReason = %v, want %q", result.DegradedReason, EventNotMaterializedReason)
	}
	if len(result.Events) != 0 || result.TotalCount != 0 {
		t.Fatalf("degraded result not empty: events=%v total=%d", result.Events, result.TotalCount)
	}
}
