//go:build integration

package streamhandlers

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// The timestamps the consumer accepts are stored by a real ClickHouse exactly as sent, at both ends of
// the range the Go driver encodes (a year-2500 timestamp used to come back as 1915).
//
// The table keeps an event for 180 days: migration 041 declares
// `TTL toDateTime(occurred_at) + INTERVAL 180 DAY DELETE`. toDateTime is UInt32 (1970-01-01 to
// 2106-02-07 06:28:15), so for a late timestamp the sum wraps past the limit, evaluates to a time in
// 1970 and the row is deleted as soon as the table merges. Measured on ClickHouse 26.8.2 with one
// insert per row and OPTIMIZE ... FINAL: 2105-08-11T06:28:16.999 stays, 2105-08-11T06:28:17.000 and
// everything after it (up to the 2262 intake ceiling) is deleted. The old Python consumer wrote the
// same table with the same TTL (this migration), so the loss is not new with the Go consumer. The test
// pins both sides of that boundary so a change to the TTL or to the ClickHouse version that moves it
// shows up here; whether to refuse such timestamps at intake is a product call (CHAOS-7617).
// An early timestamp wraps the other way (1899-12-31 evaluates to 2036) and its row is deleted then.
func TestProductTelemetryStoresTimestampsAtTheDriverRangeEdgesExactly(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	handler, err := NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	const gone = "gone" // accepted by the intake, deleted by the table TTL
	cases := map[string]string{
		"1899-12-31T00:00:00Z":      "1899-12-31 00:00:00.000",
		"1900-01-01T00:00:00":       "1900-01-01 00:00:00.000",
		"2026-09-23T02:00:00+05:30": "2026-09-22 20:30:00.000",
		"2105-08-11T06:28:16.999Z":  "2105-08-11 06:28:16.999", // last timestamp the TTL keeps
		"2105-08-11T06:28:17Z":      gone,                      // first timestamp the TTL deletes
		"2262-04-11T23:47:16.854Z":  gone,
		"2263-01-01T00:00:00Z":      "", // refused, never stored
	}
	ids := map[string]string{}
	n := 0
	for ts, want := range cases {
		n++
		event := map[string]any{"name": "page_viewed", "schemaVersion": "1", "eventId": fmt.Sprintf("edge-%d", n), "ts": ts, "sessionId": "s", "anonymousUserId": "a", "payload": map[string]any{}}
		err := handler.Handle(ctx, intakeEntryFor(t, event))
		if want == "" {
			if !streamrunner.IsPermanent(err) {
				t.Fatalf("%s: err = %v, want a permanent refusal", ts, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", ts, err)
		}
		ids[ts] = event["eventId"].(string)
	}
	// Apply the TTL now instead of whenever the background merge runs, so the readback below is the
	// settled state and not a race with it.
	if err := conn.Exec(ctx, "OPTIMIZE TABLE product_telemetry_events FINAL"); err != nil {
		t.Fatal(err)
	}
	for ts, want := range cases {
		id, handled := ids[ts]
		if !handled {
			continue
		}
		var got string
		err := conn.QueryRow(ctx, "SELECT toString(occurred_at) FROM product_telemetry_events WHERE event_id = ?", id).Scan(&got)
		if want == gone {
			if err == nil {
				t.Fatalf("ts %s is stored as %s, want it deleted by the table TTL (the boundary moved)", ts, got)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("ts %s: %v", ts, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ts %s: %v", ts, err)
		}
		if got != want {
			t.Fatalf("ts %s stored as %s, want %s", ts, got, want)
		}
	}
}
