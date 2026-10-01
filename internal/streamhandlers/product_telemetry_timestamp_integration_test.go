//go:build integration

package streamhandlers

import (
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// The timestamps the consumer accepts are stored by a real ClickHouse exactly as sent, at both ends of
// the range the Go driver encodes (a year-2500 timestamp used to come back as 1915).
func TestProductTelemetryStoresTimestampsAtTheDriverRangeEdgesExactly(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	handler, err := NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"1899-12-31T00:00:00Z":      "1899-12-31 00:00:00.000",
		"1900-01-01T00:00:00":       "1900-01-01 00:00:00.000",
		"2026-09-23T02:00:00+05:30": "2026-09-22 20:30:00.000",
		"2262-04-11T23:47:16.854Z":  "2262-04-11 23:47:16.854",
		"2263-01-01T00:00:00Z":      "", // refused, never stored
	}
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
		var got string
		if err := conn.QueryRow(ctx, "SELECT toString(occurred_at) FROM product_telemetry_events WHERE event_id = ?", event["eventId"]).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("ts %s stored as %s, want %s", ts, got, want)
		}
	}
}
