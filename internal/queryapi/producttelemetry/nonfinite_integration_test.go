//go:build integration

package producttelemetry

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6299 end to end, through a real ClickHouse at the migration head: the entry text a
// Python producer writes (bare NaN / Infinity, from streams.py's own json.dumps) goes through
// the real stream handler into product_telemetry_events, and the real dashboard reader reads
// it back. A non-finite number is stored as null: the row's other keys stay readable by
// JSONExtract*, and the average of a numeric field treats the null as MISSING, never as 0
// (North Star check 12).
func TestNonFiniteProductTelemetryIsStoredAsNullAndReadAsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open the native client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	handler, err := streamhandlers.NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}

	// The producer's text: json.dumps writes the bare words. Group v1/team has valueCounts
	// 4, NaN and 2; group v2/other has only a NaN; the feature event has a NaN next to
	// keys the dashboard reads.
	// The events are recent: the table's TTL deletes rows older than 180 days, and a fixed past
	// date would let a background merge drop them mid-test.
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	event := func(id, name, payload string) string {
		return `{"name": "` + name + `", "schemaVersion": "1", "eventId": "` + id + `", "ts": "` + day.Add(10*time.Hour).Format(time.RFC3339) + `", "sessionId": "s", "anonymousUserId": "a", "orgIdHash": null, "routePattern": null, "payload": ` + payload + `}`
	}
	events := "[" + strings.Join([]string{
		event("f1", "filter_changed", `{"view": "v1", "filterKey": "team", "valueCount": 4}`),
		event("f2", "filter_changed", `{"view": "v1", "filterKey": "team", "valueCount": NaN}`),
		event("f3", "filter_changed", `{"view": "v1", "filterKey": "team", "valueCount": 2}`),
		event("f4", "filter_changed", `{"view": "v2", "filterKey": "other", "valueCount": Infinity}`),
		event("v1", "feature_viewed", `{"feature": "home", "surface": "nav", "weight": NaN, "delta": -Infinity}`),
	}, ", ") + "]"
	if err := handler.Handle(ctx, streamrunner.Message{Stream: "product-telemetry:org:events", ID: "1-0", Fields: map[string]string{"events": events}}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Stored as strict JSON with null where the number was not finite.
	rows, err := client.Query(ctx, "SELECT event_id, name, toString(occurred_at), payload_json FROM product_telemetry_events ORDER BY event_id", nil)
	if err != nil {
		t.Fatal(err)
	}
	stored := map[string]string{}
	for rows.Next() {
		var id, name, at, payload string
		if err := rows.Scan(&id, &name, &at, &payload); err != nil {
			t.Fatalf("scan stored row: %v", err)
		}
		stored[id] = payload
		t.Logf("stored %s %s at=%s payload=%s", id, name, at, payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if len(stored) != 5 {
		t.Fatalf("stored %d events, want 5", len(stored))
	}
	if got := stored["v1"]; !json.Valid([]byte(got)) || got != `{"delta":null,"feature":"home","surface":"nav","weight":null}` {
		t.Fatalf("payload_json = %s, want strict JSON with null for the non-finite numbers", got)
	}

	reader := &Reader{ClickHouse: client}
	platform, err := reader.PlatformSections(ctx, Range{Start: day, End: day.AddDate(0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	filters := map[string]*float64{}
	for _, f := range platform.Shared.Filters {
		filters[f.View+"/"+f.FilterKey] = f.AvgValueCount
	}
	if avg := filters["v1/team"]; avg == nil || *avg != 3 {
		t.Logf("filters read: %+v", platform.Shared.Filters)
		t.Fatalf("v1/team avg = %v, want 3: the NaN valueCount is missing, not a 0 that would make it 2", avg)
	}
	if avg, ok := filters["v2/other"]; !ok || avg != nil {
		t.Fatalf("v2/other avg = %v (present=%v), want present and null: every value was non-finite, so there is nothing to average", avg, ok)
	}
	var home bool
	for _, f := range platform.Shared.Features {
		if f.Feature == "home" && f.Surface == "nav" && f.Views == 1 {
			home = true
		}
	}
	if !home {
		t.Fatalf("the feature dimensions of the event with non-finite siblings were not read: %+v", platform.Shared.Features)
	}
}
