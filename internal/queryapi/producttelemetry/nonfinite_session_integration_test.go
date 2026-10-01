//go:build integration

package producttelemetry

import (
	"context"
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

// Round 3: the session summary's percentiles and averages turned a stored null into 0, the
// same error the filter average had (a missing number counted as a measured zero). Real
// ClickHouse, real handler, real reader: a day with one finite session and one whose numbers
// were all non-finite reads the finite values only; a day with only non-finite sessions reads
// every field as missing (null), never 0.
func TestNonFiniteSessionNumbersAreMissingNotZero(t *testing.T) {
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
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	handler, err := streamhandlers.NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}

	mixedDay := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	missingDay := mixedDay.AddDate(0, 0, 1)
	session := func(id string, day time.Time, payload string) string {
		return `{"name": "session_ended", "schemaVersion": "1", "eventId": "` + id + `", "ts": "` + day.Add(time.Hour).Format(time.RFC3339) + `", "sessionId": "s", "anonymousUserId": "a", "orgIdHash": null, "routePattern": null, "payload": ` + payload + `}`
	}
	events := "[" + strings.Join([]string{
		session("m1", mixedDay, `{"durationMs": 100, "pagesViewed": 4, "interactions": 6}`),
		session("m2", mixedDay, `{"durationMs": NaN, "pagesViewed": Infinity, "interactions": -Infinity}`),
		session("n1", missingDay, `{"durationMs": NaN, "pagesViewed": NaN, "interactions": NaN}`),
	}, ", ") + "]"
	if err := handler.Handle(ctx, streamrunner.Message{Stream: "product-telemetry:org:events", ID: "1-0", Fields: map[string]string{"events": events}}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	reader := &Reader{ClickHouse: client}

	mixed, err := reader.PlatformSections(ctx, Range{Start: mixedDay, End: mixedDay.AddDate(0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := mixed.Shared.Summary
	for name, got := range map[string]*int{"p50": s.P50DurationMs, "p75": s.P75DurationMs, "p90": s.P90DurationMs, "p95": s.P95DurationMs} {
		if got == nil || *got != 100 {
			t.Fatalf("mixed day %s = %v, want 100: the non-finite session is missing, not a 0 that would pull it down", name, got)
		}
	}
	if s.AvgPagesViewed == nil || *s.AvgPagesViewed != 4 || s.AvgInteractions == nil || *s.AvgInteractions != 6 {
		t.Fatalf("mixed day averages = %v, %v; want 4 and 6", s.AvgPagesViewed, s.AvgInteractions)
	}

	missing, err := reader.PlatformSections(ctx, Range{Start: missingDay, End: missingDay.AddDate(0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	m := missing.Shared.Summary
	for name, got := range map[string]*int{"p50": m.P50DurationMs, "p75": m.P75DurationMs, "p90": m.P90DurationMs, "p95": m.P95DurationMs} {
		if got != nil {
			t.Fatalf("all-missing day %s = %v, want null (nothing was measured)", name, *got)
		}
	}
	for name, got := range map[string]*float64{"avgPages": m.AvgPagesViewed, "avgInteractions": m.AvgInteractions} {
		if got != nil {
			t.Fatalf("all-missing day %s = %v, want null (nothing was measured)", name, *got)
		}
	}
}

// Round 3: a refused entry that had opened a ClickHouse batch kept its connection, so four
// refusals exhausted the default pool of four and the next valid entry timed out in
// PrepareBatch. Real pool, real handler: refuse pool-size entries (each with a blocked key, the
// shape that reached the batch), then a valid entry must still be written.
func TestRefusedEntriesDoNotExhaustTheClickHousePool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	cfg := chstorage.DefaultConfig(instance.URI)
	conn, err := chstorage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	handler, err := streamhandlers.NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	event := func(id, payload string) string {
		return `[{"name":"page_viewed","schemaVersion":"1","eventId":"` + id + `","ts":"` + time.Now().UTC().Format(time.RFC3339) + `","sessionId":"s","anonymousUserId":"a","payload":` + payload + `}]`
	}
	for i := 0; i < cfg.MaxOpenConns+2; i++ {
		for _, payload := range []string{`{"x":NaN,"email":"a@example.test"}`, `{"x":1,"email":"a@example.test"}`} {
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			err := handler.Handle(attempt, streamrunner.Message{Fields: map[string]string{"events": event("poison", payload)}})
			stop()
			if !streamrunner.IsPermanent(err) {
				t.Fatalf("refusal %d (%s) = %v, want a permanent refusal", i, payload, err)
			}
		}
	}
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if err := handler.Handle(attempt, streamrunner.Message{Fields: map[string]string{"events": event("healthy", `{"x":1}`)}}); err != nil {
		t.Fatalf("a valid entry after the refusals = %v: the pool is exhausted", err)
	}
}

// Round 1 of the decoder fix: the same leak in internal ingest, which shares the ClickHouse
// connection with product telemetry in the ingest profile. Refused commits must not exhaust
// the pool and stall a valid telemetry entry.
func TestRefusedInternalCommitsDoNotExhaustTheSharedPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	cfg := chstorage.DefaultConfig(instance.URI)
	conn, err := chstorage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ingest, err := streamhandlers.NewInternalIngestHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	telemetry, err := streamhandlers.NewProductTelemetryHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	refused := streamrunner.Message{Stream: "ingest:org:commits", Fields: map[string]string{"payload": `{"org_id":"org","repo_url":"https://example.test/r","items":[{"hash":""}]}`}}
	for i := 0; i < cfg.MaxOpenConns+2; i++ {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		err := ingest.Handle(attempt, refused)
		stop()
		if !streamrunner.IsPermanent(err) {
			t.Fatalf("refusal %d = %v, want a permanent refusal", i, err)
		}
	}
	event := `[{"name":"page_viewed","schemaVersion":"1","eventId":"healthy","ts":"` + time.Now().UTC().Format(time.RFC3339) + `","sessionId":"s","anonymousUserId":"a","payload":{"x":1}}]`
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if err := telemetry.Handle(attempt, streamrunner.Message{Fields: map[string]string{"events": event}}); err != nil {
		t.Fatalf("a valid telemetry entry after the refused commits = %v: the shared pool is exhausted", err)
	}
}
