package streamrunner

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const telemetryStream = "product-telemetry:org-hash:events"

func telemetryMessage(events string) Message {
	return Message{Stream: telemetryStream, ID: "1-0", Fields: map[string]string{
		"ingestion_id": "ing-1", "source": "dev-health-web", "org_id_hash": "org-hash", eventsField: events,
	}}
}

// A permanent error on a product-telemetry entry must leave the entry
// replayable from its dead-letter row.
func TestDeadLetterKeepsTheTelemetryPayloadAndReplaysIt(t *testing.T) {
	events := `[{"name":"page_viewed","eventId":"e1"}]`
	message := telemetryMessage(events)
	row := DeadLetterFields(message, "invalid_telemetry_event", "2026-10-01T00:00:00Z")
	if row[eventsField] != events || row["events_retention"] != RetentionKept {
		t.Fatalf("row kept %q mode %q, want the events text kept", row[eventsField], row["events_retention"])
	}
	replay, ok := MessageFromDeadLetter(row)
	if !ok || replay.Fields[eventsField] != events || replay.Fields["source"] != "dev-health-web" || replay.Stream != telemetryStream || replay.ID != "1-0" {
		t.Fatalf("replay message = %#v ok=%v, want the original entry", replay, ok)
	}
	row[eventsField] = strings.Replace(events, "e1", "e2", 1)
	if _, ok := MessageFromDeadLetter(row); ok {
		t.Fatal("a row whose text no longer matches its digest was accepted for replay")
	}
}

func TestDeadLetterOverTheBoundKeepsMarkerSizesAndDigestNeverTheText(t *testing.T) {
	events := "[" + strings.Repeat(" ", MaxRetainedPayloadBytes) + "]"
	row := DeadLetterFields(telemetryMessage(events), "invalid_event_count", "t")
	if _, present := row[eventsField]; present {
		t.Fatal("over-bound events text was kept")
	}
	if row["events_retention"] != RetentionOverBound || row["events_bytes"] != "262146" || len(row["events_sha256"]) != 64 {
		t.Fatalf("over-bound row = %v", row)
	}
	if _, ok := MessageFromDeadLetter(row); ok {
		t.Fatal("an over-bound row claimed to be replayable")
	}
	exact := "[" + strings.Repeat(" ", MaxRetainedPayloadBytes-2) + "]"
	if got := DeadLetterFields(telemetryMessage(exact), "x", "t")["events_retention"]; got != RetentionKept {
		t.Fatalf("a payload exactly at the bound has retention %q, want kept", got)
	}
}

// An entry refused because a payload key is banned must not copy that text
// into the dead-letter stream.
func TestDeadLetterWithholdsAPayloadRefusedForABannedKey(t *testing.T) {
	row := DeadLetterFields(telemetryMessage(`[{"payload":{"email":"a@b.c"}}]`), blockedPayloadReason, "t")
	if _, present := row[eventsField]; present || row["events_retention"] != RetentionWithheld || row["events_sha256"] == "" {
		t.Fatalf("banned-key row = %v", row)
	}
	for key, value := range row {
		if strings.Contains(value, "a@b.c") {
			t.Fatalf("field %s carries the banned value", key)
		}
	}
}

func TestDeadLetterOtherFamiliesStillKeepIdentitiesOnly(t *testing.T) {
	for _, stream := range []string{"pagerduty-webhooks:b1", "ingest:org:commits", "unknown"} {
		message := Message{Stream: stream, ID: "1-0", Fields: map[string]string{"binding_id": "b1", "event_id": "ev", eventsField: "SECRET", "body": "SECRET"}}
		row := DeadLetterFields(message, "r", "t")
		for key, value := range row {
			if strings.Contains(value, "SECRET") {
				t.Fatalf("%s: field %s carries the payload", stream, key)
			}
		}
		if row["binding_id"] != "b1" || row["event_id"] != "ev" {
			t.Fatalf("%s: identities lost: %v", stream, row)
		}
	}
}

// The runner counts every quarantine by stream family and reason and logs one
// line with sizes and counts, never the payload.
func TestRunnerCountsAndLogsEachQuarantineWithoutThePayload(t *testing.T) {
	const secret = "PAYLOAD-TEXT-MUST-NOT-BE-LOGGED"
	events := `[{"payload":{"x":"` + secret + `"}}]`
	transport := &fakeTransport{new: []Message{
		{Stream: telemetryStream, ID: "1-0", Fields: map[string]string{eventsField: events}},
		{Stream: telemetryStream, ID: "2-0", Fields: map[string]string{eventsField: events}},
	}}
	var logs bytes.Buffer
	config := testConfig()
	config.Streams = []string{telemetryStream}
	config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error {
		return &PermanentError{Reason: "invalid_telemetry_event"}
	}), config, health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := runner.window(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var metrics bytes.Buffer
	if err := runner.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	want := `worker_stream_quarantined_by_reason_total{stream="product-telemetry",reason="invalid_telemetry_event"} 2`
	if !strings.Contains(metrics.String(), want) {
		t.Fatalf("metrics lack %q:\n%s", want, metrics.String())
	}
	if n := strings.Count(logs.String(), "stream message quarantined"); n != 2 {
		t.Fatalf("quarantine log lines = %d, want 2:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), secret) || !strings.Contains(logs.String(), "events_bytes=") || !strings.Contains(logs.String(), "events_count=1") {
		t.Fatalf("log line wrong:\n%s", logs.String())
	}
}

func TestQuarantineKeyBoundsItsLabels(t *testing.T) {
	if got := newQuarantineKey("product-telemetry:some-org:events", "Bad Reason!"); got != (quarantineKey{"product-telemetry", "other"}) {
		t.Fatalf("key = %v", got)
	}
	if got := newQuarantineKey("weird:org", "max_deliveries_exceeded"); got != (quarantineKey{"other", "max_deliveries_exceeded"}) {
		t.Fatalf("key = %v", got)
	}
}
