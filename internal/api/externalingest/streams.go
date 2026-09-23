package externalingest

import (
	"context"
	"errors"
	"strconv"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// streamMaxlen is streams.py's STREAM_MAXLEN.
const streamMaxlen = 100_000

// errStreamUnavailable mirrors streams.py's StreamUnavailableError: Redis/
// Valkey unavailable, or the fail-closed payload-durability precondition
// failed. Callers map it to 503, never accept-and-warn.
var errStreamUnavailable = errors.New("the durable ingest stream is temporarily unavailable")

func streamName(orgID string) string { return "external-ingest:" + orgID + ":batches" }

// enqueueBatch ports streams.py's enqueue_batch: XADDs a metadata-only
// pointer with the exact field names internal/streamhandlers/
// external_ingest.go's parseExternalPointer reads (ingestion_id, org_id,
// source_system, source_instance, schema_version) plus the extra fields
// that consumer does not need but Python's producer has always sent, kept
// for parity with any tooling reading the stream directly.
func enqueueBatch(ctx context.Context, client valkeygo.Client, p enqueueParams) (string, error) {
	if client == nil {
		return "", errStreamUnavailable
	}
	stream := streamName(p.OrgID)
	// Field order does not matter to XADD (a Redis stream entry is an
	// unordered field-value map); this order just matches streams.py's
	// dict literal for readability.
	fields := [][2]string{
		{"ingestion_id", p.IngestionID},
		{"org_id", p.OrgID},
		{"source_system", p.SourceSystem},
		{"source_instance", p.SourceInstance},
		{"schema_version", p.SchemaVersion},
		{"idempotency_key", p.IdempotencyKey},
		{"record_count", strconv.Itoa(p.RecordCount)},
		{"window_started_at", formatOptionalTime(p.WindowStartedAt)},
		{"window_ended_at", formatOptionalTime(p.WindowEndedAt)},
		{"enqueued_at", time.Now().UTC().Format(time.RFC3339Nano)},
	}

	cmd := client.B().Xadd().Key(stream).Maxlen().Almost().
		Threshold(strconv.Itoa(streamMaxlen)).Id("*").FieldValue()
	for _, kv := range fields {
		cmd = cmd.FieldValue(kv[0], kv[1])
	}
	if err := client.Do(ctx, cmd.Build()).Error(); err != nil {
		return "", errStreamUnavailable
	}
	return stream, nil
}

type enqueueParams struct {
	OrgID           string
	IngestionID     string
	SourceSystem    string
	SourceInstance  string
	SchemaVersion   string
	IdempotencyKey  string
	RecordCount     int
	WindowStartedAt *time.Time
	WindowEndedAt   *time.Time
}

func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
