package streamrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// MaxRetainedPayloadBytes bounds the product-telemetry `events` text kept in a
// dead-letter row. The web client sends at most 10 events per request (about
// 3 KiB) and the intake allows 500 events per entry (about 150 KiB at the
// observed event size), so 256 KiB keeps every entry the intake can accept
// whole. Valkey holds the dead-letter stream in memory and the stream keeps up
// to 100000 rows, so the bound must stay finite: above it the row carries a
// marker, the size and a SHA-256 digest instead of the text.
const MaxRetainedPayloadBytes = 256 * 1024

// Payload retention modes recorded in the `events_retention` field of a
// dead-letter row and in the quarantine log line.
const (
	RetentionKept      = "kept"
	RetentionOverBound = "over_bound"
	RetentionWithheld  = "withheld"
	RetentionNone      = "none"
)

const (
	productTelemetryPrefix = "product-telemetry:"
	eventsField            = "events"
	// blockedPayloadReason is the product-telemetry reason for an entry refused
	// because an event payload carried a key the privacy contract bans (email,
	// url, message, ...). Keeping that text would copy exactly the data the
	// contract refuses, so such a row keeps the digest and the sizes only.
	blockedPayloadReason = "blocked_telemetry_payload"
)

// retainedFields lists the product-telemetry entry fields, besides the durable
// identities, that a dead-letter row keeps so the entry can be replayed.
var retainedFields = []string{"source", "org_id_hash"}

// payloadRetention describes what a dead-letter row keeps of a message. It is
// pure so the Valkey writer, the runner's log line and the tests agree.
type payloadRetention struct {
	Mode        string
	Bytes       int
	SHA256      string
	EventsCount int // -1 when the text is not a JSON list
}

func retentionFor(message Message, reason string) payloadRetention {
	if !strings.HasPrefix(message.Stream, productTelemetryPrefix) {
		return payloadRetention{Mode: RetentionNone, EventsCount: -1}
	}
	text, present := message.Fields[eventsField]
	if !present {
		return payloadRetention{Mode: RetentionNone, EventsCount: -1}
	}
	sum := sha256.Sum256([]byte(text))
	retention := payloadRetention{Bytes: len(text), SHA256: hex.EncodeToString(sum[:]), EventsCount: -1}
	switch {
	case reason == blockedPayloadReason:
		retention.Mode = RetentionWithheld
	case len(text) > MaxRetainedPayloadBytes:
		retention.Mode = RetentionOverBound
	default:
		retention.Mode = RetentionKept
	}
	if retention.Mode != RetentionOverBound {
		var list []json.RawMessage
		if json.Unmarshal([]byte(text), &list) == nil {
			retention.EventsCount = len(list)
		}
	}
	return retention
}

// DeadLetterFields is the field set of a dead-letter row for a message that is
// not external-ingest: the durable identities, and for a product-telemetry
// entry the `events` text under the bound (see MaxRetainedPayloadBytes), its
// size and digest always, and the replay fields. movedAt is RFC3339Nano.
func DeadLetterFields(message Message, reason, movedAt string) map[string]string {
	fields := map[string]string{"original_stream": message.Stream, "entry_id": message.ID, "reason": reason, "moved_at": movedAt}
	for key, value := range message.Fields {
		// Durable identities only. binding_id/event_id make a PagerDuty DLQ row
		// reconcilable against the Python contract; the raw payload of every
		// other family stays out of the quarantine record.
		if key == "ingestion_id" || key == "org_id" || key == "binding_id" || key == "event_id" {
			fields[key] = value
		}
	}
	retention := retentionFor(message, reason)
	if retention.Mode == RetentionNone {
		return fields
	}
	fields["events_retention"] = retention.Mode
	fields["events_bytes"] = strconv.Itoa(retention.Bytes)
	fields["events_sha256"] = retention.SHA256
	if retention.Mode != RetentionKept {
		return fields
	}
	fields[eventsField] = message.Fields[eventsField]
	for _, key := range retainedFields {
		if value, ok := message.Fields[key]; ok {
			fields[key] = value
		}
	}
	return fields
}

// MessageFromDeadLetter rebuilds the message a handler consumed from a
// dead-letter row, so a quarantined entry can be fed to the handler again. It
// reports false when the row did not keep the payload (over the bound, withheld
// or a family that never keeps it) or when the kept text no longer matches the
// recorded digest.
func MessageFromDeadLetter(row map[string]string) (Message, bool) {
	text, ok := row[eventsField]
	if !ok || row["events_retention"] != RetentionKept {
		return Message{}, false
	}
	sum := sha256.Sum256([]byte(text))
	if hex.EncodeToString(sum[:]) != row["events_sha256"] {
		return Message{}, false
	}
	fields := map[string]string{eventsField: text}
	for _, key := range append([]string{"ingestion_id"}, retainedFields...) {
		if value, ok := row[key]; ok {
			fields[key] = value
		}
	}
	return Message{Stream: row["original_stream"], ID: row["entry_id"], Fields: fields}, true
}
