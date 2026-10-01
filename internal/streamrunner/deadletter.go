package streamrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
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

// MaxDeadLetterFieldBytes bounds every field of a dead-letter row other than the retained `events`
// text. The intake accepts an unrestricted `orgIdHash` string and stream keys embed it, so without a
// bound a single entry could write a row far larger than MaxRetainedPayloadBytes. A field over the
// bound is cut and carries the length and digest of the full value after a marker.
const MaxDeadLetterFieldBytes = 1024

const (
	productTelemetryPrefix = "product-telemetry:"
	eventsField            = "events"
	// blockedPayloadReason is the product-telemetry reason for an entry refused
	// because an event payload carried a key the privacy contract bans (email,
	// url, message, ...). Keeping that text would copy exactly the data the
	// contract refuses, so such a row keeps the digest and the sizes only.
	blockedPayloadReason = "blocked_telemetry_payload"
)

// boundedField returns value when it fits MaxDeadLetterFieldBytes, else a cut prefix (on a rune
// boundary) followed by a marker with the full length and digest.
func boundedField(value string) string {
	if len(value) <= MaxDeadLetterFieldBytes {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	marker := "...[cut " + strconv.Itoa(len(value)) + " bytes sha256:" + hex.EncodeToString(sum[:8]) + "]"
	cut := MaxDeadLetterFieldBytes - len(marker)
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + marker
}

// The field tables below are the ONE place that names which fields a dead-letter row carries and
// which of them the replay path reads. The writer (DeadLetterFields, replayFieldsFit), the replayer
// (MessageFromDeadLetter) and the every-field test all take their field sets from here, so a field
// added to the replay path joins the writer's bound and the test by being added to one table.

// identityFields are the Message.Fields keys every non-external dead-letter row copies, so a row can
// be reconciled against the Python contract (binding_id and event_id for PagerDuty).
var identityFields = []string{"ingestion_id", "org_id", "binding_id", "event_id"}

// replayFields are the Message.Fields keys (besides `events`) that MessageFromDeadLetter rebuilds. A
// row is `kept` only when every one of them fits MaxDeadLetterFieldBytes whole.
var replayFields = []string{"ingestion_id", "source", "org_id_hash"}

// replayRowFields are the row fields MessageFromDeadLetter is allowed to read. A test hands the
// replayer a reader that records every key it asks for and fails on one that is not listed here.
var replayRowFields = append([]string{"original_stream", "entry_id", eventsField, "events_retention", "events_sha256"}, replayFields...)

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
	case len(text) > MaxRetainedPayloadBytes || !replayFieldsFit(message):
		// A row whose replay fields (stream, entry id, ingestion id, source, org hash) would be cut cannot rebuild
		// the entry, so it keeps the digest and sizes instead of a text it could not replay.
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

func replayFieldsFit(message Message) bool {
	if len(message.Stream) > MaxDeadLetterFieldBytes || len(message.ID) > MaxDeadLetterFieldBytes {
		return false
	}
	for _, key := range replayFields {
		if len(message.Fields[key]) > MaxDeadLetterFieldBytes {
			return false
		}
	}
	return true
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
		if slices.Contains(identityFields, key) {
			fields[key] = value
		}
	}
	retention := retentionFor(message, reason)
	if retention.Mode == RetentionNone {
		if strings.HasPrefix(message.Stream, productTelemetryPrefix) {
			// A product-telemetry entry with no `events` (its source entry was trimmed from the
			// stream before it was quarantined) says plainly that there is nothing to replay.
			fields["events_retention"] = RetentionNone
		}
		return boundedFields(fields)
	}
	fields["events_retention"] = retention.Mode
	fields["events_bytes"] = strconv.Itoa(retention.Bytes)
	fields["events_sha256"] = retention.SHA256
	if retention.Mode != RetentionKept {
		return boundedFields(fields)
	}
	for _, key := range replayFields {
		if value, ok := message.Fields[key]; ok {
			fields[key] = value
		}
	}
	fields = boundedFields(fields)
	fields[eventsField] = message.Fields[eventsField]
	return fields
}

// boundedFields cuts every field to MaxDeadLetterFieldBytes, so the row is at most the retained
// `events` text plus a fixed number of bounded fields.
func boundedFields(fields map[string]string) map[string]string {
	for key, value := range fields {
		fields[key] = boundedField(value)
	}
	return fields
}

// MessageFromDeadLetter rebuilds the message a handler consumed from a
// dead-letter row, so a quarantined entry can be fed to the handler again. It
// reports false when the row did not keep the payload (over the bound, withheld
// or a family that never keeps it) or when the kept text no longer matches the
// recorded digest.
func MessageFromDeadLetter(row map[string]string) (Message, bool) {
	return replayFromRow(func(key string) (string, bool) {
		value, ok := row[key]
		return value, ok
	})
}

// replayFromRow is MessageFromDeadLetter over a reader, so a test can see every row field it reads.
func replayFromRow(read func(key string) (string, bool)) (Message, bool) {
	text, ok := read(eventsField)
	if retention, _ := read("events_retention"); !ok || retention != RetentionKept {
		return Message{}, false
	}
	sum := sha256.Sum256([]byte(text))
	if recorded, _ := read("events_sha256"); hex.EncodeToString(sum[:]) != recorded {
		return Message{}, false
	}
	fields := map[string]string{eventsField: text}
	for _, key := range replayFields {
		if value, ok := read(key); ok {
			fields[key] = value
		}
	}
	stream, _ := read("original_stream")
	id, _ := read("entry_id")
	return Message{Stream: stream, ID: id, Fields: fields}, true
}
