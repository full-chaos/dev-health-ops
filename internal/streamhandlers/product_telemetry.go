// Package streamhandlers contains concrete, idempotent durable-write handlers
// for the existing stream payload contracts.
//
// The provider-event-to-graph-edge pathway these handlers sit on -- external
// batch, kind registry refusal, ClickHouse sink, project_membership_transitions,
// the presence projection, and the Context Fabric edge that reads it -- is
// drawn in docs/contribute/architecture/data-and-storage.md, under "Work item
// to project: provider event to graph edge". Read it before adding a kind: the
// diagram marks where a value can be LOST, not only where it flows.
package streamhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

var productTelemetryNames = map[string]struct{}{
	"page_viewed": {}, "feature_viewed": {}, "filter_changed": {}, "chart_interacted": {},
	"navigation_interacted": {}, "guide_opened": {}, "session_started": {}, "session_ended": {}, "client_error": {},
}

var blockedProductPayloadKeys = map[string]struct{}{
	"email": {}, "name": {}, "userId": {}, "orgId": {}, "url": {}, "query": {}, "search": {}, "stack": {}, "message": {}, "title": {}, "body": {},
}

// A timestamp is stored only inside the range the ClickHouse Go driver encodes correctly: it converts a
// time to int64 nanoseconds, so a time before 1677-09-21 or after 2262-04-11 overflows and is stored as a
// different, wrong time (a year-2500 timestamp came back as 1915; the zero time as 1970). The Python
// consumer stored every year ClickHouse's DateTime64 holds, so these are the entries that cannot be
// stored as sent: they are refused, as before the intake shapes were accepted, and stay replayable from
// their dead-letter row.
var (
	minStorableTimestamp = time.Unix(0, math.MinInt64).UTC()
	maxStorableTimestamp = time.Unix(0, math.MaxInt64).UTC()
)

func timestampStorable(t time.Time) bool {
	return !t.Before(minStorableTimestamp) && !t.After(maxStorableTimestamp)
}

// presentString is a required JSON string that may be empty: pydantic's `str` field accepts "" and
// refuses a missing or null value, and so does this type (Set is false for both).
type presentString struct {
	Value string
	Set   bool
}

func (p *presentString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	if err := json.Unmarshal(data, &p.Value); err != nil {
		return err
	}
	p.Set = true
	return nil
}

// eventTime is the `ts` the intake writes: pydantic's JSON form of a datetime, which carries a zone
// ("Z" or an offset) when the client sent one and none when it did not. A value with no zone was
// stored as the UTC time it names by the Python consumer (its persist step kept a naive datetime
// as it was), so it is read as UTC here.
type eventTime struct {
	Time time.Time
	Set  bool
}

func (e *eventTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		var zoneErr error
		parsed, zoneErr = time.ParseInLocation("2006-01-02T15:04:05.999999999", text, time.UTC)
		if zoneErr != nil {
			return err
		}
	}
	e.Time, e.Set = parsed, true
	return nil
}

type productEvent struct {
	Name            string         `json:"name"`
	SchemaVersion   presentString  `json:"schemaVersion"`
	EventID         presentString  `json:"eventId"`
	Timestamp       eventTime      `json:"ts"`
	SessionID       presentString  `json:"sessionId"`
	AnonymousUserID presentString  `json:"anonymousUserId"`
	OrgIDHash       string         `json:"orgIdHash"`
	RoutePattern    *string        `json:"routePattern"`
	Payload         map[string]any `json:"payload"`
}

type productClickHouse interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
	// Query is narrow and single-purpose (CHAOS-4321, team-lead ruling
	// 2026-08-26): ClickHouseExternalBatchSink's team.v1 write needs to read
	// a team's current manual_members before overwriting the row, the same
	// preserve-on-write guard ClickHouseStore.insert_teams already applies
	// in Python -- an admin-override column that one write path can
	// silently erase is not shippable. This does not widen what
	// ProductTelemetryHandler can do; it only reads, and the real
	// driver.Conn (this interface's only production implementation)
	// already satisfies it with zero adapter changes.
	Query(context.Context, string, ...any) (driver.Rows, error)
}

type ProductTelemetryHandler struct{ conn productClickHouse }

func NewProductTelemetryHandler(conn productClickHouse) (*ProductTelemetryHandler, error) {
	if conn == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &ProductTelemetryHandler{conn: conn}, nil
}

func (h *ProductTelemetryHandler) Handle(ctx context.Context, message streamrunner.Message) error {
	raw, ok := message.Fields["events"]
	if !ok {
		return &streamrunner.PermanentError{Reason: "missing_events"}
	}
	events, nonFinite, err := decodeProductEvents(raw)
	if err != nil {
		return &streamrunner.PermanentError{Reason: "invalid_events_json"}
	}
	if len(events) == 0 || len(events) > 500 {
		return &streamrunner.PermanentError{Reason: "invalid_event_count"}
	}
	source := message.Fields["source"]
	if source == "" {
		source = "dev-health-web"
	}
	if source != "dev-health-web" {
		return &streamrunner.PermanentError{Reason: "invalid_telemetry_source"}
	}

	// Validate the whole entry before a batch is opened: a refused entry must not hold a
	// ClickHouse connection (an open, never-sent batch keeps one until it is aborted).
	payloads := make([]string, len(events))
	for index, event := range events {
		payload, err := validateProductEvent(event)
		if err != nil {
			return err
		}
		payloads[index] = payload
	}

	batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO product_telemetry_events (org_id_hash,event_id,name,schema_version,session_id,anonymous_user_id,route_pattern,payload_json,occurred_at,ingested_at,source)")
	if err != nil {
		return fmt.Errorf("prepare product telemetry sink: %w", err)
	}
	// Every exit after PrepareBatch that does not send must release the batch's connection.
	sent := false
	defer func() {
		if !sent {
			_ = batch.Abort()
		}
	}()
	for index, event := range events {
		if err := batch.Append(event.OrgIDHash, event.EventID.Value, event.Name, event.SchemaVersion.Value, event.SessionID.Value, event.AnonymousUserID.Value, event.RoutePattern, payloads[index], event.Timestamp.Time.UTC(), time.Now().UTC(), source); err != nil {
			return fmt.Errorf("append product telemetry: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("persist product telemetry: %w", err)
	}
	sent = true
	// Only now, with the whole entry validated and durably written, are the nulled numbers
	// "stored": counting earlier overstated storage for an entry refused or retried.
	recordNonFiniteNulled(ctx, message, nonFinite)
	return nil
}

func validateProductEvent(event productEvent) (string, error) {
	if _, ok := productTelemetryNames[event.Name]; !ok || !event.SchemaVersion.Set || !event.EventID.Set || !event.Timestamp.Set || !timestampStorable(event.Timestamp.Time) || !event.SessionID.Set || !event.AnonymousUserID.Set || event.Payload == nil {
		return "", &streamrunner.PermanentError{Reason: "invalid_telemetry_event"}
	}
	for key, value := range event.Payload {
		if _, blocked := blockedProductPayloadKeys[key]; blocked {
			return "", &streamrunner.PermanentError{Reason: "blocked_telemetry_payload"}
		}
		switch value.(type) {
		case nil, string, bool, float64:
		default:
			return "", &streamrunner.PermanentError{Reason: "invalid_telemetry_payload"}
		}
	}
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return "", &streamrunner.PermanentError{Reason: "invalid_telemetry_payload"}
	}
	return string(payload), nil
}

// ProductTelemetryColumns is exported for focused sink contract tests.
var ProductTelemetryColumns = strings.Split("org_id_hash,event_id,name,schema_version,session_id,anonymous_user_id,route_pattern,payload_json,occurred_at,ingested_at,source", ",")
