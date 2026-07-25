// Package streamhandlers contains concrete, idempotent durable-write handlers
// for the existing stream payload contracts.
package streamhandlers

import (
	"context"
	"encoding/json"
	"fmt"
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

type productEvent struct {
	Name            string         `json:"name"`
	SchemaVersion   string         `json:"schemaVersion"`
	EventID         string         `json:"eventId"`
	Timestamp       time.Time      `json:"ts"`
	SessionID       string         `json:"sessionId"`
	AnonymousUserID string         `json:"anonymousUserId"`
	OrgIDHash       string         `json:"orgIdHash"`
	RoutePattern    *string        `json:"routePattern"`
	Payload         map[string]any `json:"payload"`
}

type productClickHouse interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
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
	var events []productEvent
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
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

	batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO product_telemetry_events (org_id_hash,event_id,name,schema_version,session_id,anonymous_user_id,route_pattern,payload_json,occurred_at,ingested_at,source)")
	if err != nil {
		return fmt.Errorf("prepare product telemetry sink: %w", err)
	}
	for _, event := range events {
		payload, err := validateProductEvent(event)
		if err != nil {
			return err
		}
		if err := batch.Append(event.OrgIDHash, event.EventID, event.Name, event.SchemaVersion, event.SessionID, event.AnonymousUserID, event.RoutePattern, payload, event.Timestamp.UTC(), time.Now().UTC(), source); err != nil {
			return fmt.Errorf("append product telemetry: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("persist product telemetry: %w", err)
	}
	return nil
}

func validateProductEvent(event productEvent) (string, error) {
	if _, ok := productTelemetryNames[event.Name]; !ok || event.SchemaVersion == "" || event.EventID == "" || event.Timestamp.IsZero() || event.SessionID == "" || event.AnonymousUserID == "" || event.Payload == nil {
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
