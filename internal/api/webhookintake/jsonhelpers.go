package webhookintake

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/google/uuid"
)

// objectField reads a nested *pyjson.Object field, nil-safe at every step
// (a payload whose shape doesn't match what a provider normally sends
// degrades to "field absent", matching Python's dict.get(key, {}) chains).
func objectField(object *pyjson.Object, name string) *pyjson.Object {
	if object == nil {
		return nil
	}
	value, ok := object.Get(name)
	if !ok {
		return nil
	}
	nested, _ := value.(*pyjson.Object)
	return nested
}

// stringField reads a string field, "" if absent, nil, or not a string
// (Python's payload.get(key, "") / payload.get(key) is None-safe the same
// way).
func stringField(object *pyjson.Object, name string) string {
	if object == nil {
		return ""
	}
	value, ok := object.Get(name)
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

// stringPtrField is stringField, but nil (not "") when the field is absent
// -- for columns Python leaves NULL rather than empty-string.
func stringPtrField(object *pyjson.Object, name string) *string {
	if object == nil {
		return nil
	}
	value, ok := object.Get(name)
	if !ok || value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return nil
	}
	return &text
}

// decodeJSONBody applies pyjson's json.loads-compatible pre-decode (BOM/
// UTF-16/UTF-32 detection) then parses, the way pybody.Read does for the
// JSON content-type branch -- either failure means "invalid JSON payload"
// to every caller here, which report it identically (router.py's own
// json.JSONDecodeError branch for each provider, one uniform 400).
func decodeJSONBody(body []byte) (pyjson.Value, error) {
	text, err := pyjson.DecodeBody(body)
	if err != nil {
		return nil, err
	}
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("empty JSON body")
	}
	return value, nil
}

// respondForEvent is the shared tail of the GitHub/GitLab/Jira handlers:
// an UNKNOWN event type is accepted without ever persisting or publishing
// (router.py's own early return), matching each handler's per-provider
// "Event type '...' not processed" message; a recognised event dispatches
// through delivery.go and answers 200.
func respondForEvent(w http.ResponseWriter, r *http.Request, d Deps, event webhookEvent, rawLabel string) {
	if event.EventType == eventUnknown {
		policy.WriteJSON(w, http.StatusOK, map[string]any{
			"status":   "accepted",
			"event_id": uuid.New().String(),
			"message":  "Event type '" + sanitizeForLog(rawLabel) + "' not processed",
		}, nil)
		return
	}
	deliveryID, err := dispatchWebhookDelivery(r.Context(), d.Pool, d.Producer, event)
	if err != nil {
		d.logger().Error("Webhook route/enqueue failed",
			"provider", event.Provider, "event_type", string(event.EventType), "error", err)
		policy.WriteDetail(w, http.StatusServiceUnavailable, "Webhook delivery could not be routed; retry", nil)
		return
	}
	policy.WriteJSON(w, http.StatusOK, map[string]any{
		"status":   "accepted",
		"event_id": deliveryID.String(),
		"message":  "Processing " + string(event.EventType) + " event",
	}, nil)
}

// sanitizeForLog strips CR/LF the way router.py's own
// x_github_event.replace("\r","").replace("\n","") does before logging or
// echoing an unrecognised event name back to the caller.
func sanitizeForLog(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\r' || r == '\n' {
			continue
		}
		out = append(out, r)
	}
	return string(out)
}
