package webhookintake

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/google/uuid"
)

// requiredHeaderErrors reports Python's Annotated[str, Header()]-required
// headers that are entirely ABSENT from the request -- one pybody.Error per
// missing header, {"type":"missing","loc":["header","<lowercase-name>"],
// "msg":"Field required","input":null}, the exact shape FastAPI's own
// RequestValidationError renders. Each pair is (canonical Go header name,
// Python's lowercase field name for "loc"). A header sent with an EMPTY
// value still counts as present: Python's declared type is a plain `str`
// with no length constraint, so "" satisfies it -- only a header key with
// no value at all is "missing", mirroring PagerDuty's own id: str field.
func requiredHeaderErrors(r *http.Request, headers ...[2]string) []pybody.Error {
	var errs []pybody.Error
	for _, header := range headers {
		if len(r.Header.Values(header[0])) == 0 {
			errs = append(errs, pybody.Error{
				Type: "missing", Loc: []pyjson.Value{"header", header[1]}, Msg: "Field required", Input: nil,
			})
		}
	}
	return errs
}

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

// writeIntLimit answers the Python api's unhandled ValueError for an integer
// literal past the 4300-digit limit: the routes catch only
// json.JSONDecodeError, so json.loads' ValueError is a generic 500.
func writeIntLimit(w http.ResponseWriter, err error) bool {
	var limit *pyjson.IntLimitError
	if !errors.As(err, &limit) {
		return false
	}
	policy.WriteInternal(w)
	return true
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

// webhookResponseBody builds {"status", "event_id", "message"} in that
// EXACT order -- models.py's WebhookResponse pydantic field declaration
// order (status, event_id, message), which FastAPI's response_model
// serializes in declared order, not alphabetically. A plain map[string]any
// through policy.WriteJSON sorts keys alphabetically (event_id, message,
// status), the same class of bug health.go's own handler had -- masked
// here until the venue oracle's normalizer itself stopped alphabetizing
// away every response containing event_id (which is all four: both
// accepted branches below).
func webhookResponseBody(status, eventID, message string) *pyjson.Object {
	body := pyjson.NewObject()
	body.Set("status", status)
	body.Set("event_id", eventID)
	body.Set("message", message)
	return body
}

// respondForEvent is the shared tail of the GitHub/GitLab/Jira handlers:
// an UNKNOWN event type is accepted without ever persisting or publishing
// (router.py's own early return), matching each handler's per-provider
// "Event type '...' not processed" message; a recognised event dispatches
// through delivery.go and answers 200.
func respondForEvent(w http.ResponseWriter, r *http.Request, d Deps, event webhookEvent, rawLabel string) {
	if event.EventType == eventUnknown {
		d.logger().Debug("Ignoring unsupported webhook event",
			"provider", event.Provider, "raw_event_type", sanitizeForLog(rawLabel))
		policy.WriteModel(w, http.StatusOK, webhookResponseBody(
			"accepted", uuid.New().String(), "Event type '"+sanitizeForLog(rawLabel)+"' not processed",
		), nil)
		return
	}
	deliveryID, err := dispatchWebhookDelivery(r.Context(), d.Pool, d.Producer, event)
	if err != nil {
		d.logger().Error("Webhook route/enqueue failed",
			"provider", event.Provider, "event_type", string(event.EventType), "error", err)
		policy.WriteDetail(w, http.StatusServiceUnavailable, "Webhook delivery could not be routed; retry", nil)
		return
	}
	// router.py logs "Dispatched webhook event" at Info on every successful
	// dispatch (_persist_and_route); this path had no equivalent, so a
	// regression that stopped dispatching GitHub/GitLab/Jira events
	// successfully would be invisible without turning on debug logging.
	d.logger().Info("Dispatched webhook event",
		"provider", event.Provider, "event_type", string(event.EventType), "delivery_id", deliveryID.String())
	policy.WriteModel(w, http.StatusOK, webhookResponseBody(
		"accepted", deliveryID.String(), "Processing "+string(event.EventType)+" event",
	), nil)
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
