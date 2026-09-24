// Package producttelemetry is plan area K's public event intake
// (api/product_telemetry): POST /api/v1/product-telemetry/events validates
// a ProductTelemetryBatch as pydantic does and appends it to the
// product-telemetry Valkey stream the Go stream runner consumes.
package producttelemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// eventNames is TelemetryEventName, in declaration order.
var eventNames = []string{
	"page_viewed", "feature_viewed", "filter_changed", "chart_interacted", "navigation_interacted",
	"guide_opened", "session_started", "session_ended", "client_error",
}

const (
	source     = "dev-health-web"
	maxEvents  = 500
	streamMax  = "100000"
	streamName = "product-telemetry:%s:events"
)

// Streams appends one batch to a stream. ValkeyStreams is the production
// implementation.
type Streams interface {
	Append(ctx context.Context, stream string, fields [][2]string) error
}

// ValkeyStreams opens its client on first use from VALKEY_URI ("" = no
// stream: every batch is accepted with stream "disabled", as the Python
// api does without REDIS_URL). A failed open is not cached: the next
// batch tries again, as the Python client reconnects per command. The
// open runs under its own bounded context, so a cancelled request cannot
// fail it for later ones.
type ValkeyStreams struct {
	URI string

	mu     sync.Mutex
	client valkeygo.Client
}

// openTimeout bounds one open attempt (dial + ping).
const openTimeout = 5 * time.Second

var (
	errNoStream    = errors.New("producttelemetry: Valkey is not configured")
	errUnencodable = errors.New("producttelemetry: stream key or field is not encodable as UTF-8")
)

func (s *ValkeyStreams) open() (valkeygo.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return s.client, nil
	}
	config := valkey.DefaultConfig(s.URI)
	config.ClientName = "dev-health-api"
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()
	client, err := valkey.Open(ctx, config)
	if err != nil {
		return nil, err
	}
	s.client = client
	return client, nil
}

// Append is XADD stream MAXLEN ~ 100000 * field value ...
func (s *ValkeyStreams) Append(ctx context.Context, stream string, fields [][2]string) error {
	if s.URI == "" {
		return errNoStream
	}
	client, err := s.open()
	if err != nil {
		return err
	}
	command := client.B().Xadd().Key(stream).Maxlen().Almost().Threshold(streamMax).Id("*").FieldValue()
	for _, field := range fields {
		command = command.FieldValue(field[0], field[1])
	}
	return client.Do(ctx, command.Build()).Error()
}

// Routes returns the area's route. It is public: FastAPI runs no
// dependency before the body, so any validation error is the 422.
func Routes(streams Streams, logger *slog.Logger) []httpapi.Route {
	h := handler{streams: streams, logger: logger}
	return []httpapi.Route{{Method: http.MethodPost, Pattern: "/api/v1/product-telemetry/events", Handler: h}}
}

type handler struct {
	streams Streams
	logger  *slog.Logger
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, outcome, failure, err := pybody.Read(r)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			policy.WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
			return
		}
		h.logger.ErrorContext(r.Context(), "product telemetry: read body failed", slog.String("error", err.Error()))
		policy.WriteInternal(w)
		return
	}
	switch outcome {
	case pybody.DecodeFailed:
		writeJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}))
		return
	case pybody.ParseFailed:
		policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
		return
	}
	batch, problems := validateBatch(body)
	if len(problems) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems))
		return
	}
	ingestionID := uuid.NewString()
	stream, err := h.write(r.Context(), batch, ingestionID)
	if err != nil {
		h.logger.WarnContext(r.Context(), "Product telemetry stream unavailable", slog.String("error", err.Error()))
		stream = "disabled"
	}
	out := pyjson.NewObject()
	out.Set("ingestion_id", ingestionID)
	out.Set("status", "accepted")
	out.Set("items_received", int64(len(batch.events)))
	out.Set("stream", stream)
	writeJSON(w, http.StatusAccepted, out)
}

func (h handler) write(ctx context.Context, batch batch, ingestionID string) (string, error) {
	key := "anonymous"
	if batch.orgIDHash != nil && *batch.orgIDHash != "" {
		key = *batch.orgIDHash
	}
	stream := strings.Replace(streamName, "%s", key, 1)
	events := make([]pyjson.Value, len(batch.events))
	for index, event := range batch.events {
		events[index] = event
	}
	encoded, err := pyjson.Dumps(events)
	if err != nil {
		return "", err
	}
	orgHash := ""
	if batch.orgIDHash != nil {
		orgHash = *batch.orgIDHash
	}
	if h.streams == nil {
		return "", errNoStream
	}
	// The Python Redis client UTF-8 encodes the key and every field before
	// it sends anything; a lone surrogate fails that encode, so no entry is
	// written and the batch is accepted with stream "disabled".
	if pyjson.HasSurrogate(stream) || pyjson.HasSurrogate(orgHash) {
		return "", errUnencodable
	}
	err = h.streams.Append(ctx, stream, [][2]string{
		{"ingestion_id", ingestionID}, {"source", batch.source}, {"org_id_hash", orgHash}, {"events", encoded},
	})
	return stream, err
}

// writeJSON is the shared JSONResponse writer (policy.WriteJSON), which
// logs a body it cannot serialize and answers the unhandled-error 500.
func writeJSON(w http.ResponseWriter, status int, body pyjson.Value) {
	policy.WriteJSON(w, status, body, nil)
}

type batch struct {
	orgIDHash *string
	source    string
	events    []*pyjson.Object // each already in model_dump(mode="json", by_alias=True) form
}

// field reads a populate_by_name field: the alias wins when both are
// present; the location names whichever key was read (the alias when
// neither is).
func field(object *pyjson.Object, alias, name string) (pyjson.Value, string, bool) {
	if value, ok := object.Get(alias); ok {
		return value, alias, true
	}
	if name != alias {
		if value, ok := object.Get(name); ok {
			return value, name, true
		}
	}
	return nil, alias, false
}

func loc(prefix []pyjson.Value, parts ...pyjson.Value) []pyjson.Value {
	return append(append([]pyjson.Value(nil), prefix...), parts...)
}

func literal(expected []string) string {
	quoted := make([]string, len(expected))
	for index, value := range expected {
		quoted[index] = "'" + value + "'"
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
}

func literalError(location []pyjson.Value, input pyjson.Value, expected []string) pybody.Error {
	ctx := pyjson.NewObject()
	text := literal(expected)
	ctx.Set("expected", text)
	return pybody.Error{Type: "literal_error", Loc: location, Msg: "Input should be " + text, Input: input, Ctx: ctx}
}

func missing(location []pyjson.Value, input pyjson.Value) pybody.Error {
	return pybody.Error{Type: "missing", Loc: location, Msg: "Field required", Input: input}
}

// optionalString is `str | None`: present strings and null pass; any other
// value is string_type.
func optionalString(problems *[]pybody.Error, object *pyjson.Object, prefix []pyjson.Value, alias, name string) (*string, bool) {
	value, key, ok := field(object, alias, name)
	if !ok || value == nil {
		return nil, true
	}
	if text, isString := value.(string); isString {
		return &text, true
	}
	*problems = append(*problems, pybody.Error{Type: "string_type", Loc: loc(prefix, key), Msg: "Input should be a valid string", Input: value})
	return nil, false
}

func requiredString(problems *[]pybody.Error, object *pyjson.Object, prefix []pyjson.Value, alias, name string) (string, bool) {
	value, key, ok := field(object, alias, name)
	if !ok {
		*problems = append(*problems, missing(loc(prefix, key), object))
		return "", false
	}
	if text, isString := value.(string); isString {
		return text, true
	}
	*problems = append(*problems, pybody.Error{Type: "string_type", Loc: loc(prefix, key), Msg: "Input should be a valid string", Input: value})
	return "", false
}

func validateBatch(body pybody.Body) (batch, []pybody.Error) {
	var problems []pybody.Error
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		return batch{}, errs
	}
	root := []pyjson.Value{"body"}
	out := batch{source: source}
	out.orgIDHash, _ = optionalString(&problems, object, root, "orgIdHash", "org_id_hash")
	if value, present := object.Get("source"); present {
		if text, isString := value.(string); isString && text == source {
			out.source = text
		} else {
			problems = append(problems, literalError(loc(root, "source"), value, []string{source}))
		}
	}
	value, present := object.Get("events")
	switch list, isList := value.([]pyjson.Value); {
	case !present:
		problems = append(problems, missing(loc(root, "events"), object))
	case !isList:
		problems = append(problems, pybody.Error{Type: "list_type", Loc: loc(root, "events"), Msg: "Input should be a valid list", Input: value})
	case len(list) < 1:
		ctx := pyjson.NewObject()
		ctx.Set("field_type", "List")
		ctx.Set("min_length", int64(1))
		ctx.Set("actual_length", int64(0))
		problems = append(problems, pybody.Error{Type: "too_short", Loc: loc(root, "events"),
			Msg: "List should have at least 1 item after validation, not 0", Input: list, Ctx: ctx})
	case len(list) > maxEvents:
		ctx := pyjson.NewObject()
		ctx.Set("field_type", "List")
		ctx.Set("max_length", int64(maxEvents))
		ctx.Set("actual_length", int64(len(list)))
		problems = append(problems, pybody.Error{Type: "too_long", Loc: loc(root, "events"),
			Msg: "List should have at most 500 items after validation, not " + strconv.Itoa(len(list)), Input: list, Ctx: ctx})
	default:
		for index, item := range list {
			if event, ok := validateEvent(&problems, item, loc(root, "events", int64(index))); ok {
				out.events = append(out.events, event)
			}
		}
	}
	return out, problems
}

func validateEvent(problems *[]pybody.Error, item pyjson.Value, prefix []pyjson.Value) (*pyjson.Object, bool) {
	object, isObject := item.(*pyjson.Object)
	if !isObject {
		// FastAPI validates the body with from_attributes, so a non-object
		// event is model_attributes_type, not model_type.
		*problems = append(*problems, pybody.Error{Type: "model_attributes_type", Loc: prefix,
			Msg: "Input should be a valid dictionary or object to extract fields from", Input: item})
		return nil, false
	}
	before := len(*problems)
	out := pyjson.NewObject()

	if value, key, ok := field(object, "name", "name"); !ok {
		*problems = append(*problems, missing(loc(prefix, key), object))
	} else if text, isString := value.(string); isString && contains(eventNames, text) {
		out.Set("name", text)
	} else {
		*problems = append(*problems, literalError(loc(prefix, key), value, eventNames))
	}
	for _, pair := range [][3]string{{"schemaVersion", "schema_version", "schemaVersion"}, {"eventId", "event_id", "eventId"}} {
		if text, ok := requiredString(problems, object, prefix, pair[0], pair[1]); ok {
			out.Set(pair[2], text)
		}
	}
	if value, key, ok := field(object, "ts", "ts"); !ok {
		*problems = append(*problems, missing(loc(prefix, key), object))
	} else if parsed, failure := pytime.ParseDatetime(timeInput(value)); failure != nil {
		ctx := pyjson.NewObject()
		reason := strings.TrimPrefix(strings.TrimPrefix(failure.Msg, "Input should be a valid datetime or date, "), "Input should be a valid datetime, ")
		ctx.Set("error", reason)
		problem := pybody.Error{Type: failure.Type, Loc: loc(prefix, key), Msg: failure.Msg, Input: value}
		if failure.Type != "datetime_type" {
			problem.Ctx = ctx
		}
		*problems = append(*problems, problem)
	} else {
		out.Set("ts", pytime.Pydantic(parsed))
	}
	for _, pair := range [][3]string{{"sessionId", "session_id", "sessionId"}, {"anonymousUserId", "anonymous_user_id", "anonymousUserId"}} {
		if text, ok := requiredString(problems, object, prefix, pair[0], pair[1]); ok {
			out.Set(pair[2], text)
		}
	}
	for _, pair := range [][3]string{{"orgIdHash", "org_id_hash", "orgIdHash"}, {"routePattern", "route_pattern", "routePattern"}} {
		if text, ok := optionalString(problems, object, prefix, pair[0], pair[1]); ok {
			if text != nil {
				out.Set(pair[2], *text)
			} else {
				out.Set(pair[2], nil)
			}
		}
	}
	if value, key, ok := field(object, "payload", "payload"); !ok {
		*problems = append(*problems, missing(loc(prefix, key), object))
	} else if payload, ok := validatePayload(problems, value, loc(prefix, key)); ok {
		out.Set("payload", payload)
	}
	return out, len(*problems) == before
}

func timeInput(value pyjson.Value) any {
	switch typed := value.(type) {
	case pyjson.Int:
		return typed.Int
	case pyjson.Float:
		return float64(typed)
	default:
		return value
	}
}

// validatePayload is dict[str, str | int | float | bool | None] in smart
// union mode: a JSON scalar keeps its own type; a list or object fails every
// member.
func validatePayload(problems *[]pybody.Error, value pyjson.Value, prefix []pyjson.Value) (*pyjson.Object, bool) {
	object, isObject := value.(*pyjson.Object)
	if !isObject {
		*problems = append(*problems, pybody.Error{Type: "dict_type", Loc: prefix, Msg: "Input should be a valid dictionary", Input: value})
		return nil, false
	}
	ok := true
	for _, key := range object.Keys() {
		item, _ := object.Get(key)
		switch item.(type) {
		case nil, string, bool, pyjson.Int, pyjson.Float:
		default:
			ok = false
			for _, member := range [][3]string{
				{"str", "string_type", "Input should be a valid string"},
				{"int", "int_type", "Input should be a valid integer"},
				{"float", "float_type", "Input should be a valid number"},
				{"bool", "bool_type", "Input should be a valid boolean"},
			} {
				*problems = append(*problems, pybody.Error{Type: member[1], Loc: loc(prefix, key, member[0]), Msg: member[2], Input: item})
			}
		}
	}
	return object, ok
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
