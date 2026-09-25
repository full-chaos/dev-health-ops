// Package legacyingest is the legacy metrics-ingest area of the api
// (api/ingest/router.py): POST /api/v1/ingest/{commits,pull-requests,
// work-items,deployments,incidents,telemetry}. A route validates a batch as
// its pydantic model does, buffers the model's JSON on a Valkey stream per
// org and kind (ingest:{org}:{kind}) that the stream runner's
// InternalIngestHandler consumes, and answers 202. Authentication is the
// route's own, not the api's: an API key and/or an HMAC of the raw body
// from the environment, failing closed when neither is configured outside
// a development environment (CHAOS-4720).
//
// The order is FastAPI's: the body is read and decoded first (a decode
// failure is answered before any dependency), then authentication (401),
// then the idempotency key (409), and only then the body's validation
// errors (422).
package legacyingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// streamMax is the stream's approximate length bound.
const streamMax = "100000"

// idempotencyTTLSeconds is how long an X-Idempotency-Key is remembered.
const idempotencyTTLSeconds = 86400

// Store is the Valkey operations the routes use.
type Store interface {
	// Append is XADD stream MAXLEN ~ 100000 * ingestion_id ... payload ...
	Append(ctx context.Context, stream, ingestionID, payload string) error
	// Claim is SET key 1 NX EX 86400: whether the key was newly set.
	Claim(ctx context.Context, key string) (bool, error)
}

// ValkeyStore is Store over a Valkey client.
type ValkeyStore struct{ Client valkeygo.Client }

// Append implements Store.
func (s ValkeyStore) Append(ctx context.Context, stream, ingestionID, payload string) error {
	command := s.Client.B().Xadd().Key(stream).Maxlen().Almost().Threshold(streamMax).Id("*").
		FieldValue().FieldValue("ingestion_id", ingestionID).FieldValue("payload", payload).Build()
	return s.Client.Do(ctx, command).Error()
}

// Claim implements Store.
func (s ValkeyStore) Claim(ctx context.Context, key string) (bool, error) {
	err := s.Client.Do(ctx, s.Client.B().Set().Key(key).Value("1").Nx().ExSeconds(idempotencyTTLSeconds).Build()).Error()
	if valkeygo.IsValkeyNil(err) {
		return false, nil
	}
	return err == nil, err
}

// Deps is what the area needs.
type Deps struct {
	// Store is nil when Valkey is not configured: batches are accepted and
	// not streamed, and no idempotency key is checked, as the Python api does
	// without REDIS_URL.
	Store Store
	// Metrics counts credential refusals; register it with the health
	// registry to expose it. Nil counts nothing.
	Metrics *Metrics
	// ClickHouse is nil when no ClickHouse is configured: telemetry batches
	// are accepted and not persisted (the Python route logs and skips).
	ClickHouse ClickHouse
	// Getenv reads the credentials and the environment name on every
	// request, as the Python api does; nil means os.Getenv.
	Getenv func(string) string
	Logger *slog.Logger
}

// route is one of the area's POST routes.
type route struct {
	path  string
	kind  string
	parse parser
	// telemetry routes write ClickHouse rows instead of streaming a payload.
	telemetry bool
}

var routes = []route{
	{path: "commits", kind: "commits", parse: batchParser(true, parseCommit)},
	{path: "pull-requests", kind: "pull-requests", parse: batchParser(true, parsePullRequest)},
	{path: "work-items", kind: "work-items", parse: batchParser(false, parseWorkItem)},
	{path: "deployments", kind: "deployments", parse: batchParser(true, parseDeployment)},
	{path: "incidents", kind: "incidents", parse: batchParser(true, parseIncident)},
	{path: "telemetry", kind: "telemetry", parse: parseTelemetry, telemetry: true},
}

// Routes returns the area's routes.
func Routes(deps Deps) []httpapi.Route {
	getenv := deps.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	out := make([]httpapi.Route, len(routes))
	for index, item := range routes {
		out[index] = httpapi.Route{
			Method:  http.MethodPost,
			Pattern: "/api/v1/ingest/" + item.path,
			Handler: handler{route: item, store: deps.Store, metrics: deps.Metrics, clickhouse: deps.ClickHouse, getenv: getenv, logger: logger},
		}
	}
	return out
}

type handler struct {
	route      route
	store      Store
	metrics    *Metrics
	clickhouse ClickHouse
	getenv     func(string) string
	logger     *slog.Logger
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			policy.WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
			return
		}
		h.logger.ErrorContext(r.Context(), "legacy ingest: read body failed", slog.String("error", err.Error()))
		policy.WriteInternal(w)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	body, outcome, failure, err := pybody.Read(r)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "legacy ingest: read body failed", slog.String("error", err.Error()))
		policy.WriteInternal(w)
		return
	}
	switch outcome {
	case pybody.DecodeFailed:
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
		return
	case pybody.ParseFailed:
		policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
		return
	}
	if !h.authenticate(w, r, raw) {
		return
	}
	if !h.claimIdempotencyKey(w, r) {
		return
	}
	parsed, problems := h.route.parse(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	if h.route.telemetry {
		h.telemetry(w, r, parsed)
		return
	}
	// model_dump_json runs before anything is written: a value it cannot
	// serialize (a lone surrogate) is the unhandled 500.
	payload, err := pyjson.MarshalModel(parsed.Dump)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "legacy ingest: payload could not be serialized", slog.String("error", err.Error()))
		policy.WriteInternal(w)
		return
	}
	ingestionID := uuid.NewString()
	stream := "ingest:" + parsed.OrgID + ":" + h.route.kind
	if !h.write(r.Context(), stream, ingestionID, string(payload)) {
		h.logger.WarnContext(r.Context(), "Ingest "+ingestionID+": Redis unavailable, payload not streamed")
	}
	out := pyjson.NewObject()
	out.Set("ingestion_id", ingestionID)
	out.Set("status", "accepted")
	out.Set("items_received", int64(parsed.Items))
	out.Set("stream", stream)
	policy.WriteModel(w, http.StatusAccepted, out, nil)
}

// write is write_to_stream: it reports whether the entry was written. A key
// or field the Python Redis client cannot encode as UTF-8 (a lone
// surrogate) is not written.
func (h handler) write(ctx context.Context, stream, ingestionID, payload string) bool {
	if h.store == nil || pyjson.HasSurrogate(stream) || pyjson.HasSurrogate(payload) {
		return false
	}
	if err := h.store.Append(ctx, stream, ingestionID, payload); err != nil {
		h.logger.ErrorContext(ctx, "Failed to write to stream "+stream, slog.String("error", err.Error()))
		return false
	}
	return true
}

// claimIdempotencyKey is check_idempotency: an X-Idempotency-Key is
// remembered for a day and a repeat is a 409; with no Valkey, or a Valkey
// that fails, the request goes on, as the Python api degrades.
func (h handler) claimIdempotencyKey(w http.ResponseWriter, r *http.Request) bool {
	key := r.Header.Get("X-Idempotency-Key")
	if key == "" || h.store == nil {
		return true
	}
	claimed, err := h.store.Claim(r.Context(), "idem:"+latin1(key))
	switch {
	case err != nil:
		h.logger.WarnContext(r.Context(), "Redis unavailable for idempotency check, skipping", slog.String("error", err.Error()))
		return true
	case !claimed:
		policy.WriteDetail(w, http.StatusConflict, "Duplicate request", nil)
		return false
	}
	return true
}

// telemetry is the telemetry route's tail: the rows are written to
// ClickHouse in one insert (skipped, with a warning, when none is
// configured); a failed insert is the unhandled 500. Nothing is streamed.
func (h handler) telemetry(w http.ResponseWriter, r *http.Request, parsed batch) {
	ingestionID := uuid.NewString()
	if h.clickhouse == nil {
		h.logger.WarnContext(r.Context(), "No ClickHouse URI configured, skipping telemetry persistence")
	} else if err := insertSignals(r.Context(), h.clickhouse, parsed.OrgID, parsed.Signals, time.Now().UTC()); err != nil {
		h.logger.ErrorContext(r.Context(), "legacy ingest: telemetry persistence failed", slog.String("error", err.Error()))
		policy.WriteInternal(w)
		return
	}
	out := pyjson.NewObject()
	out.Set("ingestion_id", ingestionID)
	out.Set("status", "accepted")
	out.Set("items_received", int64(parsed.Items))
	out.Set("stream", "ingest:"+parsed.OrgID+":telemetry")
	policy.WriteModel(w, http.StatusAccepted, out, nil)
}
