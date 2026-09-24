// Package policy is the shared protected-route runtime of the dho api: the
// principal (who is calling), the request's org scope and impersonation, and
// the per-route authorization gates. It reproduces the Python api's
// get_current_user, OrgIdMiddleware, ImpersonationMiddleware, require_admin,
// require_superuser and get_admin_org_id (src/dev_health_ops/api), so a
// client cannot tell which plane answered.
//
// Area packages (internal/api/<area>) wrap their handlers with Guard and read
// the principal and scope from the request context. The two middlewares are
// installed once, around the whole mux, by internal/apiservice.
package policy

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// WriteJSON writes body as Starlette's JSONResponse renders it
// (pyjson.Marshal: json.dumps with ensure_ascii=False, compact separators).
// Objects are ordered *pyjson.Object values, never Go maps, so keys keep the
// order Python declares them in. extra headers are set before the status
// line.
func WriteJSON(w http.ResponseWriter, status int, body pyjson.Value, extra http.Header) {
	if status >= 200 && status < 300 {
		if responseModel, key, known := routeResponseModel(w); known && responseModel {
			recordWriterViolation("WriteJSON wrote a success body on a response_model route", key)
		}
	}
	payload, err := pyjson.Marshal(body)
	writePayload(w, status, payload, err, extra)
}

// WriteModel writes body as FastAPI writes a route's response_model
// (pyjson.MarshalModel: pydantic-core's dump_json). Use it for a route that
// declares a response model or a return annotation FastAPI takes as one;
// WriteJSON is the JSONResponse of a route without one.
func WriteModel(w http.ResponseWriter, status int, body pyjson.Value, extra http.Header) {
	if responseModel, key, known := routeResponseModel(w); known && !responseModel {
		recordWriterViolation("WriteModel wrote a body on a route without a response_model", key)
	}
	payload, err := pyjson.MarshalModel(body)
	writePayload(w, status, payload, err, extra)
	if err == nil {
		MarkModelBody(w)
	}
}

// MarkModelBody tells w's route that its body was written as a
// response_model (pydantic-core dump_json spelling). policy.WriteModel
// calls it; a writer that streams such a body itself calls it too.
func MarkModelBody(w io.Writer) {
	for w != nil {
		if route, ok := w.(interface{ MarkModelBody() }); ok {
			route.MarkModelBody()
			return
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return
		}
		w = unwrapper.Unwrap()
	}
}

// writePayload writes a serialized body. A body the Python api could not
// serialize either (a lone surrogate, a non-finite float json.dumps refuses)
// raises there, so the Python api answers its unhandled-exception 500; this
// logs the failure and answers that same 500 through WriteInternal, never a
// silent one.
func writePayload(w http.ResponseWriter, status int, payload []byte, err error, extra http.Header) {
	if err != nil {
		slog.Default().Error("api response: body could not be serialized",
			slog.Int("status", status), slog.String("error", err.Error()))
		WriteInternal(w)
		return
	}
	for key, values := range extra {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}

// WriteDetail writes FastAPI's HTTPException body, {"detail": detail}.
func WriteDetail(w http.ResponseWriter, status int, detail pyjson.Value, extra http.Header) {
	body := pyjson.NewObject()
	body.Set("detail", detail)
	WriteJSON(w, status, body, extra)
}

// ErrorDetail is api/utils/errors.py's error_detail(message): the
// {"message": ...} object some routes put under "detail".
func ErrorDetail(message string) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("message", message)
	return out
}

// UnhandledErrorHeader marks a response as the Python api's answer to an
// unhandled exception. Starlette's ServerErrorMiddleware, outside every
// other middleware, builds that response with only its own headers; the
// api's outermost middleware strips the others when it sees this marker
// (and removes the marker).
const UnhandledErrorHeader = "X-Dho-Unhandled-Error"

// WriteInternal writes the Python api's generic 500
// (api/_errors.py _generic_exception_handler) for an unhandled exception.
func WriteInternal(w http.ResponseWriter) {
	WriteDetail(w, http.StatusInternalServerError, "Internal Server Error", http.Header{UnhandledErrorHeader: {"1"}})
}

// routeResponseModel reads the ResponseModel flag of the registered route
// w serves (httpapi.RouteWriter), unwrapping writer wrappers; known is
// false for a writer outside any registered route.
func routeResponseModel(w http.ResponseWriter) (responseModel bool, key string, known bool) {
	for w != nil {
		if route, ok := w.(interface {
			ResponseModelRoute() bool
			RouteKey() string
		}); ok {
			return route.ResponseModelRoute(), route.RouteKey(), true
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false, "", false
		}
		w = unwrapper.Unwrap()
	}
	return false, "", false
}

var writerViolations atomic.Int64

// MarkStreamedBody tells w's route that its 2xx body was written in the
// Python api's own shape outside the response_model path: a
// StreamingResponse's error chunks, which FastAPI never renders through the
// model. It marks the body as MarkModelBody does, so the route is not
// counted as a wrong writer.
func MarkStreamedBody(w io.Writer) { MarkModelBody(w) }

// RecordMissingModelBody counts a 2xx response on a response_model route
// whose body did not go through WriteModel (or MarkModelBody): a handler
// that wrote it another way, which FastAPI never does.
func RecordMissingModelBody(route string) {
	recordWriterViolation("a 2xx body on a response_model route was not written as a response_model", route)
}

func recordWriterViolation(what, route string) {
	writerViolations.Add(1)
	slog.Error("policy: wrong body writer for the route; FastAPI writes this body the other way", "violation", what, "route", route)
}

// WriterViolations is how many times a handler wrote a body with the
// writer its route's ResponseModel flag does not allow. The venues assert
// it stays zero.
func WriterViolations() int64 { return writerViolations.Load() }
