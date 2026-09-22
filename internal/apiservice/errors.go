package apiservice

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// pythonError is one error response exactly as the Python api renders it.
type pythonError struct {
	status int
	body   any
}

// pythonErrors maps every transport error code to the Python api's own
// response for the same condition, so a client cannot tell which plane
// answered:
//
//   - not found / method not allowed: Starlette's HTTPException handler,
//     {"detail": "<reason phrase>"};
//   - rate limited: src/dev_health_ops/api/_errors.py _rate_limit_handler;
//   - internal: _errors.py _generic_exception_handler;
//   - payload too large / invalid request: the Python api has no such bound, so
//     the body uses the same {"detail": "<reason phrase>"} shape.
var pythonErrors = map[httpapi.Code]pythonError{
	httpapi.CodeNotFound:         {http.StatusNotFound, map[string]string{"detail": "Not Found"}},
	httpapi.CodeMethodNotAllowed: {http.StatusMethodNotAllowed, map[string]string{"detail": "Method Not Allowed"}},
	httpapi.CodePayloadTooLarge:  {http.StatusRequestEntityTooLarge, map[string]string{"detail": "Request Entity Too Large"}},
	httpapi.CodeInvalidRequest:   {http.StatusBadRequest, map[string]string{"detail": "Bad Request"}},
	httpapi.CodeRateLimited: {http.StatusTooManyRequests, map[string]any{
		"detail": map[string]string{"message": "Rate limit exceeded. Please try again later."},
	}},
	httpapi.CodeInternal: {http.StatusInternalServerError, map[string]string{"detail": "Internal Server Error"}},
}

// WriteError is the api's httpapi.ErrorWriter. A code with no Python mapping
// is a programming error and renders as the internal error, the least
// informative response.
func WriteError(w http.ResponseWriter, _ *http.Request, code httpapi.Code) {
	response, known := pythonErrors[code]
	if !known {
		response = pythonErrors[httpapi.CodeInternal]
	}
	payload, err := json.Marshal(response.body)
	if err != nil {
		// The bodies above are fixed string maps; Marshal cannot fail on them.
		payload = []byte(`{"detail":"Internal Server Error"}`)
		response.status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(response.status)
	writeFixedBody(w, payload)
}

// writeFixedBody writes one of this package's own fixed bodies: the JSON
// error bodies above and the CORS preflight text. None carries request data,
// each response declares its Content-Type (application/json or text/plain)
// and every api response carries X-Content-Type-Options: nosniff, so there is
// nothing for an HTML escape to protect. The copy through io.Copy keeps the
// bytes exact: json.Encoder would append a newline the Python api does not
// send.
func writeFixedBody(w http.ResponseWriter, body []byte) {
	_, _ = io.Copy(w, bytes.NewReader(body))
}
