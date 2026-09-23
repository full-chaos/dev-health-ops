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
	"encoding/json"
	"net/http"
	"strconv"
)

// WriteJSON writes body as Starlette's JSONResponse renders it: compact
// separators, no HTML escaping, UTF-8, and no trailing newline.
// extra headers are set before the status line.
func WriteJSON(w http.ResponseWriter, status int, body any, extra http.Header) {
	payload, err := Marshal(body)
	if err != nil {
		payload = []byte(`{"detail":"Internal Server Error"}`)
		status = http.StatusInternalServerError
	}
	for key, values := range extra {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

// Marshal renders body the way json.dumps(..., ensure_ascii=False,
// separators=(",", ":")) does for the values the api emits.
func Marshal(body any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// WriteDetail writes FastAPI's HTTPException body, {"detail": detail}.
func WriteDetail(w http.ResponseWriter, status int, detail any, extra http.Header) {
	WriteJSON(w, status, map[string]any{"detail": detail}, extra)
}

// ErrorDetail is api/utils/errors.py's error_detail(message): the
// {"message": ...} object some routes put under "detail".
func ErrorDetail(message string) map[string]any {
	return map[string]any{"message": message}
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
