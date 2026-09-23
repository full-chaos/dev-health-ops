package externalingest

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ingestError mirrors errors.py's ExternalIngestError: the customer-facing
// envelope for every /api/v1/external-ingest/* error response, distinct
// from the app-wide {"detail": ...} shape (ACP-ADR-01's envelope, which the
// Python api reserves for internal/session routes).
type ingestError struct {
	Status  int
	Code    string
	Message string
	Errors  []ValidationErrorItem
}

func (e *ingestError) Error() string { return e.Message }

func newIngestError(status int, code, message string) *ingestError {
	return &ingestError{Status: status, Code: code, Message: message}
}

// writeIngestError renders errors.py's external_ingest_error_body:
// {"error": {"code": ..., "message": ..., "errors": [...]?}}. An explicitly
// ordered *pyjson.Object through policy.WriteJSON, not a map[string]any:
// stdlib encoding/json alphabetizes map keys on the way out, which would
// silently diverge from Python's declared field order the moment this body
// carried more than one key whose alphabetical and declared order differ
// (they don't today, by coincidence -- code before message before errors is
// already alphabetical -- which is exactly the kind of accident this class
// of fix exists to stop relying on).
// pythonRepr renders s the way Python's f"{s!r}" (or an explicit '{s}'
// literal) does for the plain, quote-free strings these error messages
// ever hold (schema versions, record kind names, idempotency keys,
// ingestion ids) -- a single-quoted literal, not Go's %q double-quoted
// form. Confirmed a real divergence by the venue oracle, not inferred: the
// live Python response used single quotes where the Go response's %q
// produced double quotes for the identical case.
func pythonRepr(s string) string {
	return "'" + s + "'"
}

func writeIngestError(w http.ResponseWriter, err *ingestError) {
	errorObject := pyjson.NewObject()
	errorObject.Set("code", err.Code)
	errorObject.Set("message", err.Message)
	if len(err.Errors) > 0 {
		items := make([]pyjson.Value, len(err.Errors))
		for i, item := range err.Errors {
			items[i] = item.toPyJSON()
		}
		errorObject.Set("errors", items)
	}
	body := pyjson.NewObject()
	body.Set("error", errorObject)
	policy.WriteJSON(w, err.Status, body, nil)
}

// writeUnorderedJSON is the ONE remaining stdlib-map writer in this package,
// used solely by GET /schemas/{schema_version}'s schema-bundle document
// (handlers.go): that body is a generated JSON Schema document
// (pydantic.json_schema.models_json_schema(), arbitrary $defs depth), not a
// hand-declared response shape with a known field order to preserve, and
// nothing here re-derives Python's live dict-insertion order from the
// checked-in golden fixture (which was itself written with sort_keys=True
// for content-addressed storage, not to record wire order). NAMED LIMIT,
// not fixed by this change -- see bundle.go's schemaDocument doc comment.
func writeUnorderedJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// recoverToIngestError wraps a handler so a panic on this route group still
// answers with this package's own {"error": {...}} envelope (errors.py's
// shape for this prefix), not the app-wide {"detail": ...} shape the shared
// transport middleware writes for every other area. It must run BEFORE that
// shared middleware sees anything on this prefix, so it recovers here rather
// than relying on internal/auth/httpapi's own panic recovery.
func recoverToIngestError(logger *slog.Logger, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if logger != nil {
					logger.Error("external-ingest: unhandled panic", "panic", rec, "path", r.URL.Path)
				}
				writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "internal server error"))
			}
		}()
		next(w, r)
	}
}
