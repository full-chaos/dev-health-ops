package externalingest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"unicode"

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

// pythonRepr renders s the way CPython's str.__repr__ (what f"{s!r}" and an
// explicit '{s}' literal both call) renders it: single-quoted unless s
// contains a single quote and no double quote (then double-quoted, no
// escaping needed for the embedded '), backslash/quote/control characters
// backslash-escaped, other non-printable runes as \xHH/\uHHHH/\UHHHHHHHH,
// everything else passed through. A first version always single-quoted
// unconditionally -- round 1 review reproduced the divergence live:
// bad'version rendered as "bad'version" from Python (repr switches to
// double quotes rather than escape the embedded ') and as 'bad'version'
// (unescaped, wrong) from the naive version.
func pythonRepr(s string) string {
	quote := byte('\'')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case byte(r) == quote && r < 0x80:
			b.WriteByte('\\')
			b.WriteByte(quote)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case unicode.IsPrint(r):
			b.WriteRune(r)
		case r <= 0xff:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteByte(quote)
	return b.String()
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
