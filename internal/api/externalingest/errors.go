package externalingest

import (
	"encoding/json"
	"net/http"
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
// {"error": {"code": ..., "message": ..., "errors": [...]?}}.
func writeIngestError(w http.ResponseWriter, err *ingestError) {
	body := map[string]any{
		"error": map[string]any{"code": err.Code, "message": err.Message},
	}
	if len(err.Errors) > 0 {
		body["error"].(map[string]any)["errors"] = err.Errors
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
