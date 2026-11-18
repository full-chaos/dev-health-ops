package admin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// dry_run is read the way FastAPI reads it: a ";" or a bad escape stays in
// the value (422 bool_parsing), and a pair holding one never makes the whole
// query unreadable (it was a 500).
func TestOrgDeletionReadsDryRunLiterally(t *testing.T) {
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"?dry_run=true;x", http.StatusUnprocessableEntity},
		{"?dry_run=%zz", http.StatusUnprocessableEntity},
		{"?dry_run=true&x=;", http.StatusServiceUnavailable},
		{"?dry_run=true&x=%zz", http.StatusServiceUnavailable},
		{"?dry_run=true", http.StatusServiceUnavailable},
	} {
		db := &touchRecorder{}
		h := &handlers{store: pgStore{Pool: db}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		orgID := uuid.NewString()
		request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+orgID+tc.query, nil)
		request.SetPathValue("org_id", orgID)
		recorder := httptest.NewRecorder()
		h.deleteOrganization(recorder, request)
		if recorder.Code != tc.status {
			t.Errorf("%s: status %d, want %d; body %s", tc.query, recorder.Code, tc.status, recorder.Body.String())
		}
		if calls := db.touched(); len(calls) > 0 {
			t.Errorf("%s: %d Postgres call(s), want 0", tc.query, len(calls))
		}
	}
}
