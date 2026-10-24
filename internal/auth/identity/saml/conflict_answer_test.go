package saml

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSAMLEmailConflictOmitsTheExistingMethod(t *testing.T) {
	for _, method := range []string{"github", "local", ""} {
		rec := httptest.NewRecorder()
		Handlers{}.writeError(rec, httptest.NewRequest(http.MethodPost, "/acs", nil), &EmailConflictError{Method: method})
		want := `{"detail":{"message":"` + AccountExistsMessage + `"}}`
		if rec.Code != http.StatusConflict || rec.Body.String() != want {
			t.Errorf("method %q: %d %s, want 409 %s", method, rec.Code, rec.Body.String(), want)
		}
	}
}
