package social

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteErrorConflictOmitsTheExistingMethod(t *testing.T) {
	for _, method := range []string{"github", "local", "password", ""} {
		rec := httptest.NewRecorder()
		writeError(rec, GitLab, &ConflictError{ExistingMethod: method})
		want := `{"detail":{"message":"` + AccountExistsMessage + `"}}`
		if rec.Code != http.StatusConflict || rec.Body.String() != want {
			t.Errorf("method %q: %d %s, want 409 %s", method, rec.Code, rec.Body.String(), want)
		}
	}
}
