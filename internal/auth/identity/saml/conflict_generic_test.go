package saml

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const genericConflictBody = `{"detail":{"message":"This sign-in could not be completed. Contact your administrator if this continues."}}`

func TestACSConflictAnswersShareOneGenericBody(t *testing.T) {
	cases := map[string]error{"collision github": &EmailConflictError{Method: "github"}, "collision none": &EmailConflictError{}, "ambiguous": ErrIdentityAmbiguous}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Handlers{}.writeError(rec, httptest.NewRequest(http.MethodPost, "/acs", nil), err)
			if rec.Code != http.StatusConflict || rec.Body.String() != genericConflictBody {
				t.Fatalf("%d %s, want 409 %s", rec.Code, rec.Body.String(), genericConflictBody)
			}
			if body := strings.ToLower(rec.Body.String()); strings.Contains(body, "exists") || strings.Contains(body, "more than one") {
				t.Fatalf("body names an account state: %s", body)
			}
		})
	}
}
