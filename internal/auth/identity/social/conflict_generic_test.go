package social

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

const genericConflictBody = `{"detail":{"message":"This sign-in could not be completed. Contact your administrator if this continues.","reason":"identity_conflict"}}`

func TestConflictAnswersShareOneGenericBody(t *testing.T) {
	cases := map[string]error{"collision github": &ConflictError{ExistingMethod: "github"}, "collision local": &ConflictError{ExistingMethod: "local"},
		"collision none": &ConflictError{}, "identity conflict": identity.ErrIdentityConflict, "provider linked": ErrProviderLinked,
		"wrapped collision": fmt.Errorf("exchange: %w", &ConflictError{ExistingMethod: "password"})}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeError(rec, GitLab, err)
			if rec.Code != http.StatusConflict || rec.Body.String() != genericConflictBody {
				t.Fatalf("%d %s, want 409 %s", rec.Code, rec.Body.String(), genericConflictBody)
			}
			if body := strings.ToLower(rec.Body.String()); strings.Contains(body, "exists") || strings.Contains(body, "linked") || strings.Contains(body, "gitlab") {
				t.Fatalf("body names an account state: %s", body)
			}
		})
	}
}
