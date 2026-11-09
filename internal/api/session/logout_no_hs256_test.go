package session

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// TestLogoutVerifiesNoHS256RefreshToken: go-api holds no HS256 key, so logout never verifies a refresh JWT
// and never opens a transaction to revoke its jti. A refresh token signed with the retired key answers the
// plain 200, the same as any other refresh_token value.
func TestLogoutVerifiesNoHS256RefreshToken(t *testing.T) {
	signer, err := edgetoken.NewSigner("wiring-test-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := signer.Refresh(edgetoken.RefreshClaims{UserID: uuid.NewString(), OrgID: uuid.NewString(), FamilyID: uuid.NewString()},
		time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := logoutRoute(t, edtokens.New(t))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", strings.NewReader(`{"refresh_token":"`+refresh+`"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Pattern = "POST /api/v1/auth/logout"
	recorder := httptest.NewRecorder()
	if panicked := serveRecovering(handler, recorder, request); panicked != "" {
		t.Fatalf("logout of an HS256 refresh token reached the database: %s", panicked)
	}
	if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != logoutDone {
		t.Fatalf("logout of an HS256 refresh token: %d %s, want 200 %s", recorder.Code, recorder.Body.String(), logoutDone)
	}
}

func serveRecovering(handler http.Handler, w http.ResponseWriter, r *http.Request) (panicked string) {
	defer func() {
		if value := recover(); value != nil {
			panicked = fmt.Sprint(value)
		}
	}()
	handler.ServeHTTP(w, r)
	return ""
}
