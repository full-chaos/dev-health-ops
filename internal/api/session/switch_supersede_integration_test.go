//go:build integration

package session_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
)

// switchIssueRoute is auth-service's issue route for the switch tests: it
// records each body and answers the pair, or the error.v1 refusal in refuse.
type switchIssueRoute struct {
	mu     sync.Mutex
	bodies []map[string]any
	refuse string
}

func (f *switchIssueRoute) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	refuse := f.refuse
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if refuse != "" {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"schema_version":"error.v1","status":401,"reason_code":"`+refuse+`"}`)
		return
	}
	_, _ = io.WriteString(w, `{"access_token":"issued.by.auth","refresh_token":"dhr1_issued_by_auth","token_type":"bearer","expires_in":3600}`)
}

func (f *switchIssueRoute) last(t *testing.T) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		t.Fatal("the issue route was not called")
	}
	return f.bodies[len(f.bodies)-1]
}

// switchOrgAs posts switch-org to ownedOrgB with token.
func switchOrgAs(t *testing.T, ctx context.Context, st stack, token string) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/switch-org", strings.NewReader(`{"org_id": "`+ownedOrgB+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

// With auth-service owning sessions, switch-org asks the issue route to
// supersede the caller's own session (CHAOS-12085): a token whose sid names a
// live session sends that session as predecessor_session_id; a token without
// sid sends none; and an issue refused because the predecessor ended first is
// a 401, never the 503 of an unavailable auth-service.
func TestSwitchOrgSupersedesTheCallersSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &switchIssueRoute{}
	auth := httptest.NewServer(route)
	defer auth.Close()
	st := startStackWith(t, ctx, issueClient(t, auth.URL), nil)
	rig := pinrig.NewPinRig(t, ctx, st.pool, st.pg)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := uuid.MustParse(changeAlice)
	sid := rig.SignIn(t, ctx, alice)
	wire := sessionid.Wire(sid)
	withSID, err := st.signer.Access(edgetoken.AccessClaims{UserID: changeAlice, Email: "alice@example.com", OrgID: orgA, Role: "owner", SessionID: &wire},
		time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}

	status, body := switchOrgAs(t, ctx, st, withSID)
	if status != http.StatusOK {
		t.Fatalf("switch with a sid token: %d %s", status, body)
	}
	if got := route.last(t)["predecessor_session_id"]; got != sid.String() {
		t.Fatalf("predecessor_session_id %v, want the caller's session %s", got, sid)
	}

	status, body = switchOrgAs(t, ctx, st, changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0))
	if status != http.StatusOK {
		t.Fatalf("switch with a go-api token: %d %s", status, body)
	}
	if _, present := route.last(t)["predecessor_session_id"]; present {
		t.Fatalf("a token without sid sent a predecessor: %v", route.last(t))
	}

	route.mu.Lock()
	route.refuse = "session_revoked"
	route.mu.Unlock()
	status, body = switchOrgAs(t, ctx, st, withSID)
	if status != http.StatusUnauthorized || !strings.Contains(body, "Invalid or expired token") {
		t.Fatalf("switch whose predecessor ended first: %d %s, want 401", status, body)
	}
	route.mu.Lock()
	route.refuse = "invalid_credentials"
	route.mu.Unlock()
	if status, body = switchOrgAs(t, ctx, st, withSID); status != http.StatusServiceUnavailable {
		t.Fatalf("switch refused for another reason: %d %s, want 503", status, body)
	}
}
