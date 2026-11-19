//go:build integration

package sso_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// On the real stack with the OIDC forward off, a malformed provider id gets the same status, content type and body bytes
// as a well-formed provider id that has no row: the 503 config_missing naming AUTH_PROXY_OIDC (CHAOS-14131).
func TestOAuthAuthorizeMalformedIDAnswersAsAnUnknownIDOnARealDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	post := func(id string) (int, string, string) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/oauth/"+id+"/authorize", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, response.Header.Get("Content-Type"), string(raw)
	}
	wantStatus, wantType, wantBody := post(uuid.NewString())
	if wantStatus != http.StatusServiceUnavailable || !strings.Contains(wantBody, "AUTH_PROXY_OIDC is not set") {
		t.Fatalf("unknown id: status %d %s, want 503 config_missing AUTH_PROXY_OIDC", wantStatus, wantBody)
	}
	for _, id := range []string{"providers", "not-a-uuid", "00000000-0000-4000-8000-00000000000g"} {
		status, contentType, body := post(id)
		if status != wantStatus || contentType != wantType || body != wantBody {
			t.Errorf("%s: got %d %q %s, want the unknown-id answer %d %q %s", id, status, contentType, body, wantStatus, wantType, wantBody)
		}
	}
}
