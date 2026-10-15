package credentials

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestGitHubBrowseProviderFailuresKeepProviderTextOutOfTheBody(t *testing.T) {
	cases := []struct {
		providerStatus int
		wantStatus     int
		wantDetail     string
	}{
		{http.StatusUnauthorized, http.StatusUnauthorized, "The provider rejected the credential."},
		{http.StatusForbidden, http.StatusUnauthorized, "The provider rejected the credential."},
		{http.StatusTooManyRequests, http.StatusTooManyRequests, "The provider rate limit was reached. Try again later."},
		{http.StatusInternalServerError, http.StatusBadGateway, "The provider request failed."},
	}
	for _, tc := range cases {
		transport := &browseTransport{statuses: map[string]int{"/user/repos": tc.providerStatus}}
		recorder, logs := browseRaw(t, transport, map[string]pyjson.Value{"token": "tok"}, "", nil)
		if recorder.Code != tc.wantStatus {
			t.Fatalf("provider %d: status %d, want %d: %s", tc.providerStatus, recorder.Code, tc.wantStatus, recorder.Body.String())
		}
		if want := `{"detail":"` + tc.wantDetail + `"}`; recorder.Body.String() != want {
			t.Fatalf("provider %d: body %q, want %q", tc.providerStatus, recorder.Body.String(), want)
		}
		if strings.Contains(recorder.Body.String(), "denied") || strings.Contains(recorder.Body.String(), "github ") {
			t.Fatalf("body carries the provider response text: %s", recorder.Body.String())
		}
		if !strings.Contains(logs, "github ") || (tc.providerStatus == http.StatusForbidden && !strings.Contains(logs, "denied")) {
			t.Fatalf("log lacks the provider cause: %s", logs)
		}
	}
}

func TestUnclassifiedBrowseFailureLogsTheRequestID(t *testing.T) {
	logs := &bytes.Buffer{}
	h := newHandlers(Deps{HTTPClient: &http.Client{Transport: &browseTransport{}}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	recorder := httptest.NewRecorder()
	recorder.Header().Set("X-Request-Id", "req-browse-1")
	h.internal(recorder, httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil), "list github repositories", errors.New("transport marker"))
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "marker") {
		t.Fatalf("status %d body %s", recorder.Code, recorder.Body.String())
	}
	for _, want := range []string{"level=ERROR", "request_id=req-browse-1", "transport marker"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %q: %s", want, logs.String())
		}
	}
}
