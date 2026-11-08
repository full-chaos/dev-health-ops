package githubapp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

func configMissingRun(t *testing.T, call func(handlers, http.ResponseWriter, *http.Request), payload string) *httptest.ResponseRecorder {
	t.Helper()
	h := handlers{Deps: Deps{
		Logger: slog.New(slog.DiscardHandler), Now: time.Now,
		Config: Config{Slug: "app", AppID: "1", ClientID: "id", ClientSecret: "secret"},
		Signer: Signer{Issuer: "iss", Audience: "aud"},
	}}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	body, outcome, _, err := pybody.Read(request)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("read the body: %v %v", outcome, err)
	}
	ctx := policy.WithUser(policy.WithBody(context.Background(), body), &policy.User{OrgID: "org-1"})
	recorder := httptest.NewRecorder()
	call(h, recorder, request.WithContext(ctx))
	return recorder
}

func assertConfigMissing(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503: %s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Detail struct {
			Message    string `json:"message"`
			ReasonCode string `json:"reason_code"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	if body.Detail.ReasonCode != "config_missing" || !strings.Contains(body.Detail.Message, "GITHUB_APP_STATE_SECRET") {
		t.Fatalf("body %q, want reason_code config_missing and a message naming GITHUB_APP_STATE_SECRET", recorder.Body.String())
	}
}

func TestInstallURLAnswers503ConfigMissingWithoutTheStateKey(t *testing.T) {
	assertConfigMissing(t, configMissingRun(t, handlers.installURL, `{}`))
}

func TestInstallCallbackAnswers503ConfigMissingWithoutTheStateKey(t *testing.T) {
	assertConfigMissing(t, configMissingRun(t, handlers.installCallback, `{"installation_id":1,"state":"x","code":"c"}`))
}
