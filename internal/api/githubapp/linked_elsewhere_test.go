package githubapp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func notAccessibleAnswer(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/login/oauth/access_token") {
			_, _ = w.Write([]byte(`{"access_token":"token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"installations":[]}`))
	}))
	t.Cleanup(github.Close)
	h := handlers{Deps: Deps{HTTPClient: github.Client(), GitHubURL: github.URL, GitHubAPIURL: github.URL,
		Config: Config{ClientID: "id", ClientSecret: "secret"}}}
	_, outcome := h.verifyInstallerAccess(context.Background(), big.NewInt(1), "code")
	if outcome.status == 0 || outcome.err != nil {
		t.Fatalf("the installer-access check did not refuse: %+v", outcome)
	}
	recorder := httptest.NewRecorder()
	detail(recorder, outcome.status, outcome.detail)
	return recorder
}

func TestLinkedElsewhereAnswersLikeAnInstallationTheInstallerCannotAccess(t *testing.T) {
	want := notAccessibleAnswer(t)
	var logs bytes.Buffer
	h := handlers{Deps: Deps{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}}
	got := httptest.NewRecorder()
	h.refuseLinkedElsewhere(context.Background(), got)

	if got.Code != want.Code {
		t.Fatalf("status %d, want %d", got.Code, want.Code)
	}
	if got.Body.String() != want.Body.String() {
		t.Fatalf("body %q, want %q", got.Body.String(), want.Body.String())
	}
	if len(got.Header()) != len(want.Header()) {
		t.Fatalf("headers %v, want %v", got.Header(), want.Header())
	}
	for name, values := range want.Header() {
		if name == "Content-Length" {
			continue
		}
		if strings.Join(got.Header()[name], ",") != strings.Join(values, ",") {
			t.Fatalf("header %s %v, want %v", name, got.Header()[name], values)
		}
	}
	if got.Header().Get("Location") != "" {
		t.Fatal("the answer carries a Location header")
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("expected exactly one JSON log record, got %q: %v", logs.String(), err)
	}
	want2 := map[string]any{
		"msg":   "install callback refused: installation not linkable",
		"class": "linked_elsewhere",
	}
	delete(record, "time")
	delete(record, "level")
	if !reflect.DeepEqual(record, want2) {
		t.Fatalf("log attributes %v, want exactly %v", record, want2)
	}
}
