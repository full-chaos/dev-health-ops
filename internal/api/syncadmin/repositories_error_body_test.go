package syncadmin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type credentialRow struct{ values []*string }

func (r credentialRow) Scan(dest ...any) error {
	for i, target := range dest {
		*(target.(**string)) = r.values[i]
	}
	return nil
}

type credentialTx struct{ pgx.Tx }

func (credentialTx) QueryRow(context.Context, string, ...any) pgx.Row {
	encrypted, config := "sealed", "{}"
	return credentialRow{values: []*string{&encrypted, &config}}
}

type tokenDecryptor struct{}

func (tokenDecryptor) Decrypt(secrets.Value) ([]byte, error) { return []byte(`{"token":"tok"}`), nil }

type forbiddenGitLab struct{}

func (forbiddenGitLab) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Request: request,
		Body: io.NopCloser(strings.NewReader(`{"message":"gitlab-body-marker internal.example"}`))}, nil
}

func TestGitLabGroupListingFailureKeepsProviderTextOutOfTheRefusal(t *testing.T) {
	logs := &bytes.Buffer{}
	h := &handlers{logger: slog.New(slog.NewTextHandler(logs, nil)), decryptor: tokenDecryptor{},
		gitlabHTTP: &http.Client{Transport: forbiddenGitLab{}}}
	credentialID := "11111111-1111-1111-1111-111111111111"
	options := pyjson.NewObject()
	options.Set("group", "platform")
	_, _, err := h.resolveGitLabProjects(context.Background(), credentialTx{}, "org-1", &credentialID, options, []string{"api"})
	var refusal *answer
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if refusal.status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", refusal.status)
	}
	if got, want := refusal.detail, "Failed to list GitLab projects for group 'platform'"; got != want {
		t.Fatalf("detail %v, want %q", got, want)
	}
	if !strings.Contains(logs.String(), "gitlab-body-marker") {
		t.Fatalf("log lacks the provider cause: %s", logs.String())
	}
}

func TestFailLogsTheRequestIDAtErrorLevelAndKeepsTheCauseOutOfTheBody(t *testing.T) {
	logs := &bytes.Buffer{}
	h := &handlers{logger: slog.New(slog.NewTextHandler(logs, nil))}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/x", nil)
	request.Header.Set("X-Request-Id", "sync-review-1")
	h.fail(recorder, request, "replace repositories", context.Canceled)
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "canceled") {
		t.Fatalf("status %d body %s", recorder.Code, recorder.Body.String())
	}
	for _, want := range []string{"level=ERROR", "request_id=sync-review-1", "context canceled"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %q: %s", want, logs.String())
		}
	}
}
