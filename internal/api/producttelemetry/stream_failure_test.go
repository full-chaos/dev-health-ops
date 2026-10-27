package producttelemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failingStreams struct {
	err     error
	appends int
}

func (f *failingStreams) Append(context.Context, string, [][2]string) error {
	f.appends++
	return f.err
}

func postTo(t *testing.T, streams Streams, body string) *httptest.ResponseRecorder {
	t.Helper()
	route := Routes(streams, slog.New(slog.NewTextHandler(io.Discard, nil)))[0]
	request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	route.Handler.ServeHTTP(recorder, request)
	return recorder
}

func TestStreamWriteFailureAnswers503(t *testing.T) {
	streams := &failingStreams{err: errors.New("valkey down")}
	got := postTo(t, streams, `{"orgIdHash":"`+testOrgHash+`","events":[`+validEvent+`]}`)
	if got.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body %s", got.Code, got.Body.String())
	}
	if got.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", got.Header().Get("Retry-After"))
	}
	if !strings.HasPrefix(got.Body.String(), `{"detail":"`) {
		t.Fatalf("body %s is not the detail envelope", got.Body.String())
	}
	for _, banned := range []string{"ingestion_id", `"accepted"`, `"stream"`, "disabled"} {
		if strings.Contains(got.Body.String(), banned) {
			t.Fatalf("body %s acknowledges the batch (%s)", got.Body.String(), banned)
		}
	}
	if streams.appends != 1 {
		t.Fatalf("appends = %d, want exactly one attempt", streams.appends)
	}
}

func TestNotConfiguredStreamStaysAcceptedDisabled(t *testing.T) {
	got := postTo(t, &ValkeyStreams{}, `{"orgIdHash":"`+testOrgHash+`","events":[`+validEvent+`]}`)
	if got.Code != http.StatusAccepted || !strings.Contains(got.Body.String(), `"stream":"disabled"`) {
		t.Fatalf("not configured = %d %s, want 202 disabled", got.Code, got.Body.String())
	}
}
