package producttelemetry

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordingStreams struct{ appended []string }

func (r *recordingStreams) Append(_ context.Context, stream string, _ [][2]string) error {
	r.appended = append(r.appended, stream)
	return nil
}

const validEvent = `{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{}}`

// A lone surrogate in orgIdHash cannot be UTF-8 encoded into the stream
// key, so the Python Redis client sends nothing and the batch is accepted
// with stream "disabled". Go must append nothing and answer the same.
func TestALoneSurrogateOrgHashIsAStreamFailure(t *testing.T) {
	for _, tc := range []struct {
		name, body, stream string
		appends            int
	}{
		{"surrogate hash", `{"orgIdHash":"\ud800","events":[` + validEvent + `]}`, `"stream":"disabled"`, 0},
		{"surrogate inside hash", `{"org_id_hash":"a\udfffb","events":[` + validEvent + `]}`, `"stream":"disabled"`, 0},
		{"paired surrogates", `{"orgIdHash":"😀","events":[` + validEvent + `]}`, `"stream":"product-telemetry:😀:events"`, 1},
		{"plain hash", `{"orgIdHash":"h1","events":[` + validEvent + `]}`, `"stream":"product-telemetry:h1:events"`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streams := &recordingStreams{}
			route := Routes(streams, slog.New(slog.NewTextHandler(io.Discard, nil)))[0]
			request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			route.Handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), tc.stream) {
				t.Fatalf("body %s lacks %s", recorder.Body.String(), tc.stream)
			}
			if len(streams.appended) != tc.appends {
				t.Fatalf("appends = %v, want %d", streams.appended, tc.appends)
			}
		})
	}
}
