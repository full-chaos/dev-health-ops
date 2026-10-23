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

type recordingStreams struct {
	appended []string
	fields   [][][2]string
}

func (r *recordingStreams) Append(_ context.Context, stream string, fields [][2]string) error {
	r.appended = append(r.appended, stream)
	r.fields = append(r.fields, fields)
	return nil
}

const testOrgHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const validEvent = `{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{}}`

// The stream key is built from orgIdHash, so the hash is bounded to the shape
// the web client sends (payload_rules.json): any other value, a lone surrogate
// included, is an envelope error of the whole batch and creates no key.
func TestTheOrgHashNamesTheStreamOnlyInTheWebShape(t *testing.T) {
	for _, tc := range []struct {
		name, body, stream string
		appends            int
	}{
		{"sha256 hash", `{"orgIdHash":"` + testOrgHash + `","events":[` + validEvent + `]}`, `"stream":"product-telemetry:` + testOrgHash + `:events"`, 1},
		{"fallback hash", `{"org_id_hash":"fnv1a-0123abcd","events":[` + validEvent + `]}`, `"stream":"product-telemetry:fnv1a-0123abcd:events"`, 1},
		{"null hash", `{"orgIdHash":null,"events":[` + validEvent + `]}`, `"stream":"product-telemetry:anonymous:events"`, 1},
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
