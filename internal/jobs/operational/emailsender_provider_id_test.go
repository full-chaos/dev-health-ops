package operational

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestResendMessageIDReachesLogsOnlyInTheProviderIDShape: the id Resend
// returns in a response body reaches the success log line and the ambiguous
// error only when it has the shape of a provider-assigned id; any other value
// is dropped and flagged. Not parallel: it swaps the process default logger.
func TestResendMessageIDReachesLogsOnlyInTheProviderIDShape(t *testing.T) {
	for _, testCase := range []struct {
		name, id    string
		status      int
		wantID      string
		wantDropped bool
	}{
		{"accepted, id kept", "msg_ABC-123", http.StatusOK, "msg_ABC-123", false},
		{"accepted, id dropped", "canary id with spaces", http.StatusOK, "", true},
		{"server error, id kept", "msg_ABC-123", http.StatusBadGateway, "msg_ABC-123", false},
		{"server error, id dropped", `canary"token=x`, http.StatusBadGateway, "", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(`{"id":` + quoteJSON(testCase.id) + `}`))
			}))
			defer server.Close()
			t.Setenv("RESEND_API_BASE_URL", server.URL)
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })

			sender := &resendEmailSender{from: "billing@example.test", apiKey: "k", client: server.Client()}
			err := sender.Send(context.Background(), EmailMessage{To: "owner@example.test", Subject: "s", HTML: "<p>h</p>"})
			if testCase.status == http.StatusOK {
				if err != nil {
					t.Fatalf("Send() = %v", err)
				}
				line := logs.String()
				if strings.Contains(line, "canary") || !strings.Contains(line, `"message_id":"`+testCase.wantID+`"`) ||
					!strings.Contains(line, `"id_dropped":`+map[bool]string{true: "true", false: "false"}[testCase.wantDropped]) {
					t.Fatalf("success line: %s", line)
				}
				return
			}
			var ambiguous *AmbiguousSendError
			if !errors.As(err, &ambiguous) {
				t.Fatalf("Send() = %v, want an *AmbiguousSendError", err)
			}
			if ambiguous.ProviderMessageID != testCase.wantID || ambiguous.ProviderMessageIDDropped != testCase.wantDropped {
				t.Fatalf("ambiguous id = %q dropped=%v", ambiguous.ProviderMessageID, ambiguous.ProviderMessageIDDropped)
			}
		})
	}
}

func quoteJSON(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
