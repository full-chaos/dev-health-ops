package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// auditPaths are the document pipeline's paths on the access stack that apply a session.
var auditPaths = []struct{ name, listener, path string }{
	{"public /query", "query", "/query"},
	{"internal /query", "internal", "/query"},
	{"/query/run-operation", "internal", "/query/run-operation"},
	{"POST /graphql", "query", "/graphql"},
}

func sendAuditPath(s *behaviourStack, path struct{ name, listener, path string }, token string) legacyAnswer {
	if path.path == "/graphql" {
		return legacyGraphQLDocument(s, http.MethodPost, token, iaDocument)
	}
	return postDocument(s, path.listener, path.path, token, iaDocument)
}

// capturedLog is the default slog logger's output for the length of a test, through the process logger dho
// installs (logging.NewJSON at INFO, with its redactor), so a field the redactor would hide fails the cell.
func capturedLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&buffer, slog.LevelInfo))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// impersonationAuditLines are the impersonationAuditMessage records in buffer.
func impersonationAuditLines(t *testing.T, buffer *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record["msg"] == impersonationAuditMessage {
			lines = append(lines, record)
		}
	}
	return lines
}

// TestImpersonatedRequestWritesOneAuditLine: query-api has no audit table, so a request served in an applied
// session writes exactly one INFO line naming the actor (the admin user id), the target (user id and org), the route
// and the request id, and nothing else; a request with no applied session (no live row, or the legacy refusal with
// the switch on) writes none.
func TestImpersonatedRequestWritesOneAuditLine(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for signing, token := range legacyTokens(t, minter) {
		for _, path := range auditPaths {
			t.Run(signing+" "+path.name, func(t *testing.T) {
				buffer := capturedLog(t)
				s := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
				if got := sendAuditPath(s, path, token); got.Status != http.StatusOK || len(*s.seen) != 1 || !(*s.seen)[0].ImpersonationActive {
					t.Fatalf("got %d %s claims=%+v, want 200 served as the target", got.Status, got.Body, *s.seen)
				}
				lines := impersonationAuditLines(t, buffer)
				if len(lines) != 1 {
					t.Fatalf("got %d audit lines, want exactly 1: %s", len(lines), buffer.String())
				}
				line := lines[0]
				want := map[string]string{"level": "INFO", "actor_user_id": ecUser.String(), "target_user_id": ecTarget.String(), "target_org_id": irTargetOrg.String(), "route": path.path}
				for key, value := range want {
					if line[key] != value {
						t.Fatalf("audit %s = %v, want %s (line %v)", key, line[key], value, line)
					}
				}
				if id, _ := line["request_id"].(string); id == "" {
					t.Fatalf("audit line has no request_id: %v", line)
				}
				allowed := map[string]bool{"time": true, "level": true, "msg": true, "actor_user_id": true, "target_user_id": true, "target_org_id": true, "route": true, "request_id": true}
				for key := range line {
					if !allowed[key] {
						t.Fatalf("audit line carries %q, want only the ids, route and request id: %v", key, line)
					}
				}
			})
			for name, tc := range map[string]struct {
				store *accessStore
				on    bool
			}{
				"no live row":    {legacyStore(nil, nil), false},
				"legacy refusal": {legacyStore(legacySession(), nil), true},
			} {
				t.Run(signing+" "+path.name+" "+name+": no audit line", func(t *testing.T) {
					buffer := capturedLog(t)
					sendAuditPath(legacyStack(t, minter, tc.store, tc.on), path, token)
					if lines := impersonationAuditLines(t, buffer); len(lines) != 0 {
						t.Fatalf("got %d audit lines, want none: %v", len(lines), lines)
					}
				})
			}
		}
	}
}
