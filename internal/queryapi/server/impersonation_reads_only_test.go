package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/producttelemetry"
)

// An applied impersonation session is reads only (D6714): a live holder of admin.impersonate (a superuser, or a
// non-superuser by its platform set) beside a live go-api row is served as the target on a query and refused (403
// policy.WriteImpersonationRefused, no resolver) on any other document kind -- a mutation, a subscription, a text
// that does not parse -- on every path of the document pipeline, whatever the switch. With the switch on the legacy
// refusal answers first, with the same 403.

// Registered on the access stack beside iaDocument and legacyMutationDocument: a subscription and a text that does not
// parse, whose kind is not a query.
const (
	impersonationSubscriptionDocument = "subscription ImpersonationReadsOnlySub { probe }"
	impersonationUnparsableDocument   = "mutation ImpersonationReadsOnlyBroken {"
)

// impersonationHolders are the two holders of admin.impersonate beside a live row, with their bearers in each signing.
func impersonationHolders(t *testing.T, minter *accessMinter) map[string]struct {
	store  func() *accessStore
	tokens map[string]string
} {
	t.Helper()
	return map[string]struct {
		store  func() *accessStore
		tokens map[string]string
	}{
		"superuser holder": {func() *accessStore { return legacyStore(legacySession(), nil) }, legacyTokens(t, minter)},
		"non-superuser holder": {nonSuperuserHolderStore, map[string]string{
			"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
			"EdDSA": minter.mint(ecUser, nil, nil),
		}},
	}
}

// legacyQueryDocument is legacyQuery with the document named.
func legacyQueryDocument(s *behaviourStack, listener, path, bearer, document string) legacyAnswer {
	body, _ := json.Marshal(map[string]string{"query": document})
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header}
}

// pipelinePaths are the document pipeline's paths on the access stack (the write door is its own handler, below).
var pipelinePaths = []struct{ name, listener, path string }{
	{"public /query", "query", "/query"},
	{"internal /query", "internal", "/query"},
	{"/query/run-operation", "internal", "/query/run-operation"},
	{"POST /graphql", "query", "/graphql"},
}

func sendPipeline(s *behaviourStack, path struct{ name, listener, path string }, token, document string) legacyAnswer {
	if path.path == "/graphql" {
		return legacyGraphQLDocument(s, http.MethodPost, token, document)
	}
	return legacyQueryDocument(s, path.listener, path.path, token, document)
}

// TestImpersonatedSessionIsReadsOnlyOnThePipeline: each holder's mutation, subscription and unparsable registered
// text are refused on each path, the switch unset or on; its query is served as the target (switch unset).
func TestImpersonatedSessionIsReadsOnlyOnThePipeline(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for holder, h := range impersonationHolders(t, minter) {
		for signing, token := range h.tokens {
			for _, path := range pipelinePaths {
				for switchName, on := range revokedSwitches {
					for kind, document := range map[string]string{"mutation": legacyMutationDocument, "subscription": impersonationSubscriptionDocument, "unparsable": impersonationUnparsableDocument} {
						t.Run(holder+" "+signing+" "+path.name+" "+switchName+" "+kind, func(t *testing.T) {
							s := legacyStack(t, minter, h.store(), on)
							if got := sendPipeline(s, path, token, document); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*s.seen) != 0 {
								t.Fatalf("got %d %s, ran %d; want 403 %s and no resolver", got.Status, got.Body, len(*s.seen), legacyRefusalBody)
							}
						})
					}
				}
				t.Run(holder+" "+signing+" "+path.name+" switch unset query", func(t *testing.T) {
					s := legacyStack(t, minter, h.store(), false)
					got := sendPipeline(s, path, token, iaDocument)
					if got.Status != http.StatusOK || len(*s.seen) != 1 || !(*s.seen)[0].ImpersonationActive || (*s.seen)[0].OrgID != irTargetOrg.String() || (*s.seen)[0].Role != "viewer" {
						t.Fatalf("got %d %s claims=%+v, want 200 served as the target (its org and role)", got.Status, got.Body, *s.seen)
					}
				})
			}
		}
	}
}

// TestImpersonatedWriteDoorRefusesTheMutation: the write door refuses each holder's mutation, the switch unset or on;
// the same caller with no live row is served as itself.
func TestImpersonatedWriteDoorRefusesTheMutation(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for holder, h := range impersonationHolders(t, minter) {
		for switchName, on := range revokedSwitches {
			for signing, token := range h.tokens {
				t.Run(holder+" "+switchName+" "+signing, func(t *testing.T) {
					door, seen := revokedWriteDoor(t, minter, h.store(), on)
					if got := postProofWrite(door, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seen) != 0 {
						t.Fatalf("live row: %d %q, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seen), legacyRefusalBody)
					}
					store := h.store()
					store.sessions = nil
					none, seenNone := revokedWriteDoor(t, minter, store, on)
					if got := postProofWrite(none, token); got.Code != http.StatusOK || len(*seenNone) != 1 || (*seenNone)[0].ImpersonationActive {
						t.Fatalf("no row: %d %q claims=%+v, want 200 served as itself", got.Code, got.Body.String(), *seenNone)
					}
				})
			}
		}
	}
}

// TestImpersonatedSessionIsNotAPlatformAdmin: the claims a holder's applied session runs with carry the
// impersonation, so the platform-admin gate refuses them (D6714: platform refused while impersonating).
func TestImpersonatedSessionIsNotAPlatformAdmin(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for holder, h := range impersonationHolders(t, minter) {
		for signing, token := range h.tokens {
			t.Run(holder+" "+signing, func(t *testing.T) {
				s := legacyStack(t, minter, h.store(), false)
				if got := legacyQueryDocument(s, "query", "/query", token, iaDocument); got.Status != http.StatusOK || len(*s.seen) != 1 {
					t.Fatalf("got %d %s, ran %d; want 200 and one run", got.Status, got.Body, len(*s.seen))
				}
				claims := (*s.seen)[0]
				if !claims.ImpersonationActive {
					t.Fatalf("claims = %+v, want the applied session", claims)
				}
				if err := producttelemetry.RequirePlatformAdmin(producttelemetry.Principal{Present: true, IsSuperuser: claims.IsSuperuser, ImpersonationActive: claims.ImpersonationActive, Platform: claims.Platform}); err == nil {
					t.Fatalf("claims %+v pass the platform-admin gate, want it refused while impersonating", claims)
				}
			})
		}
	}
}

// capturedLog is the default slog logger's output for the length of a test.
func capturedLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// impersonationAuditLines are the "query-api impersonation session applied" records in buffer.
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

// TestImpersonatedRequestWritesOneAuditLine: an applied session writes exactly one info line naming the actor (the
// admin user id), the target (user id and org), the route and the request id, and nothing else of the caller; a
// request with no applied session (no live row, or the legacy refusal) writes none.
func TestImpersonatedRequestWritesOneAuditLine(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for holder, h := range impersonationHolders(t, minter) {
		for signing, token := range h.tokens {
			for _, path := range pipelinePaths {
				t.Run(holder+" "+signing+" "+path.name, func(t *testing.T) {
					buffer := capturedLog(t)
					s := legacyStack(t, minter, h.store(), false)
					if got := sendPipeline(s, path, token, iaDocument); got.Status != http.StatusOK {
						t.Fatalf("got %d %s, want 200", got.Status, got.Body)
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
					store func() *accessStore
					on    bool
				}{
					"no live row":    {func() *accessStore { s := h.store(); s.sessions = nil; return s }, false},
					"legacy refusal": {h.store, true},
				} {
					t.Run(holder+" "+signing+" "+path.name+" "+name+": no audit line", func(t *testing.T) {
						buffer := capturedLog(t)
						s := legacyStack(t, minter, tc.store(), tc.on)
						sendPipeline(s, path, token, iaDocument)
						if lines := impersonationAuditLines(t, buffer); len(lines) != 0 {
							t.Fatalf("got %d audit lines, want none: %v", len(lines), lines)
						}
					})
				}
			}
		}
	}
}
