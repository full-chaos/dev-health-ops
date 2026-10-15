package httpapi

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func graphQLDocRoute(digest, operation string, reason GraphQLDigestReason) Route {
	return Route{Method: http.MethodPost, Pattern: "/graphql", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		RecordGraphQLDocument(r.Context(), digest, operation, reason)
		w.WriteHeader(http.StatusOK)
	})}
}

func TestRecordedGraphQLDocumentReachesTheAccessLineAndTheSpan(t *testing.T) {
	read := tracedEnv(t, "1")
	h := newAccessHarness(t, slog.LevelInfo, nil, graphQLDocRoute(testDigest, "home", GraphQLDigestRegistered))
	h.do(http.MethodPost, "/graphql", nil)
	lines := h.accessLines(t)
	if len(lines) != 1 || lines[0]["graphql_digest"] != testDigest || lines[0]["graphql_operation"] != "home" || lines[0]["graphql_digest_reason"] != "registered" {
		t.Fatalf("lines = %v", lines)
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	for key, want := range map[string]string{GraphQLDigestAttribute: testDigest, GraphQLOperationAttribute: "home", GraphQLDigestReasonAttr: "registered"} {
		if got, _ := attrString(spans[0], key); got != want {
			t.Errorf("span attribute %s = %q, want %q", key, got, want)
		}
	}
}

func TestRecordedGraphQLDigestIsHexOnlyAndReasonIsClosed(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, nil, graphQLDocRoute("query { secret }", "", "free text"))
	h.do(http.MethodPost, "/graphql", nil)
	lines := h.accessLines(t)
	if len(lines) != 1 || lines[0]["graphql_digest"] != "" || lines[0]["graphql_digest_reason"] != "not_resolved" {
		t.Fatalf("lines = %v", lines)
	}
	if strings.Contains(h.logs.String(), "secret") {
		t.Fatalf("free text reached the log: %s", h.logs.String())
	}
}

func TestARequestThatRecordsNoGraphQLDocumentLogsNoGraphQLFields(t *testing.T) {
	h := newAccessHarness(t, slog.LevelInfo, nil, itemsRoute())
	h.do(http.MethodGet, "/v1/items/1", nil)
	lines := h.accessLines(t)
	if _, present := lines[0]["graphql_digest"]; len(lines) != 1 || present {
		t.Fatalf("lines = %v", lines)
	}
}
