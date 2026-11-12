package authapi

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// readableEmbeddedServer mounts the read listing route from the embedded endpoint-profile document, over a state
// whose every read fails: reaching the read answers 503.
func readableEmbeddedServer(t *testing.T, logs *strings.Builder) (http.Handler, *fakeDecisionState) {
	t.Helper()
	state := &fakeDecisionState{snapErr: errors.New("a bearer reached the decision state")}
	routes, err := ReadableRepositoriesRoutes(ReadableRepositoriesDeps{State: state, Evaluator: authorization.NewEvaluator(),
		Workload: testIssueWorkload(), Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("the embedded document mounts %d readable repositories routes, want 1 (its row)", len(routes))
	}
	mux := http.NewServeMux()
	mux.Handle(routes[0].Method+" "+routes[0].Pattern, routes[0].Handler)
	return mux, state
}

// The read listing route (POST /internal/v1/principals/readable-repositories) takes only the go-api workload token.
// Every kind of user token, as the Authorization bearer, is refused 401 invalid_credentials at the credential gate,
// through the route mounted from the embedded document: no state read, no echo of the token in the body or the logs.
func TestReadableRepositoriesRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	var logs strings.Builder
	server, state := readableEmbeddedServer(t, &logs)
	body := `{"principal_id":"0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d","organization_id":"6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11"}`
	for _, tok := range tokens {
		request := httptest.NewRequest(http.MethodPost, PatternReadableRepositories, strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+tok.Value)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)
	if state.reads != 0 {
		t.Fatalf("a user token reached the decision state %d time(s)", state.reads)
	}
}

// The control: the go-api workload token passes the same gate and reaches the state read (which fails, so 503).
func TestReadableRepositoriesControlTheWorkloadTokenReachesTheStateRead(t *testing.T) {
	var logs strings.Builder
	server, state := readableEmbeddedServer(t, &logs)
	request := httptest.NewRequest(http.MethodPost, PatternReadableRepositories,
		strings.NewReader(`{"principal_id":"0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d","organization_id":"6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11"}`))
	for k, v := range issuerBearer() {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || state.snapReads != 1 {
		t.Fatalf("%d %s after %d snapshot read(s), want 503 after 1 (the gate passed and the state read failed)", response.Code, response.Body, state.snapReads)
	}
}
