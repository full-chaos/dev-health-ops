package goapiproof

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const principalTestBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"

// authSpy is an edge that records, per request, the Authorization header and whether the request was the Python-plane
// control leg. It answers AUTHORIZATION_ERROR to the read-level token, as the operator-gated resolvers do, and data to the admin one.
type authSpy struct {
	mu   sync.Mutex
	seen []spyRequest
}

type spyRequest struct {
	token    string
	baseline bool
}

func (s *authSpy) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		baseline := strings.Contains(parsed.Query, "python-plane control")
		s.mu.Lock()
		s.seen = append(s.seen, spyRequest{token: r.Header.Get("Authorization"), baseline: baseline})
		s.mu.Unlock()
		w.Header().Set(buildHeader, principalTestBuild)
		if baseline {
			w.Header().Set(planeHeader, "python")
		} else {
			w.Header().Set(planeHeader, "go")
		}
		w.WriteHeader(http.StatusOK)
		if r.Header.Get("Authorization") == "Bearer viewer" {
			_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"Data health requires operator access","extensions":{"code":"AUTHORIZATION_ERROR"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"dataHealth":{"connectors":[]}}}`))
	}))
	t.Cleanup(server.Close)
	return server
}

func principalRunner(t *testing.T, server *httptest.Server, operation, document string, admin *Credential) *Runner {
	t.Helper()
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	return &Runner{
		Client:    NewLegClient(0),
		Documents: map[string]string{operation: document},
		Registry: RegistryView{
			SchemaDigest:   "sha256:29d509cd",
			BuildIdentity:  principalTestBuild,
			DocumentDigest: map[string]string{operation: "06ca28a0"},
		},
		Routing:   map[string]RoutingRow{operation: {Mode: "canary", CandidateBuild: principalTestBuild}},
		Artifacts: store,
		Config: Config{
			OrgID:               "70d529e0",
			Window:              DefaultWindow(),
			PythonEdgeURL:       server.URL,
			Auth:                AuthContext{PrincipalKind: "service", Audience: "query-api", KeyID: "k"},
			EdgeCredential:      StaticCredential("Authorization", "edge access token", "Bearer viewer"),
			AdminEdgeCredential: admin,
			ProofCredential:     StaticCredential("Authorization", "envelope", "Bearer envelope"),
		},
	}
}

const dataHealthDocument = "query ConnectorsDataHealth($teamId: String!) { dataHealth { connectors { id } } }"

// The allowlist is closed and exact. A new entry is a reviewed change to this test; productTelemetryPlatformDashboard (superuser-gated)
// must never appear: the proof principals are never superusers.
func TestTheOperatorGatedAllowlistIsClosedAndExact(t *testing.T) {
	want := []string{"connectorsDataHealth", "dataHealthIdentity", "mappingCoverageHealth", "metricLineage"}
	if got := OperatorGatedOperations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("allowlist = %v, want %v", got, want)
	}
	for _, operation := range want {
		if PrincipalFor(operation) != PrincipalAdminProof || PrincipalAdminProof != "admin-proof" {
			t.Fatalf("%s: principal %q", operation, PrincipalFor(operation))
		}
	}
	for _, operation := range []string{"productTelemetryPlatformDashboard", "busFactor", "pr", "", "reportRuns"} {
		if PrincipalFor(operation) != "" {
			t.Fatalf("%q must never be widened", operation)
		}
	}
}

func TestOnlyAQueryDocumentIsEverWidened(t *testing.T) {
	for document, mutation := range map[string]bool{
		dataHealthDocument:                                       false,
		"mutation X { createThing { id } }":                      true,
		"MUTATION X { createThing { id } }":                      true,
		"subscription S { tick }":                                true,
		"query Q { a } # a mutation in a comment is refused too": true,
		"query Q { mutationsCount }":                             false, // a word that merely contains it is not the keyword
	} {
		if got := isMutationDocument(document); got != mutation {
			t.Errorf("isMutationDocument(%q) = %v, want %v", document, got, mutation)
		}
	}
}

// Red-then-green anchor for the operator-gated refusal: as the read-level principal the operation only measures the AUTHORIZATION_ERROR;
// with no admin credential the run must refuse it BY NAME and send nothing.
func TestAnOperatorGatedOperationWithoutAnAdminCredentialIsRefusedAndSendsNothing(t *testing.T) {
	spy := &authSpy{}
	runner := principalRunner(t, spy.server(t), "connectorsDataHealth", dataHealthDocument, nil)
	outcomes, _, _ := runner.Run(context.Background())
	if len(outcomes) != 1 || outcomes[0].RefusalReason != "admin_principal_not_supplied_for_an_operator_gated_operation" {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	if len(spy.seen) != 0 {
		t.Fatalf("%d request(s) went out for a refused operation: %+v", len(spy.seen), spy.seen)
	}
	if RefusalAdminPrincipalUnavailable != "admin_principal_not_supplied_for_an_operator_gated_operation" || outcomes[0].Principal != "" {
		t.Fatalf("wire string moved or principal set on a refusal: %q", outcomes[0].Principal)
	}
}

// With the admin credential BOTH legs carry it (candidate and baseline share the edge), the read-level token is never sent for the operation,
// the outcome names the principal, and the candidate is no longer refused for an authorization error.
func TestAnOperatorGatedOperationCarriesTheAdminCredentialOnBothLegs(t *testing.T) {
	spy := &authSpy{}
	admin := StaticCredential("Authorization", "org-admin proof principal edge access token", "Bearer admin")
	runner := principalRunner(t, spy.server(t), "connectorsDataHealth", dataHealthDocument, admin)
	outcomes, _, _ := runner.Run(context.Background())
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	if outcomes[0].Principal != "admin-proof" {
		t.Fatalf("outcome principal = %q", outcomes[0].Principal)
	}
	if outcomes[0].RefusalReason == RefusalAdminPrincipalUnavailable || outcomes[0].RefusalReason == RefusalErroredResponse {
		t.Fatalf("refused as if unauthorized/unsupplied: %s %s", outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
	var candidate, baseline int
	for _, request := range spy.seen {
		if request.token != "Bearer admin" {
			t.Fatalf("a request carried %q, want the admin credential on every leg: %+v", request.token, spy.seen)
		}
		if request.baseline {
			baseline++
		} else {
			candidate++
		}
	}
	if candidate == 0 || baseline == 0 {
		t.Fatalf("expected a candidate and a baseline request, saw candidate=%d baseline=%d", candidate, baseline)
	}
}

// An operation that is NOT on the allowlist never gets the admin credential, even when the run holds one.
func TestAnOperationOffTheAllowlistNeverCarriesTheAdminCredential(t *testing.T) {
	spy := &authSpy{}
	admin := StaticCredential("Authorization", "org-admin proof principal edge access token", "Bearer admin")
	runner := principalRunner(t, spy.server(t), "mappingCoverageHealth", dataHealthDocument, admin)
	// Take the operation off the allowlist for this test only, to prove the credential follows the LIST and not the run's flag.
	saved := operatorGatedOperations
	operatorGatedOperations = map[string]string{}
	t.Cleanup(func() { operatorGatedOperations = saved })
	outcomes, _, _ := runner.Run(context.Background())
	if len(outcomes) != 1 || outcomes[0].Principal != "" {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	for _, request := range spy.seen {
		if request.token == "Bearer admin" {
			t.Fatalf("an operation that is not on the allowlist carried the admin credential: %+v", spy.seen)
		}
	}
}

// A mutation document for an allowlisted operation gets NO widened principal and NO request. Two layers: the runner's own document-kind
// check (RefusalNotAQueryDocument, every operation) refuses a real mutation first; the widened principal's own, deliberately broader check
// (isMutationDocument) refuses a document the kind check calls a query but that names the keyword, e.g. a field called mutation.
func TestAMutationDocumentIsRefusedTheWidenedPrincipalBeforeAnyRequest(t *testing.T) {
	admin := StaticCredential("Authorization", "org-admin proof principal edge access token", "Bearer admin")
	for name, tc := range map[string]struct{ document, reason string }{
		"a real mutation (the kind check refuses it first)":      {"mutation Drop($teamId: String!) { dropConnectors(teamId: $teamId) { id } }", "document_is_not_a_query"},
		"a query naming the keyword (the principal's own check)": {"query Q($teamId: String!) { dataHealth { mutation } }", "widened_principal_refused_a_document_that_is_not_a_query"},
	} {
		spy := &authSpy{}
		runner := principalRunner(t, spy.server(t), "connectorsDataHealth", tc.document, admin)
		outcomes, _, _ := runner.Run(context.Background())
		if len(outcomes) != 1 || outcomes[0].RefusalReason != tc.reason {
			t.Fatalf("%s: outcomes = %+v", name, outcomes)
		}
		if len(spy.seen) != 0 || outcomes[0].Principal != "" {
			t.Fatalf("%s: %d request(s) went out, principal %q: %+v", name, len(spy.seen), outcomes[0].Principal, spy.seen)
		}
	}
}

// A widened receipt's identity differs from a read-level one; every other identity is unchanged.
func TestAWidenedRequestHasItsOwnIdentityAndOthersAreUnchanged(t *testing.T) {
	runner := principalRunner(t, httptest.NewServer(http.NotFoundHandler()), "connectorsDataHealth", dataHealthDocument, nil)
	if !reflect.DeepEqual(runner.authFor(""), runner.Config.Auth) {
		t.Fatalf("the default principal's auth shape changed: %+v", runner.authFor(""))
	}
	variables := map[string]any{"teamId": "ALL"}
	plain, err := RequestIdentity("o", runner.authFor(""), variables)
	if err != nil {
		t.Fatal(err)
	}
	widened, err := RequestIdentity("o", runner.authFor("admin-proof"), variables)
	if err != nil {
		t.Fatal(err)
	}
	if plain == widened {
		t.Fatal("a widened request shares an identity with a read-level one")
	}
}
