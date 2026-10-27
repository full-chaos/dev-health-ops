package investment

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

func servedRowsOf(t *testing.T, served *ServedDecision) []chwrite.AttemptRecord {
	t.Helper()
	m := &Materializer{}
	m.SetServed(served)
	if _, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6))); err != nil {
		t.Fatalf("categorize: %v", err)
	}
	rows := servedAttempts(t, served)
	if len(rows) == 0 {
		t.Fatal("no attempt row was written")
	}
	return rows
}

// A BYO clef row names its backend and its endpoint on the row itself: the
// endpoint is derived from the validated base_url (the Workers AI account URL),
// and that URL holds no gateway id.
func TestABYOClefAttemptRowNamesItsBackendAndEndpoint(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	for _, row := range servedRowsOf(t, served) {
		if row.Provider != "byo-clef" || row.APIMode != "systemone" || row.EndpointType != "workers_ai" || row.GatewayID != "" {
			t.Fatalf("row provider=%q api=%q endpoint_type=%q gateway_id=%q", row.Provider, row.APIMode, row.EndpointType, row.GatewayID)
		}
	}
}

// A BYO text provider has no clef endpoint: the endpoint columns stay empty.
func TestABYOOtherProviderAttemptRowHasItsOwnProviderAndNoEndpoint(t *testing.T) {
	stub := &stubOrgModel{model: "org-model-1", reply: answerAllQuestions}
	served, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	for _, row := range servedRowsOf(t, served) {
		if row.Provider != "byo-openai" || row.EndpointType != "" || row.GatewayID != "" {
			t.Fatalf("row provider=%q endpoint_type=%q gateway_id=%q", row.Provider, row.EndpointType, row.GatewayID)
		}
	}
}

// The endpoint is the configured one. A response header, even a gateway-looking
// one, never sets the gateway id or the endpoint type.
func TestAResponseHeaderNeverSetsTheEndpointColumns(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	stub.network.header = http.Header{"Cf-Aig-Gateway-Id": {"evil-gateway"}, "Cf-Aig-Log-Id": {"log-1"}, "Cf-Aig-Endpoint-Type": {"ai_gateway"}}
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	for _, row := range servedRowsOf(t, served) {
		if row.EndpointType != "workers_ai" || row.GatewayID != "" {
			t.Fatalf("endpoint_type=%q gateway_id=%q", row.EndpointType, row.GatewayID)
		}
	}
}

// Recording the endpoint sends nothing different: the POST goes to the same
// URL with the same three headers as before.
func TestRecordingTheEndpointChangesNothingSent(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	servedRowsOf(t, served)
	req := stub.network.requests[0]
	var names []string
	for name := range req.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	if got := strings.Join(names, ","); got != "Authorization,Content-Type,X-Request-Id" {
		t.Fatalf("request headers = %s", got)
	}
	if req.URL.String() != testClefPost || req.Method != http.MethodPost {
		t.Fatalf("request = %s %s", req.Method, req.URL)
	}
}

// A hostile endpoint value on the backend never reaches a row.
func TestAHostileEndpointValueIsDroppedFromTheRow(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	served.endpointType, served.gatewayID = "workers_ai\nx", "gw\n"+strings.Repeat("a", 80)
	for _, row := range servedRowsOf(t, served) {
		if row.EndpointType != "" || row.GatewayID != "" {
			t.Fatalf("endpoint_type=%q gateway_id=%q", row.EndpointType, row.GatewayID)
		}
	}
}

func TestShadowEndpointTypeKeepsOnlyTheClosedTypes(t *testing.T) {
	for _, kept := range []string{"workers_ai", "ai_gateway"} {
		if got := shadowEndpointType(kept); got != kept {
			t.Fatalf("shadowEndpointType(%q) = %q", kept, got)
		}
	}
	for _, dropped := range []string{"", "Workers_AI", "other", "workers_ai ", "ai_gateway\n"} {
		if got := shadowEndpointType(dropped); got != "" {
			t.Fatalf("shadowEndpointType(%q) = %q, want an empty string", dropped, got)
		}
	}
}

func TestShadowGatewayIDKeepsOnlyShapedIDs(t *testing.T) {
	for _, kept := range []string{"", "my-gateway", "gw_1", "a"} {
		if got := shadowGatewayID(kept); got != kept {
			t.Fatalf("shadowGatewayID(%q) = %q", kept, got)
		}
	}
	for _, dropped := range []string{"gw 1", "gw\n", "gw/../x", "gw?x=1", strings.Repeat("a", 65), "gwé"} {
		if got := shadowGatewayID(dropped); got != "" {
			t.Fatalf("shadowGatewayID(%q) = %q, want an empty string", dropped, got)
		}
	}
}
