package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// TestProverGoEdgeModeProvesTheRealGraphQLEdge runs the prover's Go-edge
// mode against the REAL /graphql chain and pipeline, not a fake of them: the
// control document the prover sends is refused by the real registered-
// document gate, with the real provenance headers, and that refusal is what
// the prover must accept as "the edge is query-api alone". If either side
// changes what an unregistered document is answered with, this fails; a fake
// edge in the prover's own tests could not notice.
func TestProverGoEdgeModeProvesTheRealGraphQLEdge(t *testing.T) {
	build := stampedBuild(t)
	const document = "query FeatureFlags { featureFlags { key } }"
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "viewer"},
	}
	verifier, _ := iaVerifier(t)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"featureFlags": true})
	mux.Register("featureFlags", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"featureFlags":[{"key":"a"}]}}`)
	}))
	auth := ecEdgeAuth(t, store)
	pipeline := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(document): "featureFlags"}, verifier, auth, store, "", nil)
	edge := httptest.NewServer(internalidentity.Public(graphQLEdgeChain(pipeline, graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})))
	t.Cleanup(edge.Close)

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := goapiproof.DefaultGoServedLedger()
	if err != nil {
		t.Fatal(err)
	}
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "viewer", tokenVersion: 5})
	newRunner := func(goEdge bool) *goapiproof.Runner {
		return &goapiproof.Runner{
			Client:    goapiproof.NewLegClient(0),
			GoServed:  ledger,
			Documents: map[string]string{"featureFlags": document},
			Registry: goapiproof.RegistryView{
				SchemaDigest:   "sha256:edge",
				BuildIdentity:  build,
				DocumentDigest: map[string]string{"featureFlags": digestHex(document)},
			},
			Routing:   map[string]goapiproof.RoutingRow{"featureFlags": {Mode: "canary", CandidateBuild: build}},
			Artifacts: artifacts,
			Config: goapiproof.Config{
				OrgID:           ecOrg.String(),
				Window:          goapiproof.DefaultWindow(),
				PythonEdgeURL:   edge.URL + "/graphql",
				GoEdge:          goEdge,
				Auth:            goapiproof.AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "test"},
				EdgeCredential:  goapiproof.StaticCredential("Authorization", "edge access token", "Bearer "+token).BindOrg(ecOrg.String()),
				ProofCredential: goapiproof.StaticCredential("Authorization", "envelope", "Bearer envelope"),
			},
		}
	}

	outcomes, summary, err := newRunner(true).Run(context.Background())
	if err != nil {
		t.Fatalf("Go-edge run against the real /graphql: %v", err)
	}
	if summary.EdgeMode != goapiproof.EdgeModeGo || summary.ProvenGoOnly != 1 || summary.Refused != 0 || len(outcomes) != 1 {
		t.Fatalf("summary %+v outcomes %+v, want one go-edge proof and no refusal", summary, outcomes)
	}
	if control := outcomes[0].Baseline; control == nil || control.StatusCode != http.StatusNotFound || control.Plane != "go" || control.Build != build {
		t.Fatalf("control leg %+v, want the real gate's 404 from plane go, build %s", control, build)
	}

	// The same real edge in the Python-reference mode is refused: its
	// control document is not answered by a Python plane.
	outcomes, _, _ = newRunner(false).Run(context.Background())
	if len(outcomes) != 1 || outcomes[0].Executed || outcomes[0].RefusalReason != goapiproof.RefusalWrongPlane {
		t.Fatalf("python-reference mode against the real /graphql: %+v, want refused %s", outcomes, goapiproof.RefusalWrongPlane)
	}
}
