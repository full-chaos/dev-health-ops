package goapiproof

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests reproduce the two credential defects JOB 4 found by running
// the merged verb against the real compose stack on 2026-09-09. Each is
// written so it FAILS against the previous design -- one credential for
// every request, read once at startup -- rather than merely passing
// against the new one.

// twoPlaneServers stands in for the deployed stack's actual behaviour,
// measured from inside the api container: the Python edge accepts ONLY the
// access token and answers 401 to an envelope; /buildinfo and /query/proof
// accept ONLY the envelope and answer 401 to an access token.
//
// That asymmetry is the whole defect. A single Authorization value cannot
// satisfy both, so a runner that sends one map to every request always
// fails on one leg -- and a 401 on the candidate leg is indistinguishable
// in a report from the edge legitimately refusing the caller.
type twoPlaneServers struct {
	edge   *httptest.Server
	proof  *httptest.Server
	edgeOK string
	seen   map[string]string
	mu     sync.Mutex
}

func newTwoPlaneServers(t *testing.T, body string, envelope func() string) *twoPlaneServers {
	t.Helper()
	s := &twoPlaneServers{edgeOK: "Bearer edge-access-token", seen: map[string]string{}}

	s.edge = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		s.record("edge", got)
		if got != s.edgeOK {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		plane := "go"
		if bodyMentionsControl(r) {
			plane = "python"
		}
		w.Header().Set(planeHeader, plane)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.edge.Close)

	s.proof = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		s.record("proof", got)
		if got != envelope() {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set(planeHeader, "go")
		w.Header().Set(buildHeader, "b18e56fa79cfe20ce0f75df148144b832d92be36")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.proof.Close)
	return s
}

func (s *twoPlaneServers) record(plane, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[plane] = value
}

func (s *twoPlaneServers) credentialSeenBy(plane string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[plane]
}

// Defect 1. The two planes must each receive THEIR OWN credential.
//
// Fails against the single-map design: whichever value that map held, one
// of the two assertions below sees the other plane's credential.
func TestEachPlaneReceivesItsOwnCredential(t *testing.T) {
	envelope := "Bearer effective-principal-envelope"
	servers := newTwoPlaneServers(t, `{"data":{"featureFlags":[{"key":"a"}]}}`,
		func() string { return envelope })

	runner := newTwoPlaneRunner(t, servers,
		StaticCredential("Authorization", "edge access token", servers.edgeOK),
		StaticCredential("Authorization", "envelope", envelope),
		"shadow")

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := servers.credentialSeenBy("edge"); got != servers.edgeOK {
		t.Fatalf("the edge received %q, not the access token: the baseline leg must authenticate to the EDGE", got)
	}
	if got := servers.credentialSeenBy("proof"); got != envelope {
		t.Fatalf("the proof route received %q, not the envelope: /query/proof does not accept the edge access token (measured 401 on the deployed stack)", got)
	}
}

// Defect 1, the operator-visible half: a leg with no credential must
// refuse by name rather than send an unauthenticated request.
//
// An unauthenticated request comes back 401, which reads in a report
// exactly like a credential the server rejected -- and that confusion is
// what cost JOB 4 four separate attempts to diagnose.
func TestAMissingCredentialRefusesRatherThanSendingNone(t *testing.T) {
	servers := newTwoPlaneServers(t, `{"data":{"featureFlags":[{"key":"a"}]}}`,
		func() string { return "Bearer envelope" })

	runner := newTwoPlaneRunner(t, servers,
		StaticCredential("Authorization", "edge access token", servers.edgeOK),
		nil, // no proof credential
		"shadow")

	// Run reports "this run measured NOTHING" -- correct, and the point:
	// the refusal is at the credential, and the run is honest that it
	// produced no evidence rather than writing a receipt.
	outcomes, _, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("a run that measured nothing must say so")
	}
	if len(outcomes) != 1 {
		t.Fatalf("expected one outcome, got %d", len(outcomes))
	}
	if outcomes[0].Executed {
		t.Fatal("a shadow operation with no proof credential must not produce a measurement")
	}
	if servers.credentialSeenBy("proof") != "" {
		t.Fatal("an unauthenticated request reached /query/proof: it would answer 401, which is indistinguishable in the report from a rejected credential")
	}
}

// Defect 2. A credential whose value ages out must be re-minted, and the
// refresh has to survive the LAST call of a run -- the closing
// /buildinfo, which is where JOB 4's attempt B died with every
// measurement already taken.
//
// Expiry is simulated rather than slept: the server accepts only the most
// recently minted value, so a value read once at startup is stale by the
// second request. That is the same failure as a 60-second TTL, minus 60
// seconds of test.
func TestAnAgingCredentialIsReMintedBeforeEachUse(t *testing.T) {
	var mu sync.Mutex
	minted := 0
	current := func() string {
		mu.Lock()
		defer mu.Unlock()
		return fmt.Sprintf("Bearer envelope-%d", minted)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != current() {
			// Exactly what an expired envelope gets.
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`))
	}))
	t.Cleanup(server.Close)

	// freshFor 0 means "mint for every use", which is what a credential
	// shorter-lived than the run requires.
	credential := MintedCredential("Authorization", "envelope", 0, func(context.Context) (string, error) {
		mu.Lock()
		minted++
		value := fmt.Sprintf("Bearer envelope-%d", minted)
		mu.Unlock()
		return value, nil
	})

	before, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, credential)
	if err != nil {
		t.Fatalf("first /buildinfo: %v", err)
	}

	// Stand in for the fifteen operations between the two /buildinfo
	// reads: each one ages the envelope further.
	for i := 0; i < 15; i++ {
		mu.Lock()
		minted++
		mu.Unlock()
	}

	// The closing stability check. A value read once at startup is now
	// sixteen generations stale and this is a 401.
	if err := VerifyBuildStable(context.Background(), server.Client(), server.URL, credential, before); err != nil {
		t.Fatalf("the closing build-stability check failed on a stale credential: %v", err)
	}
}

// A static credential is still legal -- it is right for a one-operation
// run -- but it must FAIL against a server that has moved on, so that
// this test cannot pass merely because everything got a fresh value.
func TestAStaticCredentialStillExpires(t *testing.T) {
	var mu sync.Mutex
	generation := 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		want := fmt.Sprintf("Bearer envelope-%d", generation)
		mu.Unlock()
		if r.Header.Get("Authorization") != want {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"commit":"abc","modified":false}`))
	}))
	t.Cleanup(server.Close)

	static := StaticCredential("Authorization", "envelope", "Bearer envelope-1")
	if _, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, static); err != nil {
		t.Fatalf("a current static credential must work: %v", err)
	}
	mu.Lock()
	generation = 2
	mu.Unlock()
	if _, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, static); err == nil {
		t.Fatal("a static credential the server no longer accepts must fail -- otherwise the refresh test above proves nothing")
	}
}

// A minter that fails must surface its error, never fall back to the
// stale cached value: a run that cannot authenticate has to refuse by
// name rather than send something the server will reject.
func TestAMintFailureIsReportedRatherThanFallingBackToStale(t *testing.T) {
	calls := 0
	credential := MintedCredential("Authorization", "envelope", 0, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "Bearer good", nil
		}
		return "", fmt.Errorf("minting command exited 1")
	})

	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	if err := credential.Apply(context.Background(), request); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := credential.Apply(context.Background(), request); err == nil {
		t.Fatal("a mint failure must be returned, not hidden behind the previously cached value")
	}
}

// An empty minted value would be sent as a bare "Bearer ", which the
// server answers 401 -- the same indistinguishable-401 problem one level
// down.
func TestAnEmptyMintedValueIsRefused(t *testing.T) {
	credential := MintedCredential("Authorization", "envelope", 0, func(context.Context) (string, error) {
		return "", nil
	})
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	if err := credential.Apply(context.Background(), request); err == nil {
		t.Fatal("an empty minted credential must be refused at the source")
	}
}

// freshFor caches: within the window the minter is not re-run, so a
// fifteen-operation run does not shell out thirty times.
func TestAFreshCredentialIsNotReMinted(t *testing.T) {
	calls := 0
	credential := MintedCredential("Authorization", "envelope", time.Minute, func(context.Context) (string, error) {
		calls++
		return fmt.Sprintf("Bearer v%d", calls), nil
	})
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	for i := 0; i < 5; i++ {
		if err := credential.Apply(context.Background(), request); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("minted %d times inside the freshness window, expected 1", calls)
	}
}

// bodyMentionsControl reports whether this request carries the
// comment-suffixed control document -- the mechanism the baseline leg
// uses to miss the registered digest and be served by Python.
func bodyMentionsControl(r *http.Request) bool {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return false
	}
	return strings.Contains(string(raw), "python-plane control")
}

func newTwoPlaneRunner(t *testing.T, servers *twoPlaneServers, edge, proof *Credential, mode string) *Runner {
	t.Helper()
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	return &Runner{
		Client:    servers.edge.Client(),
		Documents: map[string]string{"featureFlags": "query FeatureFlags { featureFlags { key } }"},
		Registry: RegistryView{
			SchemaDigest:   "sha256:29d509cd",
			BuildIdentity:  "b18e56fa79cfe20ce0f75df148144b832d92be36",
			DocumentDigest: map[string]string{"featureFlags": "06ca28a0"},
		},
		Routing:   map[string]RoutingRow{"featureFlags": {Mode: mode, CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}},
		Artifacts: store,
		Config: Config{
			OrgID:           "70d529e0",
			Window:          DefaultWindow(),
			PythonEdgeURL:   servers.edge.URL,
			GoProofURL:      servers.proof.URL,
			Auth:            AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
			EdgeCredential:  edge,
			ProofCredential: proof,
		},
	}
}

// A minting command that prints something OTHER than a token -- a usage
// line, an error, a JSON blob, a shell prompt -- must be refused at the
// source. Sending it produces a 401 that reads exactly like a rejected
// credential, which is the confusion this whole file exists to remove.
func TestAMintedValueThatIsNotAnEnvelopeIsRefused(t *testing.T) {
	for name, printed := range map[string]string{
		"a usage line":        "usage: mint-envelope [--org ORG]",
		"an error message":    "Error: no such container: api",
		"a JSON blob":         `{"envelope":"eyJhbGci"}`,
		"an opaque token":     "abcdef0123456789",
		"two segments":        "eyJhbGci.eyJzdWIi",
		"an empty segment":    "eyJhbGci..c2ln",
		"a token with a tab":  "eyJhbGci.eyJzdWIi.c2ln\there",
		"non-base64url bytes": "eyJhbGci.eyJzdWIi.++signature++",
	} {
		t.Run(name, func(t *testing.T) {
			credential := MintedCredential("Authorization", "envelope", 0,
				func(context.Context) (string, error) { return printed, nil },
			).WithShapeValidator(ValidateEnvelopeShape)

			request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
			err := credential.Apply(context.Background(), request)
			if err == nil {
				t.Fatalf("%q was accepted as an envelope", printed)
			}
			if strings.Contains(err.Error(), printed) {
				t.Fatalf("the error quoted the minted value back: a malformed credential is still a credential (%v)", err)
			}
		})
	}
}

// And a real-shaped envelope passes, so the validator is not simply
// refusing everything.
func TestAWellFormedEnvelopePassesTheShapeCheck(t *testing.T) {
	// Three non-empty base64url segments -- the shape jwt.encode produces.
	token := "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEiLCJvcmdfaWQiOiJvLTEifQ.c2lnbmF0dXJl"
	if err := ValidateEnvelopeShape(token); err != nil {
		t.Fatalf("a well-formed envelope was refused: %v", err)
	}
	if err := ValidateEnvelopeShape("Bearer " + token); err != nil {
		t.Fatalf("the Bearer prefix must be tolerated: %v", err)
	}
}

// The mint COUNT is observable so the report can show the refresh
// working. A fifteen-operation run that minted once is a run that will
// fail at the closing /buildinfo.
func TestMintsCountsRefreshesAndNeverExposesAValue(t *testing.T) {
	credential := MintedCredential("Authorization", "envelope", 0, func(context.Context) (string, error) {
		return "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln", nil
	}).WithShapeValidator(ValidateEnvelopeShape)

	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	for i := 0; i < 3; i++ {
		if err := credential.Apply(context.Background(), request); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if credential.Mints() != 3 {
		t.Fatalf("expected 3 mints, got %d", credential.Mints())
	}
	if credential.Kind() == "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln" {
		t.Fatal("Kind must name the credential, never carry its value")
	}
}
