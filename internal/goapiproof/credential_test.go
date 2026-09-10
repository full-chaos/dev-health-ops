package goapiproof

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
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
	all    []string
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

// record APPENDS. r7 found the map version hiding a rejected candidate:
// the baseline leg runs after the candidate and overwrote the recorded
// value, so sending the proof credential on a canary candidate leg looked
// identical to sending the right one. A fixture that keeps only the last
// observation cannot see the first.
func (s *twoPlaneServers) record(plane, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[plane] = value
	s.all = append(s.all, plane+" "+value)
}

// credentialsSeen returns every (plane, credential) pair observed, in
// order.
func (s *twoPlaneServers) credentialsSeen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.all...)
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

	// BOTH modes, not just shadow. r6 killed the shadow-only version by
	// sending the proof credential on a CANARY candidate leg: that leg
	// goes to the edge, the fixture never exercised it, and the mutation
	// survived.
	for _, mode := range []string{"shadow", "canary"} {
		runner := newTwoPlaneRunner(t, servers,
			StaticCredential("Authorization", "edge access token", servers.edgeOK),
			StaticCredential("Authorization", "envelope", envelope),
			mode)
		if _, _, err := runner.Run(context.Background()); err != nil && mode == "shadow" {
			t.Fatalf("Run(%s): %v", mode, err)
		}
		// EVERY edge observation, not the last one: the baseline leg runs
		// after the candidate and used to overwrite it.
		for _, seen := range servers.credentialsSeen() {
			if strings.HasPrefix(seen, "edge ") && seen != "edge "+servers.edgeOK {
				t.Fatalf("mode=%s: an edge leg carried %q, not the access token", mode, seen)
			}
			if strings.HasPrefix(seen, "proof ") && seen != "proof "+envelope {
				t.Fatalf("mode=%s: a proof leg carried %q, not the envelope", mode, seen)
			}
		}
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

// syntheticJWT builds a JWT-SHAPED value at RUNTIME, from segments this
// function encodes itself.
//
// It exists so no `eyJ...` literal appears anywhere in the tree. Gitleaks'
// `jwt` rule matches on shape, not on whether a value is real, so a
// synthetic fixture written as a literal fails the secret scan exactly
// like a leaked one -- and the right answer is to stop writing the shape
// into the source, not to teach the scanner to skip a file. (An ignore
// entry would also cover any FUTURE literal added to that file, which is
// the opposite of what a secret scan is for.)
//
// Building it from marshalled structs also states what these tests are
// actually about: three non-empty base64url segments joined by dots, the
// shape jwt.encode produces.
func syntheticJWT(t *testing.T, claims map[string]string) string {
	t.Helper()
	segment := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal a JWT segment: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return strings.Join([]string{
		segment(map[string]string{"alg": "EdDSA"}),
		segment(claims),
		base64.RawURLEncoding.EncodeToString([]byte("synthetic-signature")),
	}, ".")
}

// jwtSegment is the same, for ONE segment, so the malformed-shape table
// below can be built without a literal either.
func jwtSegment(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal a JWT segment: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// A minting command that prints something OTHER than a token -- a usage
// line, an error, a JSON blob, a shell prompt -- must be refused at the
// source. Sending it produces a 401 that reads exactly like a rejected
// credential, which is the confusion this whole file exists to remove.
func TestAMintedValueThatIsNotAnEnvelopeIsRefused(t *testing.T) {
	header := jwtSegment(t, map[string]string{"alg": "EdDSA"})
	payload := jwtSegment(t, map[string]string{"sub": "u-1"})
	signature := base64.RawURLEncoding.EncodeToString([]byte("sig"))

	for name, printed := range map[string]string{
		"a usage line":        "usage: mint-envelope [--org ORG]",
		"an error message":    "Error: no such container: api",
		"a JSON blob":         `{"envelope":"` + header + `"}`,
		"an opaque token":     "abcdef0123456789",
		"two segments":        header + "." + payload,
		"an empty segment":    header + ".." + signature,
		"a token with a tab":  header + "." + payload + "." + signature + "\there",
		"non-base64url bytes": header + "." + payload + ".++" + signature + "++",
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
	token := syntheticJWT(t, map[string]string{"sub": "u-1", "org_id": "o-1"})
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
	minted := syntheticJWT(t, map[string]string{"sub": "u-1"})
	credential := MintedCredential("Authorization", "envelope", 0, func(context.Context) (string, error) {
		return minted, nil
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
	if credential.Kind() == minted {
		t.Fatal("Kind must name the credential, never carry its value")
	}
}

// r1 P1, adapted to the restored contract. The reviewer proved a run could
// produce a receipt naming build A while the measured request was served
// by build B: /buildinfo answers from one replica, the measured /graphql
// request is served by another, and the Python edge drops the per-request
// build header that would tell them apart.
//
// Two defences now exist and this asserts both.
func TestMixedReplicasCannotProduceAReceiptForTheWrongBuild(t *testing.T) {
	// Defence 1: the routing row's build is a cross-check on the fleet
	// being on ONE build, and a disagreement refuses before any request
	// is sent.
	t.Run("a stale routing row refuses the run outright", func(t *testing.T) {
		err := VerifyCandidateBuild("build-A", "", map[string]RoutingRow{
			"featureFlags": {Mode: "canary", CandidateBuild: "build-B"},
		})
		if err == nil {
			t.Fatal("with replicas possibly on different builds, a row naming another build must refuse")
		}
	})

	// Defence 2: when the edge DOES carry a per-request build header and
	// it names a different build, that is a measured disagreement and
	// admission refuses it -- the mixed-replica case caught in the act.
	t.Run("a measured response naming another build is refused", func(t *testing.T) {
		admission := Admit(admissionWithData("build-B", "build-A"))
		if admission.Admitted {
			t.Fatal("a response served by build-B must not back a receipt naming build-A")
		}
		if admission.Reason != RefusalBuildMismatch {
			t.Fatalf("expected %s, got %s", RefusalBuildMismatch, admission.Reason)
		}
	})
}

// The absence of an edge build header is RECORDED rather than assumed
// away. It is CHAOS-5479's known gap, and a receipt that did not say
// whether it had a per-request binding cannot be told apart later from one
// that did.
func TestTheEdgeBuildBindingIsRecordedWhetherPresentOrAbsent(t *testing.T) {
	for name, testCase := range map[string]struct {
		build string
		want  string
	}{
		"edge dropped the header (CHAOS-5479, today's normal)": {"", EdgeBuildAbsent},
		"edge carried it and it agreed":                        {"build-A", EdgeBuildPresent},
	} {
		t.Run(name, func(t *testing.T) {
			admission := Admit(admissionWithData(testCase.build, "build-A"))
			if !admission.Admitted {
				t.Fatalf("unexpected refusal: %s %s", admission.Reason, admission.Detail)
			}
			if admission.EdgeBuildBinding != testCase.want {
				t.Fatalf("got binding %q, want %q", admission.EdgeBuildBinding, testCase.want)
			}
		})
	}
}

// admissionWithData builds an input that passes every precondition except
// the one under test, so a failure here is about the BUILD binding and not
// about an empty response body.
func admissionWithData(servedBuild, namedBuild string) AdmissionInput {
	snapshot := Snapshot{
		DataPresent: true,
		Data:        map[string]any{"featureFlags": []any{map[string]any{"key": "a"}}},
	}
	return AdmissionInput{
		Route:         RouteEdge,
		NamedBuild:    namedBuild,
		ResponseRoot:  "featureFlags",
		Candidate:     Observation{Plane: "go", StatusCode: 200, Build: servedBuild},
		Baseline:      Observation{Plane: "python", StatusCode: 200},
		CandidateSnap: snapshot,
		BaselineSnap:  snapshot,
	}
}

// r2 P1, the last hole: a deployment that BEGINS during the run defeats
// every other defence at once. The routing row legitimately names the
// running build, /buildinfo answers from the old replica before and after,
// and the measured request is served by the new one in between.
//
// With no per-request build header there is nothing left that can tell
// those apart -- so an edge measurement without one may never be
// enablement-eligible, whatever the comparison said.
func TestAnEdgeMeasurementWithNoBuildBindingCannotBeEnablementEligible(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	// The fake edge stamps no build header -- today's normal, and the
	// rolling-deploy window.
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]

	if outcome.TerminalState == TerminalStateMatch {
		t.Fatal("an unbound edge measurement produced a `match`: it would satisfy the enablement predicate for a build nothing showed served the request")
	}
	if outcome.TerminalState != TerminalStateUnsupported {
		t.Fatalf("expected %q, got %q", TerminalStateUnsupported, outcome.TerminalState)
	}
	// The measurement still HAPPENED, and the run must stay visible.
	if !outcome.Executed {
		t.Fatal("the measurement happened and must be recorded, not discarded")
	}
	if outcome.EdgeBuildBinding != EdgeBuildAbsent {
		t.Fatalf("the binding must be recorded as absent, got %q", outcome.EdgeBuildBinding)
	}

	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	var provenance ReceiptProvenance
	if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil {
		t.Fatal(err)
	}
	if provenance.EdgeBuildBinding != EdgeBuildAbsent {
		t.Fatalf("the receipt must SAY why it is unsupported: %+v", provenance)
	}
}

// The mirror, and the control: with the header present and equal to the
// running build, the same responses DO produce a match. Without this the
// test above would pass against an instrument that never matches anything.
func TestAnEdgeMeasurementBoundToTheRunningBuildStillMatches(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateMatch {
		t.Fatalf("a bound, agreeing measurement must match, got %q (%v)", outcomes[0].TerminalState, outcomes[0].Findings)
	}
	if outcomes[0].EdgeBuildBinding != EdgeBuildPresent {
		t.Fatalf("expected %q, got %q", EdgeBuildPresent, outcomes[0].EdgeBuildBinding)
	}
}

// The operator note is bounded, and refused rather than truncated -- a
// silently shortened note reads like a whole one, the same reason the
// helper's output bound refuses.
func TestAnOverLongOperatorNoteIsRefused(t *testing.T) {
	if err := ValidateOperatorEvidence(strings.Repeat("a", MaxOperatorEvidenceBytes)); err != nil {
		t.Fatalf("a note exactly at the limit must be accepted: %v", err)
	}
	err := ValidateOperatorEvidence(strings.Repeat("a", MaxOperatorEvidenceBytes+1))
	if err == nil {
		t.Fatal("an over-long operator note must be refused")
	}
	if !errors.Is(err, ErrOperatorEvidenceTooLong) {
		t.Fatalf("the refusal must be identifiable: %v", err)
	}
}

// An unbound MISMATCH keeps its verdict -- and is disqualified anyway.
//
// This test used to pin the downgrade as "scoped to `match` on purpose",
// on the reasoning that "a mismatch already authorizes nothing, so there
// is nothing to protect against". CHAOS-5484 made that false in the same
// PR this test lives in: a fully-cited mismatch IS enablement proof. The
// test went on passing and pinned the hole shut (astra r3, P1).
//
// What is true, and what this pins now: rewriting the verdict would
// DESTROY the divergence the run found -- "these planes disagree" is not
// "we could not tell" -- so the mismatch stands, and the missing binding
// is counted as a difference outside the cited baseline defect instead.
// The record stays honest and the receipt cannot authorize anything.
func TestAnUnboundMismatchStaysAMismatchAndCannotAuthorize(t *testing.T) {
	runner := newRunner(t, &fakeEdge{
		goBody:     `{"data":{"featureFlags":[{"key":"a"}]}}`,
		pythonBody: `{"data":{"featureFlags":[{"key":"b"}]}}`,
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateMismatch {
		t.Fatalf("an unbound measurement that DIVERGED must stay a mismatch, got %q -- the divergence is the finding and must not be erased", outcomes[0].TerminalState)
	}
	if outcomes[0].EdgeBuildBinding != EdgeBuildAbsent {
		t.Fatalf("the absent binding must still be recorded: %q", outcomes[0].EdgeBuildBinding)
	}
	// ...and it authorizes nothing, which is the half this test used to
	// assert away. An unbound measurement cannot be proof in ANY mode.
	if outcomes[0].DifferencesOutsideBaselineDefect < 1 {
		t.Fatalf("an UNBOUND mismatch reports outside=%d: with every body difference cited it would be admitted as enablement proof, for primary as well as canary, on evidence tied to no replica",
			outcomes[0].DifferencesOutsideBaselineDefect)
	}
	if !slices.ContainsFunc(outcomes[0].Findings, func(f Finding) bool {
		return f.Path == "$.http.header."+buildHeader
	}) {
		t.Fatal("the missing build header must be a NAMED finding, not just a counter bump: an operator reading the receipt has to see why it was disqualified")
	}
}

// r3 P1: the exported Admitted bool was not a boundary. A caller in any
// package could build an Outcome with the bit already set and get an
// enablement-shaped receipt for responses that never passed Admit.
//
// R57 made Admit the only door on the production path; this makes it the
// only door. The receipt constructors read an UNEXPORTED field that only
// proveOne writes, so the hand-built outcome below produces nothing --
// r5 P1. Sealing one field at a time did not work, three rounds running.
//
// r3 sealed `admitted`; r4 sealed the verdict; r5 then relabelled the
// BUILD on a genuine admitted match and got
// `receipt build=never-measured-build terminal=match`. Every fix closed
// the field just used and left the rest open.
//
// Receipts now come from records the run sealed, and the exported Outcome
// is a report view nothing reads. So this mutates EVERY exported field of
// that view after a real run and asserts the receipts are byte-identical:
// not "these fields are protected", but "the view is not an input".
func TestMutatingTheReportViewCannotChangeAReceipt(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	observedAt := time.Now().UTC()
	before, err := runner.ReceiptsFor(observedAt)
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("expected one receipt, got %d", len(before))
	}

	// Every exported field of the view, rewritten to something the run
	// never measured.
	for i := range outcomes {
		outcomes[i].Operation = "hotspots"
		outcomes[i].DocumentDigest = "never-measured-digest"
		outcomes[i].Mode = "primary"
		outcomes[i].Route = RouteProof
		outcomes[i].EdgeBuildBinding = EdgeBuildPresent
		outcomes[i].RoutingRowBuild = "never-measured-build"
		outcomes[i].TerminalState = TerminalStateMatch
		outcomes[i].Executed = true
		outcomes[i].Admitted = true
		outcomes[i].RefusalReason = ""
		outcomes[i].RefusalDetail = ""
		outcomes[i].Findings = nil
		outcomes[i].BaselineDefects = []string{"CHAOS-0000"}
		outcomes[i].DifferencesOutsideBaselineDefect = 99
		outcomes[i].Candidate = nil
		outcomes[i].Baseline = nil
		outcomes[i].ReceiptWritten = true
	}

	after, err := runner.ReceiptsFor(observedAt)
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("mutating the report view changed the receipt:\nbefore %+v\nafter  %+v", before[0], after[0])
	}

	refusals, err := runner.RefusalReceipts(observedAt, "build moved")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	for _, receipt := range refusals {
		if receipt.SelectedOperation != "featureFlags" || receipt.CandidateBuild != runner.Registry.BuildIdentity {
			t.Fatalf("a refusal receipt took its identity from the mutated view: %+v", receipt)
		}
	}
}

// The seal is only a seal while every field stays unexported. An exported
// one is assignable from outside the package, which is the whole defect
// class, so this fails the moment somebody adds one.
func TestSealedOutcomeHasNoExportedFields(t *testing.T) {
	sealedType := reflect.TypeOf(sealedOutcome{})
	for i := 0; i < sealedType.NumField(); i++ {
		field := sealedType.Field(i)
		if field.IsExported() {
			t.Fatalf("sealedOutcome.%s is exported: a receipt must not be built from anything a caller can assign", field.Name)
		}
	}
	if sealedType.NumField() == 0 {
		t.Fatal("sealedOutcome has no fields -- this test would pass vacuously")
	}
}

// The control: an outcome that DID pass Admit still produces its receipt.
// Without this the test above would pass against a constructor that never
// produces anything.
func TestAnAdmittedOutcomeStillProducesAReceipt(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 1 {
		t.Fatalf("an admitted outcome must still produce its receipt, got %d", len(receipts))
	}
}

// r3 P1: a VALUE copy of a Credential leaked. The redaction methods had
// pointer receivers, so fmt fell through to the struct printer for a copy.
// My own tests passed because they only ever formatted the pointer -- so
// this formats BOTH, through every verb that reaches fmt differently.
func TestCredentialRedactsAsBothValueAndPointer(t *testing.T) {
	const secret = "review-value-copy-secret-7d3f"
	pointer := StaticCredential("Authorization", "edge access token", secret)
	value := *pointer // the copy that leaked

	for name, rendered := range map[string]string{
		"pointer %v":     fmt.Sprintf("%v", pointer),
		"pointer %+v":    fmt.Sprintf("%+v", pointer),
		"pointer %#v":    fmt.Sprintf("%#v", pointer),
		"pointer %s":     fmt.Sprintf("%s", pointer),
		"pointer Sprint": fmt.Sprint(pointer),
		"value %v":       fmt.Sprintf("%v", value),
		"value %+v":      fmt.Sprintf("%+v", value),
		"value %#v":      fmt.Sprintf("%#v", value),
		"value %s":       fmt.Sprintf("%s", value),
		"value Sprint":   fmt.Sprint(value),
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("%s exposed the credential: %s", name, rendered)
		}
		if !strings.Contains(rendered, "REDACTED") {
			t.Fatalf("%s did not render the redaction: %s", name, rendered)
		}
	}

	// And a copy still WORKS -- sharing one state rather than being inert.
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
	if err := value.Apply(context.Background(), request); err != nil {
		t.Fatalf("a copied credential must still authenticate: %v", err)
	}
	if request.Header.Get("Authorization") != secret {
		t.Fatal("the copy did not carry the credential")
	}
}

// r3 P2: whitespace is an empty credential wearing a disguise -- the
// server answers the same 401 either way.
func TestAWhitespaceOnlyCredentialIsRefused(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
	for _, value := range []string{"", "   ", "\t", "\n", "Bearer    "} {
		credential := StaticCredential("Authorization", "edge access token", value)
		if err := credential.Apply(context.Background(), request); err == nil {
			t.Fatalf("credential %q was installed", value)
		}
	}
}

// r3 P1 / lane-routing-verbs: a URL is never printed raw, and the obvious
// url.Redacted() is NOT safe -- on a URL with no "//" the parser reads the
// username as the scheme, User is nil, and Redacted() returns the password
// verbatim. EndpointLabel rebuilds instead.
func TestEndpointLabelNeverEmitsACredential(t *testing.T) {
	const secret = "supersecret"
	for name, raw := range map[string]string{
		"ordinary userinfo":     "http://alice:" + secret + "@host:8090/registry",
		"no scheme separator":   "alice:" + secret + "@host/registry",
		"scheme-relative":       "//alice:" + secret + "@host/registry",
		"password only":         "http://:" + secret + "@host/registry",
		"credential in path":    "http://host/" + secret,
		"credential in query":   "http://host/r?token=" + secret,
		"credential in framgnt": "http://host/r#" + secret,
	} {
		t.Run(name, func(t *testing.T) {
			label := EndpointLabel(raw)
			if strings.Contains(label, secret) {
				t.Fatalf("EndpointLabel(%q) = %q -- it emitted the password", raw, label)
			}
			// The USERNAME too. lane-routing-verbs shipped a boundary that
			// kept the password safe and then named the offending scheme in
			// its refusal -- and on the no-"//" form the scheme IS the
			// username. Checking only the password is how that passes
			// review: on that form EVERY field you might safely name has
			// become part of the credential.
			if strings.Contains(label, "alice") {
				t.Fatalf("EndpointLabel(%q) = %q -- it emitted the username", raw, label)
			}
		})
	}

	// It still SAYS something useful for a URL it can fully account for,
	// or an operator cannot tell which endpoint failed.
	if got := EndpointLabel("http://query-api.test:8090/registry"); got != "http://query-api.test" {
		t.Fatalf("got %q, want a rebuilt scheme://host label", got)
	}
	// A URL carrying userinfo is NOT named, even though its scheme and
	// host are safe to rebuild. One predicate decides both "may this be
	// accepted" and "may this be described", because r4 showed what
	// happens when those two checks are separate and only one gets fixed:
	// they agree right up until they do not. The flag guard refuses this
	// shape long before an error could need to name it.
	if got := EndpointLabel("http://alice:s3cret@query-api.test:8090/registry"); got != "(unparseable endpoint)" {
		t.Fatalf("got %q, want the placeholder -- one predicate, not two", got)
	}
}

// The refusal at the flag boundary, including the form that defeats a
// userinfo check.
func TestRefuseCredentialsInURL(t *testing.T) {
	for name, raw := range map[string]string{
		"userinfo":            "http://alice:s3cret@host/registry",
		"username only":       "http://alice@host/registry",
		"no scheme separator": "alice:s3cret@host/registry",
		"not http":            "file:///etc/passwd",
	} {
		t.Run(name, func(t *testing.T) {
			err := RefuseCredentialsInURL("-registry-url", raw)
			if err == nil {
				t.Fatalf("%q was accepted", raw)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("the refusal echoed the credential: %v", err)
			}
		})
	}
	if err := RefuseCredentialsInURL("-registry-url", "http://query-api.test:8090/registry"); err != nil {
		t.Fatalf("an ordinary URL was refused: %v", err)
	}
}

// r4 P1-3. Sealing `admitted` stopped a HAND-BUILT outcome. It did not
// stop mutating a REAL one: after a genuine run, flipping only the
// exported TerminalState from `unsupported` to `match` produced a
// deployed_executed/match receipt whose own provenance still said
// edge_build_binding=absent.
//
// The seal covered "this passed the gate" and left "what the gate
// concluded" exported and writable, which is the same lesson as r3 one
// field over. Receipts now derive the terminal state from the sealed run.
func TestTheTerminalStateCannotBeReplacedAfterTheRun(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	// No build header: this run legitimately terminates `unsupported`.
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateUnsupported {
		t.Fatalf("precondition: expected unsupported, got %q", outcomes[0].TerminalState)
	}

	// The attack: change nothing about the measurement, only the verdict.
	outcomes[0].TerminalState = TerminalStateMatch

	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	for _, receipt := range receipts {
		if receipt.TerminalState == TerminalStateMatch {
			t.Fatalf("an unbound measurement was rewritten into a match receipt by assigning a field: %+v", receipt)
		}
	}
}

// The control: a run that genuinely matched still writes `match`.
func TestASealedMatchStillReachesTheReceipt(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 1 || receipts[0].TerminalState != TerminalStateMatch {
		t.Fatalf("a genuine match must reach the receipt, got %+v", receipts)
	}
}

// r5 P3: eight mutations survived their suites. Each is a guard this PR
// adds, so each gets a killer here rather than a ticket. A guard nothing
// can kill is a guard nobody is holding.
func TestTheGuardsThisChangeAddsAreKillable(t *testing.T) {
	t.Run("nil credentials are refused, not sent", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
		var absent *Credential
		if err := absent.Apply(context.Background(), request); err == nil {
			t.Fatal("a nil credential must refuse")
		}
		if request.Header.Get("Authorization") != "" {
			t.Fatal("an unauthenticated request was built")
		}
	})

	t.Run("a positive freshness window still expires", func(t *testing.T) {
		calls := 0
		// A window shorter than the gap below: the second Apply must
		// re-mint. Disabling only the positive-window branch survived r5.
		credential := MintedCredential("Authorization", "envelope", time.Nanosecond,
			func(context.Context) (string, error) {
				calls++
				return fmt.Sprintf("%s%d", syntheticJWT(t, map[string]string{"sub": "u-1"}), calls), nil
			})
		request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
		if err := credential.Apply(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := credential.Apply(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatalf("a credential past its freshness window was reused: minted %d times", calls)
		}
	})

	t.Run("stale rows are both counted and recorded", func(t *testing.T) {
		body := `{"data":{"featureFlags":[{"key":"a"}]}}`
		edge := &fakeEdge{goBody: body, pythonBody: body}
		runner := newRunner(t, edge, "canary")
		edge.goBuild = runner.Registry.BuildIdentity
		runner.Routing["featureFlags"] = RoutingRow{Mode: "canary", CandidateBuild: "0000000000000000000000000000000000000000"}

		outcomes, summary, err := runner.Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if summary.StaleRoutingRows != 1 {
			t.Fatalf("the stale row was not COUNTED: %d", summary.StaleRoutingRows)
		}
		if outcomes[0].RoutingRowBuild == "" {
			t.Fatal("the stale row was not RECORDED on the outcome")
		}
		receipts, err := runner.ReceiptsFor(time.Now().UTC())
		if err != nil {
			t.Fatalf("ReceiptsFor: %v", err)
		}
		if len(receipts) == 0 || !strings.Contains(receipts[0].ReviewEvidence, "0000000000000000000000000000000000000000") {
			t.Fatal("the stale row did not reach the receipt")
		}
	})

}
