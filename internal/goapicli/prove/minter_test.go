package prove

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// withFakeMinter substitutes mintCredential (credentials()'s own seam
// onto goapiproof.MintViaAllowlistedHelper) for the duration of one test,
// so a test can supply a synthetic credential value without a real
// envelope signing key or a real Postgres -- the same seam withFakePool
// gives run()'s Postgres connection step. Restored on cleanup.
func withFakeMinter(t *testing.T, mint func(ctx context.Context, helperName string, args []string) (string, error)) {
	t.Helper()
	original := mintCredential
	t.Cleanup(func() { mintCredential = original })
	mintCredential = mint
}

// withOrgMinter is the common case: a synthetic JWT naming edgeOrg for
// the edge (mint-edge-token) credential and proofOrg for the proof
// (mint-envelope) credential -- so a cross-org test case can give each
// credential its own claimed org without a real minting helper.
func withOrgMinter(t *testing.T, edgeOrg, proofOrg string) {
	t.Helper()
	withFakeMinter(t, func(_ context.Context, helperName string, _ []string) (string, error) {
		org, sub := proofOrg, "proof"
		if helperName == "mint-edge-token" {
			org, sub = edgeOrg, "edge"
		}
		return syntheticJWT(t, map[string]string{"sub": sub, "org_id": org}), nil
	})
}

// syntheticJWT builds a JWT-SHAPED value at RUNTIME, so no `eyJ...`
// literal appears anywhere in the tree. Gitleaks' `jwt` rule matches on
// SHAPE, not on whether a value is real, so a synthetic fixture written
// as a literal fails the secret scan exactly like a leaked one -- and the
// answer is to stop writing the shape into the source, not to teach the
// scanner to skip a file (an ignore entry would cover every FUTURE literal
// added there too, which is the opposite of what a secret scan is for).
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

// A malformed STATIC envelope must fail at construction, with a
// message that does not echo it. Before, only the minted path was
// validated, so a static value became a confusing 401 fifteen operations
// later.
func TestAMalformedStaticProofBearerFailsAtConstruction(t *testing.T) {
	const bad = "not-an-envelope-but-still-a-secret"
	t.Setenv(proofBearerEnvVar, bad)

	_, _, err := credentials(flags{orgID: "o"})
	if err == nil {
		t.Fatal("a malformed static proof bearer must be refused before anything is measured")
	}
	if strings.Contains(err.Error(), bad) {
		t.Fatalf("the error echoed the credential: %v", err)
	}
	if !strings.Contains(err.Error(), proofBearerEnvVar) {
		t.Fatalf("the error must name the variable so an operator knows what to fix: %v", err)
	}
}

// And a well-formed one is accepted, so the check above is not simply
// refusing everything.
func TestAWellFormedStaticProofBearerIsAccepted(t *testing.T) {
	t.Setenv(proofBearerEnvVar, syntheticJWT(t, map[string]string{"sub": "u-1"}))
	edge, proof, err := credentials(flags{orgID: "o"})
	if err != nil {
		t.Fatalf("a well-formed static envelope was refused: %v", err)
	}
	if edge == nil || proof == nil {
		t.Fatal("both credentials must be constructed")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, reader)
		done <- buf.String()
	}()
	fn()
	_ = writer.Close()
	os.Stdout = original
	return <-done
}

// The report was once found missing two counters it claimed to compute: an
// earlier edit reverted the computed build-binding line to a hardcoded
// sentence and dropped the mint count entirely. Nothing failed, because no
// test read this output at all.
//
// So this reads it. An instrument nobody asserts on is an instrument that
// silently stops working -- which is the same class as a guard with no
// killer test, one layer out.
func TestTheReportCarriesTheCountersItComputes(t *testing.T) {
	minted := syntheticJWT(t, map[string]string{"sub": "u-1"})
	credential := goapiproof.MintedCredential("Authorization", "envelope", 0,
		func(context.Context) (string, error) { return minted, nil },
	).WithShapeValidator(goapiproof.ValidateEnvelopeShape)
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
	for i := 0; i < 3; i++ {
		if err := credential.Apply(context.Background(), request); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	outcomes := []goapiproof.Outcome{{
		Operation:        "featureFlags",
		Route:            goapiproof.RouteEdge,
		Executed:         true,
		Admitted:         true,
		EdgeBuildBinding: goapiproof.EdgeBuildAbsent,
		TerminalState:    "unsupported",
	}, {
		// The fixture held only ADMITTED outcomes, so dropping the
		// `if outcome.Admitted` filter changed nothing and passed. A
		// refused outcome carries no binding -- counting it would print
		// `build binding  = 1`, an empty name with a real count, which
		// reads as a binding nobody can name rather than as an operation
		// that never got one.
		Operation:     "pr",
		Route:         goapiproof.RouteEdge,
		Executed:      false,
		Admitted:      false,
		RefusalReason: goapiproof.RefusalNotRouted,
		TerminalState: "",
	}}
	summary := goapiproof.Summary{Attempted: 2, Admitted: 1, Executed: 1, Refused: 1}

	printed := captureStdout(t, func() {
		// The previous fixture used "http://edge.test/graphql", which is
		// IDENTICAL under the rebuilt label and the raw string -- so no
		// assertion on that line could have distinguished them. The URL now
		// carries a secret in its QUERY and its PATH -- both of which
		// safeEndpoint accepts at flag parse by design, and both of which the
		// rebuilt label strips -- so the two forms now differ.
		if err := emitReport(flags{orgID: "o", edgeURL: "http://edge.test/graphql?token=s3cret-happy-path", proofURL: "http://proof.test/query/proof/s3cret-proof"},
			goapiproof.RegistryView{SchemaDigest: "sha256:x", BuildIdentity: "b"}, goapiproof.ProverBuild{Candidate: "b"},
			outcomes, summary, credential, nil, exitCompleted, nil); err != nil {
			t.Fatalf("emitReport: %v", err)
		}
	})

	if !strings.Contains(printed, "envelope mints = 3") { // 3, so neither 0 nor 1 can pass
		t.Fatalf("the mint COUNT must reach the report -- a run that minted once will fail at the closing build check, and this line is the only warning:\n%s", printed)
	}
	if !strings.Contains(printed, "build binding "+goapiproof.EdgeBuildAbsent+" = 1") {
		t.Fatalf("the build-binding counter must be COUNTED from the outcomes, not asserted as a sentence:\n%s", printed)
	}
	// Only ADMITTED outcomes have a binding. A refused one counted
	// here prints an empty binding name with a real count.
	if strings.Contains(printed, "build binding  =") {
		t.Fatalf("a refused outcome was counted as a build binding, printing an empty name:\n%s", printed)
	}
	if strings.Contains(printed, goapiproof.EdgeBuildAbsent+" = 2") {
		t.Fatalf("the refused outcome was folded into the admitted count:\n%s", printed)
	}
	// And it must never print the credential itself.
	if strings.Contains(printed, minted) || strings.Contains(printed, strings.SplitN(minted, ".", 2)[0]) {
		t.Fatalf("the report printed the credential:\n%s", printed)
	}
	// The endpoint line is printed on EVERY successful run and is what
	// gets pasted into a ticket. It must carry the rebuilt label, never the
	// operator's raw URL.
	for _, secret := range []string{"s3cret-happy-path", "s3cret-proof"} {
		if strings.Contains(printed, secret) {
			t.Fatalf("the happy-path endpoint line leaked %q:\n%s", secret, printed)
		}
	}
	if !strings.Contains(printed, "edge=http://edge.test") {
		t.Fatalf("the endpoint must still be NAMED, or an operator cannot tell which one ran:\n%s", printed)
	}
}

// Two surviving mutations in this command's guards -- both unrelated to
// minting.
func TestThisCommandsGuardsAreKillable(t *testing.T) {
	t.Run("the routing cross-check cannot be declined", func(t *testing.T) {
		// A mutation killed the previous version by replacing the caller's
		// `return err` with `_ = err`: the check was a separate statement
		// and could be ignored. It is now folded into readRoutingState,
		// which returns the rows and the verdict together -- so this
		// asserts BEHAVIOUR (no rows come back on a disagreement) rather
		// than the presence of an `if` in the source.
		err := goapiproof.VerifyCandidateBuild("running-build", "", map[string]goapiproof.RoutingRow{
			"featureFlags": {Mode: "canary", CandidateBuild: "some-other-build"},
			"hotspots":     {Mode: "shadow", CandidateBuild: "another-build"},
			"flowMatrix":   {Mode: "canary", CandidateBuild: "running-build"},
		})
		if err == nil {
			t.Fatal("a row naming another build must refuse the run")
		}
		for _, want := range []string{"featureFlags", "hotspots"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the refusal must name %s: %v", want, err)
			}
		}
		if strings.Contains(err.Error(), "flowMatrix") {
			t.Fatalf("an agreeing row must not be named: %v", err)
		}
	})

	t.Run("the binding line is counted, not hardcoded", func(t *testing.T) {
		// Hardcoding `absent=1` once survived undetected, so this drives a PRESENT
		// binding and asserts the report says so.
		// THREE outcomes across TWO bindings. A mutation killed the single-element
		// version by hardcoding the counter to 1: a fixture with one
		// element cannot tell a count from a constant.
		outcomes := []goapiproof.Outcome{
			{Operation: "featureFlags", Route: goapiproof.RouteEdge, Executed: true, Admitted: true,
				EdgeBuildBinding: goapiproof.EdgeBuildPresent, TerminalState: "match"},
			{Operation: "hotspots", Route: goapiproof.RouteEdge, Executed: true, Admitted: true,
				EdgeBuildBinding: goapiproof.EdgeBuildPresent, TerminalState: "match"},
			{Operation: "flowMatrix", Route: goapiproof.RouteEdge, Executed: true, Admitted: true,
				EdgeBuildBinding: goapiproof.EdgeBuildAbsent, TerminalState: "unsupported"},
		}
		printed := captureStdout(t, func() {
			_ = emitReport(flags{orgID: "o", edgeURL: "http://edge.test/graphql"},
				goapiproof.RegistryView{SchemaDigest: "s", BuildIdentity: "b"}, goapiproof.ProverBuild{Candidate: "b"},
				outcomes, goapiproof.Summary{Attempted: 1, Admitted: 1, Executed: 1}, nil, nil, exitCompleted, nil)
		})
		// 2 present, 1 absent -- neither number is 1, so a constant
		// cannot satisfy both.
		if !strings.Contains(printed, "build binding "+goapiproof.EdgeBuildPresent+" = 2") {
			t.Fatalf("the present count did not follow the outcomes:\n%s", printed)
		}
		if !strings.Contains(printed, "build binding "+goapiproof.EdgeBuildAbsent+" = 1") {
			t.Fatalf("the absent count did not follow the outcomes:\n%s", printed)
		}
	})
}

// Mutations that bypassed URL validation or stretched the credential
// freshness window all survived, because `run()` has no test and a guard
// inside it is a guard nobody holds. These pin the pieces that CAN be
// reached without a harness for the whole command; the harness itself is
// a follow-up.
func TestTheCLIsOwnGuardsAreReachable(t *testing.T) {
	t.Run("endpoint flags are validated", func(t *testing.T) {
		for _, bad := range []flags{
			{registryURL: "http://alice:s3cret@host/registry"},
			{buildInfoURL: "http:s3cret@host/buildinfo"},
			{edgeURL: "ftp://host/graphql"},
			{proofURL: "http://alice@host/query/proof"},
		} {
			if err := validateEndpointFlags(bad); err == nil {
				t.Fatalf("a credential-bearing endpoint flag was accepted: %+v", bad)
			} else if strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("the refusal echoed the credential: %v", err)
			}
		}
		if err := validateEndpointFlags(flags{
			registryURL:  "http://query-api.test:8090/registry",
			buildInfoURL: "http://query-api.test:8090/buildinfo",
			edgeURL:      "http://api.test:8000/graphql",
		}); err != nil {
			t.Fatalf("ordinary endpoints were refused: %v", err)
		}
	})

	t.Run("the credential freshness window stays well inside the TTL", func(t *testing.T) {
		// ENVELOPE_DEFAULT_TTL_SECONDS is 60. Stretching this to 250s
		// once survived: a value at or past the TTL means every request
		// after the first carries an expired envelope.
		if proofCredentialFreshness >= 60*time.Second {
			t.Fatalf("freshness %s is not inside the 60s envelope TTL", proofCredentialFreshness)
		}
		if proofCredentialFreshness > 30*time.Second {
			t.Fatalf("freshness %s leaves too little margin for the request to arrive and be verified", proofCredentialFreshness)
		}
	})
}

// The one place in the shipped binary where the shape validator is
// JOINED to the minted credential had no killer. ValidateEnvelopeShape is
// well pinned and MintedCredential(...).WithShapeValidator(...) is
// exercised, but dropping `.WithShapeValidator(...)` from credentials()
// survived -- and without it a minter printing a usage line is installed
// as an Authorization header.
func TestTheMintedProofCredentialIsShapeValidated(t *testing.T) {
	withFakeMinter(t, func(_ context.Context, helperName string, _ []string) (string, error) {
		if helperName == "mint-envelope" {
			return "usage: mint-envelope [--org ORG]", nil
		}
		return syntheticJWT(t, map[string]string{"sub": "edge"}), nil
	})

	_, proof, err := credentials(flags{orgID: "o"})
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}

	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	if err := proof.Apply(context.Background(), request); err == nil {
		t.Fatal("a minter printing a usage line was installed as a credential: the shape validator is not wired to the minted credential")
	}
	if request.Header.Get("Authorization") != "" {
		t.Fatal("a malformed credential reached the request")
	}
}

// The minted edge credential is shape-checked and re-minted like the
// proof one, and its mint COUNT reaches the report.
func TestTheMintedEdgeCredentialIsShapeValidatedAndCounted(t *testing.T) {
	withFakeMinter(t, func(_ context.Context, helperName string, _ []string) (string, error) {
		if helperName == "mint-edge-token" {
			return "usage: mint-edge-token -org ORG", nil
		}
		return syntheticJWT(t, map[string]string{"sub": "proof"}), nil
	})
	edge, _, err := credentials(flags{orgID: "o"})
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	request, _ := http.NewRequest(http.MethodPost, "http://example.invalid/graphql", nil)
	if err := edge.Apply(context.Background(), request); err == nil || request.Header.Get("Authorization") != "" {
		t.Fatalf("a minter printing a usage line was installed as the edge credential (err=%v)", err)
	}

	token := syntheticJWT(t, map[string]string{"sub": "edge-principal"})
	withFakeMinter(t, func(_ context.Context, helperName string, _ []string) (string, error) {
		if helperName == "mint-edge-token" {
			return token, nil
		}
		return syntheticJWT(t, map[string]string{"sub": "proof"}), nil
	})
	edge, _, err = credentials(flags{orgID: "o"})
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	request, _ = http.NewRequest(http.MethodPost, "http://example.invalid/graphql", nil)
	if err := edge.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if request.Header.Get("Authorization") != "Bearer "+token {
		t.Fatal("the minted edge token did not reach the request as a bearer")
	}

	printed := captureStdout(t, func() {
		_ = emitReport(flags{orgID: "o", edgeURL: "http://edge.test/graphql"},
			goapiproof.RegistryView{SchemaDigest: "s", BuildIdentity: "b"}, goapiproof.ProverBuild{Candidate: "b"},
			nil, goapiproof.Summary{}, nil, edge, exitCompleted, nil)
	})
	if !strings.Contains(printed, "edge access token mints = 1") {
		t.Fatalf("the edge mint count must reach the report:\n%s", printed)
	}
	if strings.Contains(printed, token) {
		t.Fatalf("the report printed the edge token:\n%s", printed)
	}
}
