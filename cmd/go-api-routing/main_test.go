package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// `all-registered` is the ONLY spelling of "every row". An empty value is
// the operator naming nothing, and is refused below -- see
// TestRequestedOperationsRefusesAFilterThatNamesNothing.
func TestRequestedOperationsTreatsAllRegisteredAsEveryRow(t *testing.T) {
	for _, raw := range []string{"all-registered", "  all-registered  "} {
		got, err := requestedOperations(raw)
		if err != nil {
			t.Fatalf("requestedOperations(%q) = %v, want no error", raw, err)
		}
		if got != nil {
			t.Fatalf("requestedOperations(%q) = %v, want nil (every row)", raw, got)
		}
	}
}

func TestRequestedOperationsTrimsAndDropsEmptyNames(t *testing.T) {
	got, err := requestedOperations(" flowMatrix , hotspots ,, ")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"flowMatrix", "hotspots"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requestedOperations = %v, want %v", got, want)
	}
}

// CHAOS-5486 round 1, F1 (reproduced by the lane before fixing): a
// separators-only --operations produced an EMPTY filter, which Repoint reads
// as "every row at the digest" -- an operator who named something got a
// silent re-point of everything. Asking for all rows must be explicit.
//
// THE EMPTY-STRING CASES ARE THE SECOND HALF, found by a codex round
// probing the binary: the first fix closed `","` and left `""` reading as
// "all", one input away. This very test asserted that defective behaviour,
// which is why the first fix looked complete -- a test that encodes the
// bug is worse than no test, because it makes the gap look covered.
func TestRequestedOperationsRefusesAFilterThatNamesNothing(t *testing.T) {
	for _, raw := range []string{",", " , ", ",,,", " ,, , ", "", "   ", "\t\n"} {
		got, err := requestedOperations(raw)
		if !errors.Is(err, errEmptyOperationFilter) {
			t.Fatalf("requestedOperations(%q) = (%v, %v), want errEmptyOperationFilter -- a write verb must never widen silently", raw, got, err)
		}
		if got != nil {
			t.Fatalf("requestedOperations(%q) returned %v alongside its error", raw, got)
		}
	}
}

// The credential env var must name the ENVELOPE, not the edge access
// token: /buildinfo checks the envelope and rejects the access token with
// 401. Naming the wrong one sends an operator into a 401 that reads like
// an authorization failure rather than a wrong credential KIND.
func TestBearerEnvVarIsDistinctFromTheProveEdgeToken(t *testing.T) {
	if bearerEnvVar == "GO_API_PROVE_BEARER" {
		t.Fatal("this command needs the envelope; GO_API_PROVE_BEARER is go-api-prove's EDGE access token")
	}
	if bearerEnvVar == "" {
		t.Fatal("the credential must come from a named environment variable, never a flag: a flag value reaches ps and shell history")
	}
}

// CHAOS-5479 changed FetchBuildIdentity's credential parameter, and this
// command's call site had to change with it.
//
// r11 S3: this test used to BUILD its own StaticCredential with the same
// three arguments the production line uses, which proved only that the
// test agrees with itself -- every wrong-argument mutation at the real
// call site survived it. It now calls buildInfoCredential, the one-line
// constructor `runCommand` uses, so each of the three independently
// wrong-able arguments has a killer:
//
//   - the header NAME: /buildinfo reads Authorization; anything else
//     arrives unauthenticated and 401s for the wrong reason.
//   - the `Bearer ` scheme prefix: without it the value is not a bearer
//     credential at all.
//   - the `kind` string: /buildinfo checks the effective-principal
//     envelope and 401s an edge access token, so this is the word that
//     tells an operator WHICH credential was refused. Passing the wrong
//     one is a defect that only shows up against a real stack.
func TestTheBuildInfoReadCarriesTheEnvelope(t *testing.T) {
	bearer := syntheticJWT(t, map[string]string{"sub": "u-1"})
	credential := buildInfoCredential(bearer)

	request, err := http.NewRequest(http.MethodGet, "http://query-api.test/buildinfo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := credential.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The header NAME, and the `Bearer ` prefix, exactly as /buildinfo
	// expects them. Asserting the whole value covers both at once.
	if got := request.Header.Get("Authorization"); got != "Bearer "+bearer {
		t.Fatalf("the /buildinfo read carried Authorization=%q, want %q", got, "Bearer "+bearer)
	}
	// ...and no OTHER header carries it, which is what a wrong header name
	// would look like.
	for name, values := range request.Header {
		if name == "Authorization" {
			continue
		}
		for _, value := range values {
			if strings.Contains(value, bearer) {
				t.Fatalf("the credential was installed as %s: /buildinfo reads Authorization and would see this request as unauthenticated", name)
			}
		}
	}

	// The kind reaches a 401 message so an operator learns WHICH credential
	// was refused, without its value.
	if credential.Kind() != "effective-principal envelope" {
		t.Fatalf("credential kind = %q; a 401 must name the kind /buildinfo actually checks", credential.Kind())
	}
	// Specifically NOT the edge access token: that is the wrong-credential
	// swap this pin exists for, and it 401s against a real stack.
	if strings.Contains(strings.ToLower(credential.Kind()), "access token") {
		t.Fatalf("credential kind = %q -- /buildinfo checks the envelope and 401s an edge access token", credential.Kind())
	}

	// Empty and whitespace-only are refused at use, whatever the caller's
	// own checks do -- the second floor under runCommand's explicit check.
	for _, bad := range []string{"", "   "} {
		if err := buildInfoCredential(bad).Apply(context.Background(), request); err == nil {
			t.Fatalf("buildInfoCredential(%q) was installed", bad)
		}
	}
}

// syntheticJWT builds a JWT-SHAPED value at RUNTIME, so no `eyJ...`
// literal appears anywhere in the tree. Gitleaks' `jwt` rule matches on
// SHAPE, not on whether the value is real, so a synthetic fixture written
// as a literal fails the secret scan exactly like a leaked one -- and the
// answer is to stop writing the shape into the source, not to teach the
// scanner to skip a file.
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

// CHAOS-5486: the four verbs. A first argument beginning with "-" is a
// FLAG, so it means the flat pre-verb form -- the form JOB 5's step list
// quotes verbatim -- and that form still means `repoint`. Breaking a
// recipe an operator is holding is not an acceptable cost for a nicer
// CLI.
func TestSplitVerbKeepsTheFlatPreVerbFormWorkingAsRepoint(t *testing.T) {
	for _, argv := range [][]string{
		{"-recorded-by", "lane", "-review-evidence", "why"},
		{"-dry-run"},
		{},
	} {
		verb, rest := splitVerb(argv)
		if verb != "repoint" {
			t.Fatalf("splitVerb(%v) = %q, want repoint -- #2415's invocation must keep working", argv, verb)
		}
		if len(rest) != len(argv) {
			t.Fatalf("splitVerb(%v) consumed an argument: rest=%v", argv, rest)
		}
	}
}

func TestSplitVerbTakesAnExplicitVerbOffTheFront(t *testing.T) {
	for _, want := range []string{"repoint", "enable", "disable", "status"} {
		verb, rest := splitVerb([]string{want, "-dry-run"})
		if verb != want {
			t.Fatalf("splitVerb = %q, want %q", verb, want)
		}
		if len(rest) != 1 || rest[0] != "-dry-run" {
			t.Fatalf("splitVerb left rest=%v, want the flags after the verb", rest)
		}
	}
}

// An unknown verb must NAME itself in the refusal and must not fall
// through to a write. "go-api-routing repint" quietly re-pointing every
// row is the shape of defect this whole surface exists to end.
func TestRunRefusesAnUnknownVerbByName(t *testing.T) {
	err := run([]string{"repint", "-dry-run"})
	if err == nil {
		t.Fatal("an unknown verb must be an error, never a fall-through to a write verb")
	}
	if !strings.Contains(err.Error(), "repint") {
		t.Fatalf("the refusal must name the verb it did not understand, got %q", err)
	}
}

// A refusal (a state the operator must resolve) exits 2; a crash exits 1.
// A calling script has to be able to tell them apart -- the Python CLI's
// `_refuse` sets the same convention.
// THE DEFAULT IS REFUSAL, and the test says so explicitly because the
// default was inverted in r2 (R2-01/R2-02).
//
// Classifying crash-by-default and requiring each site to opt IN to
// being a refusal produced THREE findings of one class across two rounds:
// an unknown verb, then several raw returns in `repoint`, then a
// malformed -postgres-uri and a non-sentinel /buildinfo failure. Every
// failure this command can produce is environmental; the internal-defect
// set is tiny and now opts OUT explicitly via errInternal.
func TestAnUnclassifiedErrorIsARefusalAndOnlyErrInternalCrashes(t *testing.T) {
	if got := exitCodeFor(nil); got != 0 {
		t.Fatalf("exitCodeFor(nil) = %d, want 0", got)
	}
	if got := exitCodeFor(refuse("nope")); got != 2 {
		t.Fatalf("exitCodeFor(refusal) = %d, want 2", got)
	}
	if got := exitCodeFor(fmt.Errorf("wrapped: %w", refuse("nope"))); got != 2 {
		t.Fatalf("a WRAPPED refusal is still a refusal, got %d", got)
	}
	// The inversion itself: an error nobody classified is an operator
	// state, not a crash. Forgetting to mark one now costs a script a
	// retryable exit 2; under the old default it cost a spurious alert,
	// three times.
	if got := exitCodeFor(errors.New("something nobody classified")); got != 2 {
		t.Fatalf("an UNCLASSIFIED error must default to a refusal (2), got %d", got)
	}
	if got := exitCodeFor(internal("a defect in this program")); got != 1 {
		t.Fatalf("exitCodeFor(internal) = %d, want 1", got)
	}
	if got := exitCodeFor(fmt.Errorf("wrapped: %w", internal("boom"))); got != 1 {
		t.Fatalf("a WRAPPED internal error is still internal, got %d", got)
	}
}

// Preflight 2's local half. Rows are keyed by this value, so the digest
// this binary compares against the running process must be the one the
// repo pins -- not a value it computed over some other bytes.
//
// The pin file is read at test time rather than hardcoded, so an SDL
// change that legitimately moves the digest updates one place
// (contracts/graphql/v1/schema-digest.json, itself gated by
// ci/check_go_api_routing_digest.py) instead of two.
func TestLocalSchemaDigestMatchesTheRepositoryPin(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "graphql", "v1", "schema-digest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pin struct {
		SchemaDigest string `json:"schema_digest"`
	}
	if err := json.Unmarshal(raw, &pin); err != nil {
		t.Fatal(err)
	}
	if pin.SchemaDigest == "" {
		t.Fatal("contracts/graphql/v1/schema-digest.json carries no schema_digest")
	}
	if got := localSchemaDigest(); got != pin.SchemaDigest {
		t.Fatalf("localSchemaDigest() = %s, the repository pins %s -- enable would write rows at a digest neither plane computes", got, pin.SchemaDigest)
	}
}

// The refusal an operator reads has to say what to do next. Every digest
// refusal names the runbook section for the same reason the Python verb's
// does: the message that stops the command is the one they act on.
func TestRunbookNamesTheDigestMoveSection(t *testing.T) {
	if !strings.Contains(runbook, "go-api-wave-0-proof-infrastructure.md") ||
		!strings.Contains(runbook, "When the schema digest moves") {
		t.Fatalf("runbook = %q; a digest refusal must point at the recovery procedure", runbook)
	}
}

// r1 F2: an unknown verb is a REFUSAL, not a crash. A calling script has
// to be able to tell "I would not do that" (2) from "I broke" (1), and
// the split existed but this path did not go through it.
func TestAnUnknownVerbRefusesWithExitTwo(t *testing.T) {
	err := run([]string{"definitely-not-a-verb"})
	if err == nil {
		t.Fatal("an unknown verb must be an error")
	}
	if got := exitCodeFor(err); got != 2 {
		t.Fatalf("exit code = %d, want 2 -- an unknown verb is a refusal, and the Python CLI sets that convention", got)
	}
	if !strings.Contains(err.Error(), "definitely-not-a-verb") {
		t.Fatalf("the refusal must name the verb it did not understand, got %q", err)
	}
}

// r1 F8: provenance is normalised ONCE, before it is stored. Python
// strips before persisting; a whitespace-padded identity makes two
// records of the same operator compare unequal for a reason nobody can
// see, and the CHAOS-5505 audit table is append-only.
func TestProvenanceIsTrimmedBeforeItIsStored(t *testing.T) {
	flags := commonFlags{recordedBy: "  lane-routing-verbs \t", reviewEvidence: "\n why  "}
	if err := flags.requireProvenance(); err != nil {
		t.Fatalf("requireProvenance: %v", err)
	}
	if flags.recordedBy != "lane-routing-verbs" || flags.reviewEvidence != "why" {
		t.Fatalf("stored recorded_by=%q review_evidence=%q, want both trimmed", flags.recordedBy, flags.reviewEvidence)
	}
	// Whitespace-only is still absent, and the refusal names BOTH flags
	// rather than stopping at the first.
	blank := commonFlags{recordedBy: "   ", reviewEvidence: "\t"}
	err := blank.requireProvenance()
	if err == nil {
		t.Fatal("whitespace-only provenance must be refused")
	}
	if !strings.Contains(err.Error(), "-recorded-by") || !strings.Contains(err.Error(), "-review-evidence") {
		t.Fatalf("the refusal must name every missing flag, got %q", err)
	}
	if got := exitCodeFor(err); got != 2 {
		t.Fatalf("a missing-flag refusal must exit 2, got %d", got)
	}
}

// The third declared tightening (r1 F3). Python permits an absent reason
// for a proven enablement and derives recorded_by from the environment,
// falling back to the literal "unknown". This command refuses instead,
// and that choice is pinned so it cannot drift back by accident or be
// mistaken for an oversight.
func TestEveryWriteVerbRequiresBothProvenanceFlags(t *testing.T) {
	if err := (&commonFlags{recordedBy: "who"}).requireProvenance(); err == nil {
		t.Fatal("-review-evidence is required even when -recorded-by is present")
	}
	if err := (&commonFlags{reviewEvidence: "why"}).requireProvenance(); err == nil {
		t.Fatal("-recorded-by is required even when -review-evidence is present")
	}
	if err := (&commonFlags{recordedBy: "who", reviewEvidence: "why"}).requireProvenance(); err != nil {
		t.Fatalf("both present must pass: %v", err)
	}
}

// r1 F7: every verb's own entry point was at 0% coverage, so the
// operator-facing refusals -- the code path an operator hits most --
// were entirely unexercised. These drive each verb far enough to reach
// its first semantic refusal without needing a network or a database.
//
// Flag PARSING is deliberately not exercised: the flag sets use
// flag.ExitOnError, so a malformed flag calls os.Exit and would take the
// test binary with it. Every case below parses cleanly and then refuses.
func TestEveryVerbRefusesItsOwnMissingPreconditions(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	// POSTGRES_URI is a SUPPORTED fallback for -postgres-uri, so a
	// developer or a CI runner that has it exported is an ordinary,
	// correct environment -- and this test's "with no postgres" cases
	// silently stopped testing anything there, because the fallback
	// supplied one (Trap #101; r1 measured it failing on a host that had
	// it set). A test whose subject is "this flag is ABSENT" has to make
	// it absent rather than assume the machine did.
	t.Setenv("POSTGRES_URI", "")
	for name, testCase := range map[string]struct {
		argv []string
		want string
	}{
		"enable with no -mode":            {[]string{"enable"}, "-mode is required"},
		"enable with no provenance":       {[]string{"enable", "-mode", "canary"}, "-recorded-by"},
		"enable with no postgres":         {[]string{"enable", "-mode", "canary", "-recorded-by", "w", "-review-evidence", "y"}, "-postgres-uri"},
		"enable with no credential":       {[]string{"enable", "-mode", "canary", "-recorded-by", "w", "-review-evidence", "y", "-postgres-uri", "postgres://x"}, bearerEnvVar},
		"disable with no -mode":           {[]string{"disable"}, "-mode is required"},
		"disable with no postgres":        {[]string{"disable", "-mode", "python"}, "-postgres-uri"},
		"disable applying with no reason": {[]string{"disable", "-mode", "python", "-postgres-uri", "postgres://x", "-apply"}, "-recorded-by"},
		"repoint with no provenance":      {[]string{"repoint"}, "-recorded-by"},
		"repoint with no postgres":        {[]string{"repoint", "-recorded-by", "w", "-review-evidence", "y"}, "-postgres-uri"},
		"repoint with no credential":      {[]string{"repoint", "-recorded-by", "w", "-review-evidence", "y", "-postgres-uri", "postgres://x"}, bearerEnvVar},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(testCase.argv)
			if err == nil {
				t.Fatalf("run(%v) = nil, want a refusal", testCase.argv)
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("run(%v) = %q, want it to name %q", testCase.argv, err, testCase.want)
			}
			if got := exitCodeFor(err); got != 2 {
				t.Fatalf("run(%v) exits %d, want 2 -- every one of these is a state the operator resolves", testCase.argv, got)
			}
		})
	}
}

// `status` NEVER refuses, even with nothing configured at all. It is what
// an operator runs when things are already broken, and a diagnostic that
// dies because the thing it diagnoses is down is useless exactly when it
// is needed. Driven here with no database, no credential, and a registry
// URL nothing is listening on.
func TestStatusNeverFailsEvenWithNothingConfigured(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	t.Setenv("POSTGRES_URI", "")
	for _, argv := range [][]string{
		{"status", "-registry-url", "http://127.0.0.1:1", "-timeout", "2s"},
		{"status", "-registry-url", "http://127.0.0.1:1", "-timeout", "2s", "-json"},
		{"status", "-registry-url", "http://127.0.0.1:1", "-timeout", "2s", "-catalog", "/nonexistent/catalog.json"},
	} {
		if err := run(argv); err != nil {
			t.Fatalf("run(%v) = %v, want nil -- status never fails on an unhealthy state", argv, err)
		}
	}
}

// r2 R2-01 and R2-10 together: `connectPostgres` was both unclassified
// (a malformed DSN read as a crash) and uncovered.
//
// The leak assertion is the important half. pgx's own parse error EMBEDS
// the DSN it was given, and a real DSN carries a password -- so the first
// version of the R2-01 fix wrapped that error with %w and would have put
// a password in an operator's terminal and shell history. Caught while
// verifying the fix, which is why the test exists rather than the comment.
func TestConnectPostgresRefusesAndNeverEchoesTheDSN(t *testing.T) {
	const password = "SUPERSECRET-NEVER-PRINT"
	malformed := "postgres://u:" + password + "@ =not a dsn"

	_, err := connectPostgres(t.Context(), malformed, time.Second)
	if err == nil {
		t.Fatal("a malformed DSN must be refused")
	}
	if got := exitCodeFor(err); got != 2 {
		t.Fatalf("a malformed DSN exits %d, want 2 -- it is the operator's to fix", got)
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("the refusal echoed the DSN's password: %q", err)
	}
	if !strings.Contains(err.Error(), "-postgres-uri") {
		t.Fatalf("the refusal must name the flag the operator has to change, got %q", err)
	}
}

// The other half of connectPostgres: a well-formed DSN nothing answers.
// 192.0.2.1 is TEST-NET-1 (RFC 5737) and is blackholed by definition, so
// this exercises the dial timeout without depending on the host's network
// behaving in any particular way.
func TestConnectPostgresBoundsTheDialAndSaysSo(t *testing.T) {
	const password = "SUPERSECRET-NEVER-PRINT"
	started := time.Now()
	_, err := connectPostgres(t.Context(),
		"postgres://u:"+password+"@192.0.2.1:5432/db", 2*time.Second)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("a blackholed address must not connect")
	}
	if elapsed > 15*time.Second {
		t.Fatalf("the dial took %s -- the -timeout flag must bound it, or `status` hangs forever on a dead database", elapsed)
	}
	if got := exitCodeFor(err); got != 2 {
		t.Fatalf("an unreachable database exits %d, want 2", got)
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("the refusal echoed the DSN's password: %q", err)
	}
}

// codex r3 SEC-01, reproduced with a real password before the fix.
//
// goapiproof.FetchRegistry and FetchBuildIdentity interpolate the URL
// they are handed straight into their error text, so
// `-registry-url http://alice:supersecret@host/registry` printed
// `supersecret` in full on any transport failure. Go's own url.Error
// masks the password in the NESTED error, which is exactly why it was
// easy to miss: the leak came from the OUTER interpolation, sitting next
// to a string that looked already sanitised.
//
// The interpolation is in a file this lane may not edit, so the fix is at
// the boundary this command owns: such a URL never reaches that code.
func TestCredentialBearingURLsAreRefusedBeforeTheyCanBePrinted(t *testing.T) {
	const secret = "supersecret-never-print"
	for _, raw := range []string{
		"http://alice:" + secret + "@127.0.0.1:1/registry",
		"https://alice:" + secret + "@example.invalid/buildinfo",
		// Userinfo with no password still identifies a principal, and
		// still gets echoed.
		"http://alice@127.0.0.1:1/registry",
	} {
		_, err := sanitizeEndpointURL("-registry-url", raw)
		if !errors.Is(err, ErrURLCarriesCredentials) {
			t.Fatalf("requireCredentialFreeURL(%q) = %v, want ErrURLCarriesCredentials", raw, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the refusal itself echoed the credential: %q", err)
		}
		if got := exitCodeFor(err); got != 2 {
			t.Fatalf("a credential-bearing URL exits %d, want 2", got)
		}
	}

	// The shapes that must still be accepted.
	for _, raw := range []string{
		"http://localhost:8090/registry",
		"https://query-api.internal/buildinfo",
		"http://172.19.0.12:8090/registry",
	} {
		if _, err := sanitizeEndpointURL("-registry-url", raw); err != nil {
			t.Fatalf("requireCredentialFreeURL(%q) = %v, want nil", raw, err)
		}
	}
}

// The other half: anything unparseable, or not http(s), is refused
// WITHOUT its text being echoed -- an unparseable string is exactly the
// one whose shape cannot be reasoned about, which is the lesson
// go_api_cli.py records after five rounds of trying to redact URLs.
func TestUnparseableAndNonHTTPURLsAreRefusedWithoutEchoingThem(t *testing.T) {
	const secret = "supersecret-never-print"
	for name, raw := range map[string]string{
		"not a url":     "://" + secret,
		"file scheme":   "file:///etc/passwd",
		"ftp scheme":    "ftp://host/" + secret,
		"no host":       "http:///registry",
		"bare hostname": "query-api:8090/registry",
	} {
		_, err := sanitizeEndpointURL("-registry-url", raw)
		if err == nil {
			t.Fatalf("%s must be refused", name)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: the refusal echoed the input: %q", name, err)
		}
		if got := exitCodeFor(err); got != 2 {
			t.Fatalf("%s exits %d, want 2", name, got)
		}
	}
}

// THE NO-`//` FORM, measured rather than assumed (lane-5425-prove found it
// in its own staged fix; confirmed here with the same input).
//
//	url.Parse("alice:supersecret@host/registry")
//	  scheme="alice"  opaque="supersecret@host/registry"  User=nil
//	  Redacted() => "alice:supersecret@host/registry"
//
// Two consequences, and this test pins both. `u.User` is NIL, so a check
// that relies on it alone lets the string through -- which is why the
// first gate here looks for `@` before the first `/` and does not consult
// the parser at all. And the SCHEME is the username, so a refusal that
// names the offending scheme prints a credential: the first version of
// this code refused correctly and leaked `alice` while doing it.
//
// `url.Redacted()` is the obvious Go answer and is wrong for the same
// reason: with User nil it has nothing to redact and hands the password
// straight back.
func TestTheNoSlashSlashFormIsRefusedAndNothingAboutItIsEchoed(t *testing.T) {
	const user = "alice-the-operator"
	const password = "supersecret-never-print"
	for name, raw := range map[string]string{
		"no scheme separator":     user + ":" + password + "@host/registry",
		"no separator, no colon":  user + "@host/registry",
		"scheme-relative":         "//" + user + ":" + password + "@host/registry",
		"scheme-relative no pass": "//" + user + "@host/registry",
	} {
		_, err := sanitizeEndpointURL("-registry-url", raw)
		if err == nil {
			t.Fatalf("%s must be refused: %q", name, raw)
		}
		// Neither half of the credential, and not the username on its own
		// -- a username is operator-supplied credential material too.
		if strings.Contains(err.Error(), password) {
			t.Fatalf("%s: the refusal echoed the PASSWORD: %q", name, err)
		}
		if strings.Contains(err.Error(), user) {
			t.Fatalf("%s: the refusal echoed the USERNAME: %q", name, err)
		}
		if got := exitCodeFor(err); got != 2 {
			t.Fatalf("%s exits %d, want 2", name, got)
		}
	}
}

// The parser-independent gate, on its own. It runs BEFORE url.Parse
// precisely because the parser is the thing that surprised us: an `@`
// ahead of the first `/` is userinfo in every URL shape, whatever Go
// makes of it.
func TestUserinfoIsCaughtWithoutConsultingTheParser(t *testing.T) {
	const secret = "supersecret-never-print"
	for _, raw := range []string{
		"http://u:" + secret + "@host/registry",
		"u:" + secret + "@host/registry",
		"//u:" + secret + "@host/registry",
		// An `@` AFTER the first slash is a path character, not userinfo,
		// and must not be refused on that basis.
	} {
		if _, err := sanitizeEndpointURL("-registry-url", raw); err == nil {
			t.Fatalf("%q must be refused", raw)
		}
	}
	if _, err := sanitizeEndpointURL("-registry-url", "http://host/registry@v2"); err != nil {
		t.Fatalf("an @ in the PATH is not userinfo and must be accepted, got %v", err)
	}
}

// r4 CRED-01, reproduced verbatim from the round: a query string carrying
// a token passed a userinfo-only check and reached the interpolation.
//
// THIS IS THE THIRD VARIANT OF ONE LEAK -- userinfo (r3), the no-`//`
// scheme echo, and now query/fragment. Each earlier fix rejected the
// shape that had just been found and left the next one open, which is why
// the check is now an ALLOWLIST over URL components and returns a REBUILT
// URL rather than the operator's string.
func TestQueryAndFragmentCredentialsAreRefused(t *testing.T) {
	const secret = "supersecret-never-print"
	for name, raw := range map[string]string{
		"query token":        "http://127.0.0.1:1/registry?token=" + secret,
		"query, no value":    "http://127.0.0.1:1/registry?" + secret,
		"forced empty query": "http://127.0.0.1:1/registry?",
		"fragment":           "http://127.0.0.1:1/registry#" + secret,
		"query and fragment": "http://127.0.0.1:1/registry?token=" + secret + "#x",
	} {
		got, err := sanitizeEndpointURL("-registry-url", raw)
		if err == nil {
			t.Fatalf("%s must be refused: %q rebuilt to %q", name, raw, got)
		}
		if !errors.Is(err, ErrURLCarriesCredentials) {
			t.Fatalf("%s = %v, want ErrURLCarriesCredentials", name, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: the refusal echoed the secret: %q", name, err)
		}
		if got != "" {
			t.Fatalf("%s returned %q alongside its error", name, got)
		}
	}
}

// The REBUILD is the belt to the allowlist's braces: whatever this
// function did not explicitly account for cannot survive into the value
// that reaches the code which interpolates it. If a future URL component
// carries something, it is dropped rather than forwarded.
func TestTheSanitizedURLIsRebuiltFromComponentsNotForwarded(t *testing.T) {
	// A path with characters that need escaping still round-trips to a
	// usable URL -- the rebuild must not corrupt a legitimate endpoint.
	got, err := sanitizeEndpointURL("-registry-url", "http://host:8090/api%2Fv1/registry")
	if err != nil {
		t.Fatalf("sanitizeEndpointURL: %v", err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("the rebuilt URL does not parse: %q: %v", got, err)
	}
	if parsed.Scheme != "http" || parsed.Host != "host:8090" {
		t.Fatalf("rebuild lost the endpoint: %q", got)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		t.Fatalf("the rebuilt URL carries a component it should have dropped: %q", got)
	}
}

// r4 TEST-01: `toReportOperation` was the one status path with no direct
// coverage, and it is the projection an operator actually reads.
func TestToReportOperationProjectsEveryStateFaithfully(t *testing.T) {
	rollout := 100
	updated := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	match := goapiproof.OperationStatus{
		Operation:             "featureFlags",
		DocumentDigest:        "abc",
		DigestState:           goapiproof.DigestMatch,
		Mode:                  "canary",
		CurrentCandidateBuild: "build-1",
		RolloutPercentage:     &rollout,
		Owner:                 "go",
		UpdatedAt:             &updated,
		ReviewEvidence:        "why",
		RecordedBy:            "who",
		Proven:                true,
	}
	got := toReportOperation(match, map[string]string{"featureFlags": "abc"}, false)
	if got.Mode == nil || *got.Mode != "canary" || got.CurrentCandidateBuild == nil || *got.CurrentCandidateBuild != "build-1" {
		t.Fatalf("MATCH projection lost the row: %+v", got)
	}
	if got.UpdatedAt == nil || *got.UpdatedAt != "2026-09-09T12:00:00Z" {
		t.Fatalf("updated_at = %v, want RFC3339 UTC", got.UpdatedAt)
	}
	if !got.Reachable || !got.Proven {
		t.Fatalf("a proven canary row at the live digest is reachable and proven: %+v", got)
	}
	if got.ReviewEvidence == nil || got.RecordedBy == nil {
		t.Fatal("the provenance an operator needs to read must survive the projection")
	}

	// A STALE row must project NO row fields at all -- reporting a mode
	// for a row nothing can reach is the CHAOS-5416 lie in a new column.
	stale := goapiproof.OperationStatus{
		Operation:                  "hotspots",
		DocumentDigest:             "def",
		DigestState:                goapiproof.DigestStale,
		StaleDigests:               []string{"sha256:old"},
		UnreachableDocumentDigests: []string{"ghi"},
	}
	got = toReportOperation(stale, nil, false)
	if got.Mode != nil || got.CurrentCandidateBuild != nil || got.RolloutPercentage != nil || got.UpdatedAt != nil {
		t.Fatalf("a STALE row must project no row fields: %+v", got)
	}
	if got.Reachable {
		t.Fatal("a STALE row is never reachable")
	}
	if len(got.StaleDigests) != 1 || len(got.UnreachableDocumentDigests) != 1 {
		t.Fatalf("both kinds of dead row must be named: %+v", got)
	}

	// MISSING projects empty slices, never nil -- a JSON reader must not
	// have to tell `null` from `[]` to answer "are there other rows".
	missing := toReportOperation(goapiproof.OperationStatus{
		Operation: "flowMatrix", DocumentDigest: "jkl", DigestState: goapiproof.DigestMissing,
	}, nil, false)
	if missing.StaleDigests == nil || missing.UnreachableDocumentDigests == nil {
		t.Fatalf("empty digest lists must render as [] not null: %+v", missing)
	}
}

// r4 P1 (reproduced): `status` used to discard the deployed registry's
// per-operation document digest after checking only schema_digest, so a
// MATCH row printed "ok"/reachable=true even when the deployed plane
// registered a DIFFERENT document digest for that exact operation -- the
// same disagreement `enable`'s preflight refuses on. Each case below is
// the projection `enable` would agree or disagree with.
func TestToReportOperationSurfacesDeployedDigestDisagreement(t *testing.T) {
	base := goapiproof.OperationStatus{
		Operation:      "flowMatrix",
		DocumentDigest: "catalog-digest",
		DigestState:    goapiproof.DigestMatch,
		Mode:           "canary",
		Proven:         true,
	}

	agree := toReportOperation(base, map[string]string{"flowMatrix": "catalog-digest"}, false)
	if agree.DeployedDigestState != "AGREE" {
		t.Fatalf("deployed_digest_state = %q, want AGREE", agree.DeployedDigestState)
	}
	if !agree.Reachable {
		t.Fatal("a row the deployed plane agrees with must stay reachable")
	}

	mismatch := toReportOperation(base, map[string]string{"flowMatrix": "deployed-digest"}, false)
	if mismatch.DeployedDigestState != "MISMATCH" {
		t.Fatalf("deployed_digest_state = %q, want MISMATCH", mismatch.DeployedDigestState)
	}
	if mismatch.DeployedDocumentDigest == nil || *mismatch.DeployedDocumentDigest != "deployed-digest" {
		t.Fatalf("deployed_document_digest = %v, want the deployed value named", mismatch.DeployedDocumentDigest)
	}
	if mismatch.Reachable {
		t.Fatal("a MATCH row the deployed plane disagrees with must NOT be reported reachable -- enable would refuse it")
	}

	unregistered := toReportOperation(base, map[string]string{"otherOperation": "x"}, false)
	if unregistered.DeployedDigestState != "UNREGISTERED" {
		t.Fatalf("deployed_digest_state = %q, want UNREGISTERED", unregistered.DeployedDigestState)
	}
	if unregistered.Reachable {
		t.Fatal("an operation the deployed plane does not register at all must not be reported reachable")
	}

	unreachableGoPlane := toReportOperation(base, nil, true)
	if unreachableGoPlane.DeployedDigestState != "UNKNOWN" {
		t.Fatalf("deployed_digest_state = %q, want UNKNOWN when the go plane could not be reached", unreachableGoPlane.DeployedDigestState)
	}
	if !unreachableGoPlane.Reachable {
		t.Fatal("an UNKNOWN deployed digest state (go plane unreachable) must not downgrade a row that was otherwise reachable -- that is what GoPlaneError already reports")
	}
}

// The exotic shapes, enumerated because three rounds showed that guessing
// which ones matter is how this class survives. Each is a way a URL can
// carry a secret past a naive check; none may reach the code that
// interpolates the URL into an error.
func TestNoExoticURLShapeSurvivesTheRebuildWithASecretIntact(t *testing.T) {
	const secret = "supersecret-never-print"
	for name, raw := range map[string]string{
		"percent-encoded userinfo":   "http://%61lice:%73upersecret@host/registry",
		"userinfo with encoded at":   "http://alice:" + secret + "%40host/registry",
		"IPv6 literal with userinfo": "http://alice:" + secret + "@[::1]:8090/registry",
		"at inside an IPv6 literal":  "http://[::1@" + secret + "]:8090/registry",
		"backslash separators":       `http:\\alice:` + secret + `@host\registry`,
		"backslash before at":        `http://host\@` + secret + `/registry`,
		"uppercase scheme":           "HTTP://ALICE:" + secret + "@HOST/registry",
		"embedded newline":           "http://host/registry\n?token=" + secret,
		"embedded carriage return":   "http://host/registry\r?token=" + secret,
		"embedded tab":               "http://host/registry\ttoken=" + secret,
		"embedded null":              "http://host/registry\x00" + secret,
		"leading whitespace":         "   http://alice:" + secret + "@host/registry",
		"query after fragment":       "http://host/registry#x?token=" + secret,
		"empty":                      "",
	} {
		got, err := sanitizeEndpointURL("-registry-url", raw)
		if err == nil {
			t.Fatalf("%s: %q was ACCEPTED and rebuilt to %q", name, raw, got)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: the refusal echoed the secret: %q", name, err)
		}
		if got != "" {
			t.Fatalf("%s returned %q alongside its error", name, got)
		}
	}

	// Legitimate shapes that must NOT be caught by any of the above --
	// an over-broad gate is its own outage.
	for name, raw := range map[string]string{
		"IPv6 literal":     "http://[::1]:8090/registry",
		"IDNA punycode":    "http://xn--n28h.example/registry",
		"at in the path":   "http://host/registry@v2",
		"deep path prefix": "http://host/api/v1/registry",
		"https default":    "https://query-api.internal/buildinfo",
	} {
		if _, err := sanitizeEndpointURL("-registry-url", raw); err != nil {
			t.Fatalf("%s (%q) must be accepted, got %v", name, raw, err)
		}
	}
}

// THE RESIDUAL, asserted so it is explicit rather than implied.
//
// A credential in a PATH SEGMENT is not distinguishable from the route
// itself -- the path is the endpoint's address and must be sent to reach
// it. It is documented in the runbook. This test exists so the residual
// cannot be silently "fixed" by an over-broad gate that would start
// rejecting a legitimate proxy prefix, and so a reader can see the
// boundary of what the allowlist claims.
//
// It closes for real when internal/goapiproof stops interpolating raw
// URLs into its errors, which is owned by the lane that owns that file.
func TestAPathSegmentSecretIsAcceptedAndThatIsTheKnownResidual(t *testing.T) {
	const raw = "http://host:8090/registry/a-secret-in-the-path"
	got, err := sanitizeEndpointURL("-registry-url", raw)
	if err != nil {
		t.Fatalf("a path segment cannot be told from a route, so it must be accepted: %v", err)
	}
	if got != raw {
		t.Fatalf("rebuild changed a legitimate endpoint: %q -> %q", raw, got)
	}
}

// The confirmation pass's finding, and the reason the first rebuild was
// not enough.
//
// `http://[fe80::1%25zone-secret]:8090/registry` passes EVERY component
// check -- scheme http, no userinfo, no query, no fragment -- and the
// first rebuild copied `parsed.Host` across verbatim, so the zone text
// reached the error that interpolates the URL.
//
// THE LESSON: that rebuild reconstructed the STRUCTURE of the URL and
// copied each component's CONTENTS. A component you copy is a component
// you have not validated. The host is now taken apart and put back
// together from a hostname that must be an IP literal or a DNS name and a
// port that must be digits.
func TestAnIPv6ZoneIdentifierCannotSurviveTheRebuild(t *testing.T) {
	const secret = "zone-supersecret-probe"
	for name, raw := range map[string]string{
		"encoded zone":          "http://[fe80::1%25" + secret + "]:8090/registry",
		"encoded zone, no port": "http://[fe80::1%25" + secret + "]/registry",
		"percent in hostname":   "http://host%25" + secret + "/registry",
	} {
		got, err := sanitizeEndpointURL("-registry-url", raw)
		if err == nil {
			t.Fatalf("%s: %q was ACCEPTED and rebuilt to %q", name, raw, got)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: the refusal echoed the zone text: %q", name, err)
		}
		if got != "" {
			t.Fatalf("%s returned %q alongside its error", name, got)
		}
	}
}

// The host is rebuilt from validated pieces, so anything that is neither
// an IP literal nor a DNS name is refused rather than carried. And a
// non-numeric port cannot ride along either.
func TestTheHostIsRebuiltFromValidatedPiecesNotCopied(t *testing.T) {
	const secret = "supersecret-never-print"
	for name, raw := range map[string]string{
		"underscore in host": "http://ho_st" + secret + "/registry",
		"space in host":      "http://ho st/registry",
		"quote in host":      `http://ho"st/registry`,
	} {
		if _, err := sanitizeEndpointURL("-registry-url", raw); err == nil {
			t.Fatalf("%s (%q) must be refused", name, raw)
		} else if strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: refusal echoed the input: %q", name, err)
		}
	}

	// Legitimate hosts survive, and IPv6 comes back correctly bracketed.
	for raw, want := range map[string]string{
		"http://[::1]:8090/registry":              "http://[::1]:8090/registry",
		"http://[2001:db8::1]/registry":           "http://[2001:db8::1]/registry",
		"http://127.0.0.1:8090/registry":          "http://127.0.0.1:8090/registry",
		"http://query-api.internal:8090/registry": "http://query-api.internal:8090/registry",
		"http://xn--n28h.example/registry":        "http://xn--n28h.example/registry",
		"https://host/buildinfo":                  "https://host/buildinfo",
	} {
		got, err := sanitizeEndpointURL("-registry-url", raw)
		if err != nil {
			t.Fatalf("%q must be accepted, got %v", raw, err)
		}
		if got != want {
			t.Fatalf("%q rebuilt to %q, want %q -- a legitimate endpoint must survive the rebuild intact", raw, got, want)
		}
	}
}

// The host-rebuild attack list, probed before handing it to a reviewer.
// None of these leaks -- they are pinned so the reassembly's behaviour on
// each is a decision on the record rather than whatever it happened to do.
func TestHostReassemblyBehavesDeliberatelyOnEveryOddShape(t *testing.T) {
	for raw, want := range map[string]string{
		// Accepted and normalised. An IPv4-mapped IPv6 literal comes back
		// as dotted-quad: same address, one spelling.
		"http://[::ffff:127.0.0.1]:8090/registry": "http://127.0.0.1:8090/registry",
		"http://[::ffff:7f00:1]/registry":         "http://127.0.0.1/registry",
		// An empty port is dropped rather than carried.
		"http://host:/registry": "http://host/registry",
		// A trailing dot is a root-anchored DNS name and is legitimate.
		"http://host./registry": "http://host./registry",
		// Case is preserved: DNS is case-insensitive, and rewriting it
		// would make the URL used differ from the one typed for no gain.
		"http://HOST.EXAMPLE/registry": "http://HOST.EXAMPLE/registry",
	} {
		got, err := sanitizeEndpointURL("-registry-url", raw)
		if err != nil {
			t.Fatalf("%q must be accepted, got %v", raw, err)
		}
		if got != want {
			t.Fatalf("%q rebuilt to %q, want %q", raw, got, want)
		}
	}

	for name, raw := range map[string]string{
		// Non-ASCII is refused, not punycoded -- Go's HTTP client does no
		// IDNA encoding, so it could not be dialled anyway, and refusing
		// means a homoglyph cannot carry text into an error.
		"cyrillic homoglyph": "http://examрle.com/registry",
		"latin-1 umlaut":     "http://exämple.com/registry",
		// A percent-escape in the host cannot survive the rebuild.
		"percent-escaped dot":  "http://host%2ename/registry",
		"percent-escaped char": "http://ho%73t/registry",
		// A port that cannot be dialled is refused where the failure has
		// context, not deferred to the dial.
		"port above 65535": "http://host:99999/registry",
		"port zero":        "http://host:0/registry",
	} {
		if _, err := sanitizeEndpointURL("-registry-url", raw); err == nil {
			t.Fatalf("%s (%q) must be refused", name, raw)
		}
	}

	// Punycode is the supported way to reach an IDN host, and must work.
	if got, err := sanitizeEndpointURL("-registry-url", "http://xn--n28h.example/registry"); err != nil || got != "http://xn--n28h.example/registry" {
		t.Fatalf("punycode = (%q, %v), want it accepted unchanged", got, err)
	}
}

// The usage text must never print a credential, on ANY verb.
//
// Found by RUNNING the binary, not by reading it: `flag` prints each
// flag's DEFAULT VALUE in its usage dump, so registering -postgres-uri
// with `os.Getenv("POSTGRES_URI")` as its default made the default the
// DSN itself. A mistyped flag, a missing value or a plain `-h` then wrote
// the database password to stderr in full. Measured against a real
// database before the fix:
//
//	-postgres-uri string
//	  domain Postgres DSN holding go_api_routing_state (default
//	  "postgresql://postgres:<password>@127.0.0.1:55437/devhealth")
//
// The check is on the RENDERED usage text of each verb's real flag set,
// with POSTGRES_URI set to a value carrying a recognisable secret, so a
// regression on any one verb fails here rather than in someone's CI log.
func TestNoVerbsUsageTextEverPrintsTheDSN(t *testing.T) {
	const secret = "hunter2-not-a-real-password"
	t.Setenv("POSTGRES_URI", "postgresql://postgres:"+secret+"@db.internal:5432/devhealth")

	for _, verb := range []string{"enable", "disable", "repoint", "status"} {
		t.Run(verb, func(t *testing.T) {
			// -h makes flag print usage and, under ContinueOnError, return
			// ErrHelp instead of exiting the test binary.
			var rendered strings.Builder
			set := flag.NewFlagSet(verb, flag.ContinueOnError)
			set.SetOutput(&rendered)
			var common commonFlags
			common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
			set.PrintDefaults()

			usage := rendered.String()
			if strings.Contains(usage, secret) {
				t.Fatalf("%s usage text prints the DSN's password:\n%s", verb, usage)
			}
			if strings.Contains(usage, "db.internal") {
				t.Fatalf("%s usage text prints the DSN's host, so it is printing the DSN:\n%s", verb, usage)
			}
			// ...and it still tells the operator where the value comes
			// from. Silence would trade a leak for a usability defect.
			if !strings.Contains(usage, postgresURIEnvVar) {
				t.Fatalf("%s usage text no longer names %s, so an operator cannot tell where the DSN comes from:\n%s", verb, postgresURIEnvVar, usage)
			}
		})
	}
}

// The environment fallback still WORKS, which is the half a leak fix is
// most likely to break: moving the fallback out of the flag's default is
// only correct if something else applies it.
func TestThePostgresURIStillFallsBackToTheEnvironment(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgresql://postgres:pw@db.internal:5432/devhealth")

	// No flag: the environment supplies it.
	var fromEnv commonFlags
	if err := fromEnv.requirePostgres(); err != nil {
		t.Fatalf("requirePostgres with POSTGRES_URI set: %v", err)
	}
	if fromEnv.postgresURI != "postgresql://postgres:pw@db.internal:5432/devhealth" {
		t.Fatalf("postgresURI = %q, want the environment's value", fromEnv.postgresURI)
	}

	// The FLAG wins over the environment, same precedence as before.
	explicit := commonFlags{postgresURI: "postgresql://postgres:pw@flag.internal:5432/devhealth"}
	if err := explicit.requirePostgres(); err != nil {
		t.Fatalf("requirePostgres with an explicit flag: %v", err)
	}
	if !strings.Contains(explicit.postgresURI, "flag.internal") {
		t.Fatalf("postgresURI = %q, want the flag to win over the environment", explicit.postgresURI)
	}

	// Whitespace-only is not a value, in EITHER source.
	t.Setenv("POSTGRES_URI", "   ")
	var blank commonFlags
	blank.postgresURI = "  "
	if err := blank.requirePostgres(); err == nil {
		t.Fatal("a whitespace-only flag and a whitespace-only environment variable were accepted as a DSN")
	}
}

// A -timeout that disables its own bound is refused on EVERY verb.
//
// Found by executing the input-domain table against the real binary, not
// by reading the code: `-timeout 0` and `-timeout -1s` were accepted with
// exit 0 on all four verbs. `http.Client` reads a non-positive Timeout as
// "no timeout", which is the exact opposite of what an operator typing 0
// means, and it silently undoes r1 F10 -- the fix that made `status` say
// "unreachable" instead of hanging on a blackholed endpoint.
//
// The boundary is walked on both sides rather than sampled: 0 and -1ns
// refuse, 1ns and the default accept. A guard pinned only at 0 survives a
// mutation to `< 0`.
func TestEveryVerbRefusesATimeoutThatDisablesItsOwnBound(t *testing.T) {
	refused := []time.Duration{0, -1, -time.Nanosecond, -time.Second, -time.Hour}
	accepted := []time.Duration{time.Nanosecond, time.Millisecond, 30 * time.Second}

	for _, timeout := range refused {
		common := commonFlags{timeout: timeout}
		err := common.requirePositiveTimeout()
		if err == nil {
			t.Fatalf("-timeout %s was accepted: a non-positive value means the HTTP leg never times out", timeout)
		}
		if exitCodeFor(err) != 2 {
			t.Fatalf("-timeout %s exited %d, want 2 -- an unusable flag is the operator's to fix", timeout, exitCodeFor(err))
		}
		// The refusal must NAME the value, so the operator can see what
		// was parsed rather than guess.
		if !strings.Contains(err.Error(), timeout.String()) {
			t.Fatalf("the refusal for -timeout %s does not name the value: %v", timeout, err)
		}
	}
	for _, timeout := range accepted {
		common := commonFlags{timeout: timeout}
		if err := common.requirePositiveTimeout(); err != nil {
			t.Fatalf("-timeout %s was refused: %v", timeout, err)
		}
	}
}

// Every verb WIRES the guard, which is the half a shared helper is most
// likely to be missing: a helper nobody calls refuses nothing.
//
// Driven through runCommand -- the real dispatch -- rather than by
// grepping the source, so a verb that stops calling it fails here.
func TestTheTimeoutGuardIsWiredIntoEveryVerb(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	t.Setenv(bearerEnvVar, "")
	for _, argv := range [][]string{
		{"enable", "-mode", "canary", "-timeout", "0"},
		{"disable", "-mode", "python", "-timeout", "0"},
		{"repoint", "-timeout", "0"},
		{"status", "-timeout", "0"},
	} {
		err := run(argv)
		if err == nil {
			t.Fatalf("%v was accepted", argv)
		}
		if !strings.Contains(err.Error(), "-timeout") {
			t.Fatalf("%v refused for a different reason (%v) -- the timeout guard is not wired into this verb, or runs after another check that hides it", argv, err)
		}
	}
}

// captureVerb runs a REAL verb through the REAL dispatch and returns what
// an operator would have seen.
//
// This helper is the answer to r1's P3. The tests that used it before
// rebuilt the thing under test with the same arguments the production
// line uses, so a mutation at the real call site changed nothing they
// could observe. Nothing short of running the verb closes that.
func captureVerb(t *testing.T, argv ...string) (out string, errOut string, err error) {
	t.Helper()
	var outBuf, errBuf strings.Builder
	savedOut, savedErr, savedFlag := stdout, stderr, verbFlagOutput
	stdout, stderr, verbFlagOutput = &outBuf, &errBuf, &errBuf
	t.Cleanup(func() { stdout, stderr, verbFlagOutput = savedOut, savedErr, savedFlag })
	err = run(argv)
	return outBuf.String(), errBuf.String(), err
}

// An unconsumed argument is refused by EVERY verb, before anything is
// read and before anything is written.
//
// r1's first P1, and the worst defect this command has had. `flag` stops
// at the first operand and leaves the rest unparsed, so a stray word
// silently deletes every flag after it -- including the guards. Executed
// against a real database on a row that was `python`, BEFORE the fix:
//
//	disable -operations flowMatrix -mode shadow -apply -recorded-by lane \
//	  -review-evidence why UNEXPECTED-OPERAND -candidate-build 0000…0000
//	  -> applied: 1 row(s) now mode=shadow, exit 0, row read back as shadow
//
// The same command WITHOUT the stray word refuses on that guard and
// writes nothing. The operand deleted the guard and the verb reported
// success.
//
// Driven through `run` so the refusal is proved where it is installed,
// and asserted on the EXIT CODE as well as the message: a refusal that
// exits 1 is an internal error to every calling script.
func TestEveryVerbRefusesAnUnconsumedArgument(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	t.Setenv(bearerEnvVar, "")

	for name, argv := range map[string][]string{
		// The exact shapes that bypassed a guard, one per verb.
		"disable loses -candidate-build": {"disable", "-operations", "flowMatrix", "-mode", "python", "-apply",
			"-recorded-by", "lane", "-review-evidence", "why", "UNEXPECTED-OPERAND",
			"-candidate-build", "0000000000000000000000000000000000000000"},
		"enable loses -dry-run": {"enable", "-mode", "canary", "-recorded-by", "lane",
			"-review-evidence", "why", "UNEXPECTED-OPERAND", "-dry-run"},
		"repoint loses -expect-build": {"repoint", "-recorded-by", "lane", "-review-evidence", "why",
			"UNEXPECTED-OPERAND", "-expect-build", "deadbeef"},
		"status loses -json": {"status", "UNEXPECTED-OPERAND", "-json"},
		// The boolean spelling an operator actually reaches for. `flag`
		// wants -flag=false; -flag false makes `false` an OPERAND, so the
		// operator who meant to turn the acknowledgement OFF turns it ON
		// and loses every later flag as well.
		"a bare boolean value is an operand": {"enable", "-mode", "canary", "-recorded-by", "lane",
			"-review-evidence", "why", "-acknowledge-unproven", "false", "-dry-run"},
		// A single trailing word with no flags after it, which is the
		// harmless-looking version of the same mistake.
		"one trailing word": {"status", "leftover"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := captureVerb(t, argv...)
			if err == nil {
				t.Fatalf("%v was accepted", argv)
			}
			if exitCodeFor(err) != 2 {
				t.Fatalf("%v exited %d, want 2 -- a mistyped command line is the operator's to fix", argv, exitCodeFor(err))
			}
			if !strings.Contains(err.Error(), "unexpected argument") {
				t.Fatalf("%v refused for a different reason, so the operand check is not what stopped it: %v", argv, err)
			}
			// The refusal NAMES the word. "unexpected argument" alone
			// leaves the operator hunting through their own command line.
			if !strings.Contains(err.Error(), "UNEXPECTED-OPERAND") &&
				!strings.Contains(err.Error(), "leftover") &&
				!strings.Contains(err.Error(), "false") {
				t.Fatalf("%v: the refusal does not name the offending word: %v", argv, err)
			}
		})
	}

	// ...and the same command lines WITHOUT the stray word get past the
	// operand check. Otherwise a guard that refused everything would pass
	// the loop above.
	for name, argv := range map[string][]string{
		"disable": {"disable", "-operations", "flowMatrix", "-mode", "python", "-apply",
			"-recorded-by", "lane", "-review-evidence", "why",
			"-candidate-build", "0000000000000000000000000000000000000000"},
		"status": {"status"},
	} {
		t.Run(name+" without the operand", func(t *testing.T) {
			_, _, err := captureVerb(t, argv...)
			if err != nil && strings.Contains(err.Error(), "unexpected argument") {
				t.Fatalf("%v was refused as carrying an operand: %v", argv, err)
			}
		})
	}
}

// The DSN never reaches the usage text of the REAL verb's flag set.
//
// r1's M10: reintroducing `os.Getenv("POSTGRES_URI")` as the default
// INSIDE runEnable survived both package suites, because the previous
// test built its own flag set with the same three arguments and so only
// proved that the test agreed with itself. This drives `run` with `-h` on
// each verb and reads the usage text the flag set actually produced.
func TestNoRealVerbsUsageTextEverPrintsTheDSN(t *testing.T) {
	const secret = "hunter2-not-a-real-password"
	t.Setenv("POSTGRES_URI", "postgresql://postgres:"+secret+"@db.internal:5432/devhealth")
	t.Setenv(bearerEnvVar, "")

	for _, verb := range []string{"enable", "disable", "repoint", "status"} {
		t.Run(verb, func(t *testing.T) {
			_, usage, _ := captureVerb(t, verb, "-h")
			if usage == "" {
				t.Fatalf("%s -h produced no usage text, so this test can prove nothing", verb)
			}
			if !strings.Contains(usage, "-postgres-uri") {
				t.Fatalf("%s usage text does not list -postgres-uri, so this test is not reading the right flag set:\n%s", verb, usage)
			}
			if strings.Contains(usage, secret) {
				t.Fatalf("%s usage text prints the DSN's password:\n%s", verb, usage)
			}
			if strings.Contains(usage, "db.internal") {
				t.Fatalf("%s usage text prints the DSN's host, so it is printing the DSN:\n%s", verb, usage)
			}
			if !strings.Contains(usage, postgresURIEnvVar) {
				t.Fatalf("%s usage text no longer names %s, so an operator cannot tell where the DSN comes from:\n%s", verb, postgresURIEnvVar, usage)
			}
		})
	}
}
