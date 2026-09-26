package prove

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func TestTheWriteVerbWritesOnlyInTheDeclaredFixtureOrg(t *testing.T) {
	t.Setenv(fixtureOrgEnvVar, "")
	if err := requireFixtureOrg("org-1"); err == nil || !strings.Contains(err.Error(), fixtureOrgEnvVar) {
		t.Fatalf("no declared Fixture Org must refuse and name the variable, got %v", err)
	}
	t.Setenv(fixtureOrgEnvVar, "org-fixture")
	if err := requireFixtureOrg("org-other"); err == nil {
		t.Fatal("an org that is not the Fixture Org must be refused: a write proof never writes in another org")
	}
	if err := requireFixtureOrg("org-fixture"); err != nil {
		t.Fatalf("the Fixture Org itself was refused: %v", err)
	}
}

func TestTheWriteVerbNamesEveryMissingFlagAndRefusesAnUnknownRoute(t *testing.T) {
	_, err := parseWriteFlags([]string{"-via=edge"})
	if err == nil || !strings.Contains(err.Error(), "-case") || !strings.Contains(err.Error(), "-org") || !strings.Contains(err.Error(), "-documents") {
		t.Fatalf("missing flags must be named, got %v", err)
	}
	full := []string{"-documents=d", "-org=o", "-case=c", "-recorded-by=r", "-review-evidence=e", "-postgres-uri=postgres://x/y"}
	if _, err := parseWriteFlags(append([]string{"-via=proof"}, full...)); err == nil {
		t.Fatal("an unknown -via was accepted")
	}
	if _, err := parseWriteFlags(append([]string{"-via=edge"}, full...)); err != nil {
		t.Fatalf("a complete invocation was refused: %v", err)
	}
}

func TestTheWriteVerbRefusesAnUnregisteredCaseBeforeAnyNetworkCall(t *testing.T) {
	t.Setenv(fixtureOrgEnvVar, "org-fixture")
	err := runWrite([]string{"-documents=d", "-org=org-fixture", "-case=no-such-case", "-recorded-by=r", "-review-evidence=e", "-postgres-uri=postgres://x/y",
		"-registry-url=http://127.0.0.1:1/registry"})
	if err == nil || !strings.Contains(err.Error(), "no write case named") {
		t.Fatalf("want a refusal naming the missing case, got %v", err)
	}
}

func TestPostMutationSendsTheVariablesVerbatimExactlyOnce(t *testing.T) {
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		w.Header().Set("x-dev-health-build", "abc123")
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer server.Close()

	// Repeated keys, a lone surrogate escape and an integer beyond 2^53: exactly what
	// a decode-and-re-encode would change.
	variables := `{"a":1,"a":2,"s":"\ud800","n":9007199254740993}`
	client := goapiproof.NewLegClient(5 * time.Second)
	response, err := postMutation(context.Background(), client, server.URL, goapiproof.StaticCredential("Authorization", "test credential", "Bearer x"), "mutation M { x }", variables, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 || !strings.HasSuffix(bodies[0], `"variables":`+variables+`}`) {
		t.Fatalf("variables must be spliced in verbatim, once: %v", bodies)
	}
	if response.Status != 200 || response.Build != "abc123" || response.WireAttempts != 1 {
		t.Fatalf("response fields not carried: %+v", response)
	}
}

// r1 P3: a backquoted word in a flag's usage text is taken by the flag package as
// the flag's argument name, which rendered `-via proof`.
func TestTheWriteVerbHelpNamesTheRealViaValues(t *testing.T) {
	var out strings.Builder
	original := flagOutput
	flagOutput = &out
	t.Cleanup(func() { flagOutput = original })
	fs, _ := registerWriteFlags()
	fs.PrintDefaults()
	help := out.String()
	if strings.Contains(help, "-via proof") {
		t.Fatalf("the help names a -via value the parser refuses:\n%s", help)
	}
	if !strings.Contains(help, "-via string") {
		t.Fatalf("expected the -via flag rendered as a string flag:\n%s", help)
	}
}

// A parse refusal never carries the DSN, and returns no half-parsed flags.
func TestTheWriteVerbFlagRefusalsRedactTheDSNAndReturnNoFlags(t *testing.T) {
	const dsn = "postgres://user:hunter2secret@db.internal/prod"
	for name, args := range map[string][]string{
		"missing flags": {"-postgres-uri=" + dsn},
		"bad via":       {"-postgres-uri=" + dsn, "-documents=d", "-org=o", "-case=c", "-recorded-by=r", "-review-evidence=e", "-via=proof"},
		"unknown flag":  {"-postgres-uri=" + dsn, "-no-such-flag"},
	} {
		f, err := parseWriteFlags(args)
		if err == nil {
			t.Fatalf("%s: expected a refusal", name)
		}
		if strings.Contains(err.Error(), "hunter2secret") {
			t.Errorf("%s: the refusal carries the DSN password: %v", name, err)
		}
		if f != (writeFlags{}) {
			t.Errorf("%s: a refusal must return the zero flags, got %+v", name, f)
		}
	}
}
