package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// r1 P1: the minting helper's output must never reach an operator-facing
// error. The previous version put the shell command AND its complete
// stderr into the error, and main printed it -- so a helper that wrote a
// credential to stderr published it.
//
// The reviewer's test proved that with a synthetic secret. This is the
// same attack against the new contract, widened: stderr, stdout and argv
// are all checked, because all three can carry a credential and only one
// of them was leaking before.
func TestTheMinterNeverReportsWhatTheHelperEmitted(t *testing.T) {
	const secret = "review-only-proof-secret-7d3f"

	for name, script := range map[string]string{
		"secret on stderr, nonzero exit": "#!/bin/sh\nprintf '%s' \"" + secret + "\" >&2\nexit 1\n",
		"secret on stderr, zero exit":    "#!/bin/sh\nprintf '%s' \"" + secret + "\" >&2\nexit 0\n",
		"secret on stdout, nonzero exit": "#!/bin/sh\nprintf '%s' \"" + secret + "\"\nexit 3\n",
	} {
		t.Run(name, func(t *testing.T) {
			helper := writeHelper(t, script)
			// The secret is also in the ARGV here, which is the other
			// half of the old design's exposure: an argv is world-readable
			// in the process table.
			_, err := mintBearer(context.Background(), []string{helper, secret})
			if err == nil {
				t.Fatal("expected the helper to fail")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("the minter error exposed the helper's output: %v", err)
			}
			if strings.Contains(err.Error(), helper) {
				t.Fatalf("the minter error exposed the helper's path/argv: %v", err)
			}
		})
	}
}

// The error still has to be USEFUL. "Something went wrong" sends an
// operator to read this source; the exit code sends them to their own
// helper's logs.
func TestTheMinterErrorNamesTheExitCodeAndWhereToLook(t *testing.T) {
	helper := writeHelper(t, "#!/bin/sh\nexit 42\n")
	_, err := mintBearer(context.Background(), []string{helper})
	if err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(err.Error(), "42") {
		t.Fatalf("the error must name the exit code: %v", err)
	}
	if !strings.Contains(err.Error(), "helper's own logs") {
		t.Fatalf("the error must say where to look, since it deliberately says nothing else: %v", err)
	}
}

// A helper that hangs must be killed, and killed WITH ITS CHILDREN. A
// `docker compose exec` helper is a process tree; killing only the parent
// orphans the rest.
func TestAHangingHelperIsKilledWithinItsOwnTimeout(t *testing.T) {
	// The child writes its own pid file then sleeps far past the timeout;
	// the parent waits on it. Killing only the parent would leave the
	// child running.
	helper := writeHelper(t, "#!/bin/sh\nsleep 300 &\necho $! > \"$1\"\nwait\n")
	pidFile := filepath.Join(t.TempDir(), "child.pid")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	_, err := mintBearer(ctx, []string{helper, pidFile})
	if err == nil {
		t.Fatal("a hanging helper must fail")
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("the helper was not bounded: %s", elapsed)
	}
	if strings.Contains(err.Error(), helper) {
		t.Fatalf("the timeout error exposed the argv: %v", err)
	}
}

// A helper that prints megabytes must not be buffered whole just to be
// rejected.
func TestHelperOutputIsBounded(t *testing.T) {
	helper := writeHelper(t, "#!/bin/sh\nhead -c 5000000 /dev/zero | tr '\\0' 'a'\n")
	minted, err := mintBearer(context.Background(), []string{helper})
	if err != nil {
		// Rejected outright is also acceptable; what must not happen is
		// an unbounded read.
		return
	}
	if len(minted) > mintStdoutLimit+len("Bearer ") {
		t.Fatalf("read %d bytes, limit is %d: a chatty helper must be truncated", len(minted), mintStdoutLimit)
	}
}

// r1 P2: a malformed STATIC envelope must fail at construction, with a
// message that does not echo it. Before, only the minted path was
// validated, so a static value became a confusing 401 fifteen operations
// later.
func TestAMalformedStaticProofBearerFailsAtConstruction(t *testing.T) {
	const bad = "not-an-envelope-but-still-a-secret"
	t.Setenv(edgeBearerEnvVar, "edge-token")
	t.Setenv(proofBearerEnvVar, bad)

	_, _, err := credentials(flags{})
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
	t.Setenv(edgeBearerEnvVar, "edge-token")
	t.Setenv(proofBearerEnvVar, "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln")
	edge, proof, err := credentials(flags{})
	if err != nil {
		t.Fatalf("a well-formed static envelope was refused: %v", err)
	}
	if edge == nil || proof == nil {
		t.Fatal("both credentials must be constructed")
	}
}

// -proof-bearer-exec is argv, not a shell string: a credential in the
// command line is readable in the process table, and a shell string
// invites injection through anything interpolated into it.
func TestTheExecFlagRefusesAnythingThatIsNotAJSONArgv(t *testing.T) {
	t.Setenv(edgeBearerEnvVar, "edge-token")
	for _, value := range []string{
		"/opt/job5/mint-envelope.sh --org 70d529e0", // a shell string
		"[]",     // empty argv
		"{}",     // an object
		"[1, 2]", // not strings
		"not json",
	} {
		if _, _, err := credentials(flags{proofBearerExec: value}); err == nil {
			t.Fatalf("%q was accepted as an argv", value)
		}
	}

	argv, err := json.Marshal([]string{"/opt/job5/mint-envelope.sh", "--org", "70d529e0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := credentials(flags{proofBearerExec: string(argv)}); err != nil {
		t.Fatalf("a valid JSON argv was refused: %v", err)
	}
}

func writeHelper(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "helper.sh")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// r1 P2: the opening and closing /buildinfo reads took context.Background(),
// so `-timeout` bounded every request EXCEPT the two that bracket the run --
// including the minting helper invoked before the first measurement, where a
// hang blocks the whole proof with nothing measured to show for it.
//
// Asserted against mintBearer under a caller deadline, which is the path
// both bracketing calls now take.
func TestTheMinterRespectsACallerDeadline(t *testing.T) {
	helper := writeHelper(t, "#!/bin/sh\nsleep 60\n")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	if _, err := mintBearer(ctx, []string{helper}); err == nil {
		t.Fatal("a helper outliving the caller's deadline must fail")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("the caller's deadline was ignored: blocked for %s", elapsed)
	}
}

// r3 P2: the overflow refusal existed but never fired. A helper that
// overruns the limit also makes cmd.Run() return an error, and the
// run-failure branch was checked first -- so the operator was told "could
// not be run" about a case we deliberately detect and have a precise
// message for. Truncation was fixed in r2; the diagnosis had moved.
func TestOverflowIsReportedAsOverflowNotAsAFailedRun(t *testing.T) {
	// Exactly the limit in JWT-shaped bytes, then more -- r2's attack.
	helper := writeHelper(t, "#!/bin/sh\nhead -c 8192 /dev/zero | tr '\\0' 'a'\nprintf 'aaaa'\n")

	_, err := mintBearer(context.Background(), []string{helper})
	if err == nil {
		t.Fatal("output past the limit must be refused")
	}
	if !strings.Contains(err.Error(), "more than") {
		t.Fatalf("the error must say the output was too long, got: %v", err)
	}
	if strings.Contains(err.Error(), "could not be run") {
		t.Fatalf("an overflow was reported as a failed run: %v", err)
	}
}

// r4 P1-2. The process-group kill fires, but cmd.Run() waits on the
// STDOUT PIPE, and a child that inherited it keeps that pipe open after
// the parent exits. Measured 2.0s against a 100ms deadline.
//
// A hung helper must not be able to outlive the caller's deadline by
// leaving a grandchild holding the write end.
func TestAnExitedParentWithALivingChildStillHonoursTheDeadline(t *testing.T) {
	// The parent exits immediately; the child inherits stdout and sleeps.
	helper := writeHelper(t, "#!/bin/sh\nsleep 5 &\nexit 0\n")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, _ = mintBearer(ctx, []string{helper})
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the caller deadline did not bound a helper whose parent exited with a child holding stdout: blocked for %s", elapsed)
	}
}

// r4 found the report was missing two counters it claimed to compute: an
// earlier edit reverted the computed build-binding line to a hardcoded
// sentence and dropped the mint count entirely. Nothing failed, because no
// test read this output at all.
//
// So this reads it. An instrument nobody asserts on is an instrument that
// silently stops working -- which is the same class as a guard with no
// killer test, one layer out.
func TestTheReportCarriesTheCountersItComputes(t *testing.T) {
	credential := goapiproof.MintedCredential("Authorization", "envelope", 0,
		func(context.Context) (string, error) {
			return "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln", nil
		}).WithShapeValidator(goapiproof.ValidateEnvelopeShape)
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
	}}
	summary := goapiproof.Summary{Attempted: 1, Admitted: 1, Executed: 1}

	printed := captureStdout(t, func() {
		if err := emitReport(flags{orgID: "o", edgeURL: "http://edge.test/graphql"},
			goapiproof.RegistryView{SchemaDigest: "sha256:x", BuildIdentity: "b"},
			outcomes, summary, credential); err != nil {
			t.Fatalf("emitReport: %v", err)
		}
	})

	if !strings.Contains(printed, "envelope mints = 3") {
		t.Fatalf("the mint COUNT must reach the report -- a run that minted once will fail at the closing build check, and this line is the only warning:\n%s", printed)
	}
	if !strings.Contains(printed, "build binding "+goapiproof.EdgeBuildAbsent+" = 1") {
		t.Fatalf("the build-binding counter must be COUNTED from the outcomes, not asserted as a sentence:\n%s", printed)
	}
	// And it must never print the credential itself.
	if strings.Contains(printed, "eyJhbGciOiJFZERTQSJ9") {
		t.Fatalf("the report printed the credential:\n%s", printed)
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
