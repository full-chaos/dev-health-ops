//go:build integration

package routing

// End-to-end tests for the `carry` verb: the real command, a real
// Postgres, a real HTTP server standing in for the DEPLOYED process, and
// the real registered-document dump reader.
//
// The verb's whole subject is a state no unit test can reach on its own
// -- two schema digests, rows at one of them, a process serving the
// other -- so it is driven here the way an operator drives it, through
// run(argv), with every artifact on disk.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
)

// The deployed process in these tests computes a digest this binary does
// not -- which is the only situation `carry` exists for.
const carryDeployedSchemaDigest = "sha256:29d509cd00000000000000000000000000000000000000000000000000000000"

// carryTestDocument is the registered document text for the operation
// under test. Its digest is COMPUTED from it with the same function the
// running process uses, so the fixture cannot claim a digest its own text
// does not have.
const carryTestDocument = "query flowMatrix($orgId: String!) { analytics(orgId: $orgId) { flowMatrix { nodes { id } } } }"

func carryTestDocumentDigest() string { return goapidigest.Document(carryTestDocument) }

// writeDocumentsDump writes a registrydump-shaped file: the enumeration
// the tools image carries, built from the same commit as the binary.
func writeDocumentsDump(t *testing.T, documents map[string]string) string {
	t.Helper()
	type entry struct {
		Operation string `json:"operation"`
		Document  string `json:"document"`
		ConstName string `json:"const_name"`
		Digest    string `json:"digest"`
	}
	entries := make([]entry, 0, len(documents))
	for operation, text := range documents {
		entries = append(entries, entry{
			Operation: operation,
			Document:  text,
			ConstName: "registered" + operation + "Document",
			Digest:    goapidigest.Document(text),
		})
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/documents.json"
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func carryArgs(server *httptest.Server, dsn, catalogPath, documentsPath string, extra ...string) []string {
	argv := []string{
		"carry",
		"-registry-url", server.URL + "/registry",
		"-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-documents", documentsPath,
		"-recorded-by", "lane-gwc-api-digestcarry",
		"-review-evidence", "CHAOS-6107 carry before the roll",
	}
	return append(argv, extra...)
}

// seedLiveRow puts one enabled row at the digest the DEPLOYED process
// computes -- the state a roll would otherwise un-route.
func seedLiveRow(t *testing.T, dsn, documentDigest, mode string) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		carryDeployedSchemaDigest, documentDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', $5, 100, 'the original decision', 'operator')`,
		carryDeployedSchemaDigest, documentDigest, verbTestOperation, verbTestBuild, mode); err != nil {
		t.Fatal(err)
	}
}

func countRowsAt(t *testing.T, dsn, schemaDigest string) int {
	t.Helper()
	var count int
	if err := queryRow(t, dsn,
		`SELECT count(*) FROM go_api_routing_state WHERE schema_digest = '`+schemaDigest+`'`, &count); err != nil {
		t.Fatal(err)
	}
	return count
}

// THE CLAIM, EXECUTED: an operation enabled against the deployed process
// is still routed at the digest the image about to roll computes, and the
// rows the running pods read are untouched.
func TestCarryEndToEndRoutesTheEnabledRowAtTheDigestAboutToBecomeLive(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	seedLiveRow(t, dsn, digest, "canary")

	out, errOut, err := captureVerb(t, carryArgs(server, dsn, catalogPath, documentsPath)...)
	if err != nil {
		t.Fatalf("carry: %v\nstdout:%s\nstderr:%s", err, out, errOut)
	}
	if got := countRowsAt(t, dsn, localSchemaDigest()); got != 1 {
		t.Fatalf("%d row(s) at this binary's digest, want the carried one\nstdout:%s", got, out)
	}
	if got := countRowsAt(t, dsn, carryDeployedSchemaDigest); got != 1 {
		t.Fatalf("%d row(s) at the live digest, want the original one left untouched", got)
	}

	var mode, evidence string
	if err := queryRow(t, dsn,
		`SELECT mode, review_evidence FROM go_api_routing_state WHERE schema_digest = '`+localSchemaDigest()+`'`,
		&mode, &evidence); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" {
		t.Fatalf("carried mode = %q, want the reachability it had", mode)
	}
	if !strings.HasPrefix(evidence, "CARRIED-FROM ") || !strings.Contains(evidence, "the original decision") {
		t.Fatalf("carried evidence = %q, want the carry prefix in front of the source reason", evidence)
	}
	// The operator-facing half: both digests named, and the sentence
	// telling them what still has to happen after the roll.
	for _, want := range []string{carryDeployedSchemaDigest, localSchemaDigest(), "carried=1", "NEXT:", "repoint"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout does not mention %q:\n%s", want, out)
		}
	}
	// One structured line PER ROW, so a log search finds which operations
	// moved to the new digest rather than merely that some did.
	if !strings.Contains(errOut, "go_api_routing.carried operation="+verbTestOperation) {
		t.Fatalf("no per-row structured line on stderr:\n%s", errOut)
	}
	if !strings.Contains(errOut, "proof=none_at_target_digest") {
		t.Fatalf("the structured line must say the carried row claims no proof at the new digest:\n%s", errOut)
	}
}

// `carry` is `enable`'s preflight 2 inverted, and that inversion is the
// verb's entire reason to exist: run against a process already serving
// this binary's SDL it writes nothing at all.
func TestCarryRefusesWhenTheDeployedProcessAlreadyComputesThisDigest(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	_, _, err := captureVerb(t, carryArgs(server, dsn, catalogPath, documentsPath)...)
	if err == nil {
		t.Fatal("carry ran against a process already serving this SDL, which is `enable`'s job and skips every one of its preflights")
	}
	if exitCodeFor(err) != 3 {
		t.Fatalf("exit %d, want 3", exitCodeFor(err))
	}
	if !strings.Contains(err.Error(), "already computes") {
		t.Fatalf("refused for a different reason, so the verb's OWN preflight is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)

	// The preflight runs BEFORE anything is opened, so the same command
	// against a DSN nothing answers on refuses identically. Without that
	// ordering the refusal would depend on a database this verb has no
	// business reaching when the digests already agree.
	_, _, err = captureVerb(t, carryArgs(server, "postgres://nobody@127.0.0.1:1/none", catalogPath, documentsPath)...)
	if err == nil || !strings.Contains(err.Error(), "already computes") {
		t.Fatalf("with an unusable DSN the refusal is %v, want the same preflight refusal -- it must not depend on reaching Postgres", err)
	}
}

// The document dump is what says the text did not change. A dump
// describing different text must stop the run by name, because the row it
// would write is keyed to a digest the new image never serves.
func TestCarryRefusesWhenTheImagesOwnDocumentsDoNotMatchTheLiveOnes(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	// The image being rolled to registers a DIFFERENT document text for
	// the same operation.
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument + "\n# changed"})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	seedLiveRow(t, dsn, digest, "canary")

	out, _, err := captureVerb(t, carryArgs(server, dsn, catalogPath, documentsPath)...)
	if err == nil {
		t.Fatalf("carry wrote rows for a document the new image does not serve:\n%s", out)
	}
	if !strings.Contains(err.Error(), verbTestOperation) {
		t.Fatalf("the refusal must name the operation: %v", err)
	}
	if got := countRowsAt(t, dsn, localSchemaDigest()); got != 0 {
		t.Fatalf("%d row(s) written by a refused run", got)
	}
	// The plan is printed even on a refusal: an operator deciding whether
	// to roll needs both halves.
	if !strings.Contains(out, "REFUSE") {
		t.Fatalf("stdout does not show the refused row in the plan:\n%s", out)
	}
}

// -dry-run answers "what would this do" without doing any of it.
func TestCarryDryRunEndToEndWritesNothing(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	seedLiveRow(t, dsn, digest, "primary")

	out, errOut, err := captureVerb(t, carryArgs(server, dsn, catalogPath, documentsPath, "-dry-run")...)
	if err != nil {
		t.Fatalf("carry -dry-run: %v", err)
	}
	if got := countRowsAt(t, dsn, localSchemaDigest()); got != 0 {
		t.Fatalf("a dry run wrote %d row(s)", got)
	}
	if !strings.Contains(out, "would carry=1") {
		t.Fatalf("a dry run must say what it WOULD do:\n%s", out)
	}
	if strings.Contains(errOut, "go_api_routing.carried") {
		t.Fatalf("a dry run logged a write it did not make:\n%s", errOut)
	}
}

// r1 F3: `-timeout` must bound the DATABASE WORK, not only the dial.
//
// The dial succeeds against a perfectly healthy Postgres and the QUERY
// is then what waits -- behind, here, an ACCESS EXCLUSIVE lock held by
// somebody else. Before the fix, `carry -timeout 100ms` was still
// blocked after half a second and returned only when the lock was
// released, so the one operator running the one command that must
// complete BEFORE a roll could not tell "still running" from
// "committed".
//
// Both halves are asserted, because the bound alone is not the contract:
// it must RETURN within the timeout, AND it must say that nothing was
// written. A command that gives up inside its timeout and leaves the
// operator guessing whether it committed has replaced one unanswerable
// question with another.
func TestCarryTimeoutBoundsTheDatabaseWorkAndSaysNothingWasWritten(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	seedLiveRow(t, dsn, digest, "canary")

	// A SEPARATE connection holds the table, and holds it past the
	// command's timeout. Released through the deferred rollback, so a
	// failure here cannot wedge the rest of the package.
	ctx := context.Background()
	blocker, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	tx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Belt and braces: the explicit rollback below is what the test
	// relies on; this one only covers a t.Fatal before it.
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE public.go_api_routing_state IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("take the blocking lock: %v", err)
	}

	started := time.Now()
	_, stderrOut, err := captureVerb(t, carryArgs(server, dsn, catalogPath, documentsPath,
		"-recorded-by", "operator", "-review-evidence", "r1 F3 timeout guard", "-timeout", "500ms")...)
	elapsed := time.Since(started)
	// Released IMMEDIATELY, not at the end of the test: the assertions
	// below read the same table, and leaving the lock held would make
	// THEM the thing that waits forever -- which is how this test first
	// hung for ten minutes rather than failing.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("release the blocking lock: %v", err)
	}

	if err == nil {
		t.Fatalf("carry must not report success while the table is locked:\n%s", stderrOut)
	}
	// Generous, because this asserts a BOUND, not a stopwatch: the point
	// is that the command no longer waits for the lock holder, which
	// here would be indefinitely. Anything inside a few seconds proves
	// the server-side statement/lock timeout fired rather than the
	// command blocking.
	if elapsed > 15*time.Second {
		t.Fatalf("carry ignored its -timeout and waited %s for a lock", elapsed)
	}
	// The refusal text reaches the operator through the error the verb
	// returns (main prints it); stderrOut carries the plan. Both are
	// searched so this cannot pass on the wrong channel.
	said := err.Error() + "\n" + stderrOut
	for _, want := range []string{"did not answer within the -timeout", "ROLLED BACK WHOLE", "NOTHING was written"} {
		if !strings.Contains(said, want) {
			t.Fatalf("the timeout refusal must contain %q, got:\n%s", want, said)
		}
	}
	// And it must be TRUE: nothing at the target digest.
	if count := countRowsAt(t, dsn, localSchemaDigest()); count != 0 {
		t.Fatalf("a timed-out carry must write nothing, found %d row(s) at the target digest", count)
	}
}

// The WINDOW this ticket creates has a trap `carry` cannot close on its
// own: `disable` writes at the digest ITS OWN binary computes, and run
// from the tools image built for the commit about to roll that is the
// digest nothing is reading yet. It reports rows changed, exits 0, and
// the operation it was meant to stop keeps being served by Go.
//
// `disable` deliberately makes no HTTP call (it must work when query-api
// is down, which is when it is needed most), so it cannot ask what is
// live -- but it can see that rows exist elsewhere, and must say so.
// Reported rather than refused, for the same reason: refusing would
// break the one property the verb exists for.
func TestDisableNamesTheOtherDigestsHoldingRowsSoItCannotSilentlyStopNothing(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	// The only row is at the DEPLOYED digest, not at this binary's.
	seedLiveRow(t, dsn, digest, "canary")

	_, stderrOut, err := captureVerb(t, "disable",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-operations", verbTestOperation, "-mode", "python")
	if err != nil {
		t.Fatalf("disable must not refuse just because rows sit elsewhere: %v\n%s", err, stderrOut)
	}
	for _, want := range []string{"this command writes ONLY at", carryDeployedSchemaDigest, "stops nothing"} {
		if !strings.Contains(stderrOut, want) {
			t.Fatalf("disable must name the other digest holding rows, missing %q:\n%s", want, stderrOut)
		}
	}
}

// CONTROL for the note above: with every row at this binary's own
// digest, there is nothing to warn about and the note must NOT print. A
// warning that always fires is a warning nobody reads.
func TestDisableSaysNothingWhenEveryRowIsAtThisBinarysDigest(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`, localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'control row', 'operator')`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	_, stderrOut, err := captureVerb(t, "disable",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-operations", verbTestOperation, "-mode", "python")
	if err != nil {
		t.Fatalf("disable: %v\n%s", err, stderrOut)
	}
	if strings.Contains(stderrOut, "this command writes ONLY at") {
		t.Fatalf("the other-digest note must not fire when there are no other digests:\n%s", stderrOut)
	}
}

// With the deployed process unreachable there is no authority on what is
// live, and `status` falls back to this binary's digest. A measurement
// that did not happen must not read like one that did: the fallback is
// stated, every run, or an operator reads MATCH/PENDING/STALE as a
// reading of the deployment when it is an assumption about it.
func TestStatusSaysWhenItsClassificationIsAnAssumptionNotAReading(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	seedLiveRow(t, dsn, digest, "canary")

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	out, _, err := captureVerb(t, "status", "-registry-url", down.URL+"/registry",
		"-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	if !strings.Contains(out, "that is an assumption, not a reading of what is live") {
		t.Fatalf("status must state that its classification is a fallback when the deployed process is unreachable:\n%s", out)
	}

	// CONTROL: with the plane answering, the line must NOT print -- it
	// would then be false, and a caveat that is always there is a caveat
	// that is never read.
	up := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	upOut, _, err := captureVerb(t, "status", "-registry-url", up.URL+"/registry",
		"-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	if strings.Contains(upOut, "that is an assumption, not a reading of what is live") {
		t.Fatalf("the fallback caveat must not print when the deployed process answered:\n%s", upOut)
	}
}
