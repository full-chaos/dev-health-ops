package prove

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof/writeproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// fixtureOrgEnvVar names the per-environment Fixture Org the write proof may
// write in. -org must equal it: the verb never creates an org, and never writes
// in any other (CHAOS-6810 ruling: no new scratch orgs on prod).
const fixtureOrgEnvVar = "GO_API_PROVE_WRITE_FIXTURE_ORG"

// The two routes a write proof can measure through (the receipt's
// measurement_route): a direct POST to query-api's /query ("proof": /query/proof
// refuses a mutation, and an operation not yet routed to Go cannot reach the Go
// build through the edge) or the product edge itself ("edge", which only serves
// Go once the operation is canary or primary, and is what primary enablement
// requires).
const (
	viaQueryAPI = "query-api"
	viaEdge     = "edge"
)

// WriteCommand is the `goapi prove-write` verb of the dho binary: ONE execution of
// a registered mutation case inside the Fixture Org, its persisted effects
// digested and compared with the case's committed baseline, and an immutable
// `write_executed` receipt recorded. It never runs the Python resolver (the CI
// oracle owns Python-versus-Go) and never posts the mutation more than once.
func WriteCommand() cli.Command {
	return cli.Command{
		Name:    "prove-write",
		Kind:    cli.Verb,
		Summary: "execute one registered GraphQL mutation case once in the Fixture Org and record a write_executed receipt",
		Run: func(_ context.Context, env cli.Env) int {
			err := runWrite(env.Args)
			if err != nil && !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintf(env.Stderr, "go-api-prove-write: %v\n", err)
			}
			return cli.ExitForVerbError(err)
		},
	}
}

type writeFlags struct {
	registryURL    string
	buildInfoURL   string
	queryURL       string
	edgeURL        string
	via            string
	documentsPath  string
	postgresURI    string
	orgID          string
	caseName       string
	recordedBy     string
	reviewEvidence string
	principalKind  string
	audience       string
	keyID          string
	dryRun         bool
	timeout        time.Duration
}

func registerWriteFlags() (*flag.FlagSet, *writeFlags) {
	f := &writeFlags{}
	fs := flag.NewFlagSet("go-api-prove-write", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	fs.StringVar(&f.registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api")
	fs.StringVar(&f.buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api: the ONLY source of the build identity the receipt names")
	fs.StringVar(&f.queryURL, "query-url", "http://localhost:8090/query", "query-api's POST /query, used with -via query-api")
	fs.StringVar(&f.edgeURL, "edge-url", "http://localhost:8000/graphql", "the product GraphQL edge, used with -via edge")
	fs.StringVar(&f.via, "via", viaQueryAPI, "how the mutation is reached: query-api (a direct POST to /query; the receipt records route proof, which admits canary) or edge (the product edge; the receipt records route edge, needed for primary)")
	fs.StringVar(&f.documentsPath, "documents", "", "path to `registrydump` JSON output (required)")
	secrets.BindFlag(fs, &f.postgresURI, "postgres-uri", postgresURIEnvVar, "domain Postgres DSN holding the case's tables and go_api_proof_run")
	fs.StringVar(&f.orgID, "org", "", "the Fixture Org id; must equal $"+fixtureOrgEnvVar+" (required)")
	fs.StringVar(&f.caseName, "case", "", "the registered write case to run (required)")
	fs.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on the receipt (required)")
	fs.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on the receipt (required)")
	fs.StringVar(&f.principalKind, "principal-kind", "stored_account", "auth-context SHAPE recorded in request_identity; never a credential")
	fs.StringVar(&f.audience, "audience", "query-api", "envelope audience, part of the auth-context shape")
	fs.StringVar(&f.keyID, "key-id", "", "envelope signing key id (kid)")
	fs.BoolVar(&f.dryRun, "dry-run", false, "execute and compare, but write NO receipt (the mutation STILL RUNS in the Fixture Org: dry-run is about the ledger, not the write)")
	fs.DurationVar(&f.timeout, "timeout", 60*time.Second, "per-request timeout")
	return fs, f
}

func parseWriteFlags(args []string) (writeFlags, error) {
	fs, fp := registerWriteFlags()
	if err := fs.Parse(args); err != nil {
		return *fp, cli.WrapFlagParseError(err)
	}
	f := *fp
	secrets.ResolveFlag(fs, &f.postgresURI, "postgres-uri", postgresURIEnvVar)
	var absent []string
	for name, value := range map[string]string{
		"-documents": f.documentsPath, "-org": f.orgID, "-case": f.caseName,
		"-recorded-by": f.recordedBy, "-review-evidence": f.reviewEvidence,
		"-postgres-uri (or " + postgresURIEnvVar + ")": f.postgresURI,
	} {
		if value == "" {
			absent = append(absent, name)
		}
	}
	if len(absent) > 0 {
		return f, fmt.Errorf("required flags are missing: %v", absent)
	}
	if f.via != viaQueryAPI && f.via != viaEdge {
		return f, fmt.Errorf("-via must be %q or %q, got %q", viaQueryAPI, viaEdge, f.via)
	}
	return f, nil
}

// requireFixtureOrg refuses any org but the environment's declared Fixture Org.
func requireFixtureOrg(org string) error {
	fixture := os.Getenv(fixtureOrgEnvVar)
	switch {
	case fixture == "":
		return fmt.Errorf("%s is not set: a write proof writes only inside the per-environment Fixture Org and this run has not been told which org that is (its value is never guessed and no org is created)", fixtureOrgEnvVar)
	case org != fixture:
		return fmt.Errorf("-org is not the Fixture Org named by %s: a write proof never writes in any other org", fixtureOrgEnvVar)
	}
	return nil
}

func runWrite(args []string) (err error) {
	f, parseErr := parseWriteFlags(args)
	if parseErr != nil {
		return secrets.NewBoundary(f.postgresURI).Redact(parseErr)
	}
	boundary := secrets.NewBoundary(f.postgresURI)
	defer func() { err = boundary.Redact(err) }()

	if err := goapiproof.ValidateOperatorEvidence(f.reviewEvidence); err != nil {
		return err
	}
	if err := requireFixtureOrg(f.orgID); err != nil {
		return err
	}
	for _, flagged := range []struct{ name, value string }{
		{"-registry-url", f.registryURL}, {"-buildinfo-url", f.buildInfoURL},
		{"-query-url", f.queryURL}, {"-edge-url", f.edgeURL},
	} {
		if err := goapiproof.ValidateBaseURL(flagged.name, flagged.value); err != nil {
			return err
		}
	}
	c, ok := writeproof.Lookup(f.caseName)
	if !ok {
		return fmt.Errorf("no write case named %q is registered (registered: %v): no mutation is enabled to run", f.caseName, writeproof.Names())
	}

	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()
	client := goapiproof.NewLegClient(f.timeout)
	bounded := func() (context.Context, context.CancelFunc) {
		if f.timeout <= 0 {
			return context.WithCancel(ctx)
		}
		return context.WithTimeout(ctx, f.timeout)
	}

	edgeCredential, proofCredential, err := credentials(flags{orgID: f.orgID})
	if err != nil {
		return err
	}
	edgeCredential.BindOrg(f.orgID)
	proofCredential.BindOrg(f.orgID)

	registry, err := goapiproof.FetchRegistry(ctx, client, f.registryURL)
	if err != nil {
		return err
	}
	if err := refuseEmptyRegistry(registry, f.registryURL); err != nil {
		return err
	}
	buildCtx, cancelBuild := bounded()
	registry.BuildIdentity, err = goapiproof.FetchBuildIdentity(buildCtx, client, f.buildInfoURL, proofCredential)
	cancelBuild()
	if err != nil {
		return err
	}

	documents, err := goapiproof.LoadDocuments(f.documentsPath)
	if err != nil {
		return err
	}
	if err := goapiproof.VerifyDocuments(documents, registry, goapidigest.Document); err != nil {
		return err
	}
	document, registered := documents[c.Operation]
	digest, served := registry.DocumentDigest[c.Operation]
	if !registered || !served {
		return fmt.Errorf("the running build does not register the operation %q of case %q", c.Operation, c.Name)
	}
	if kind, kindErr := goapidigest.DocumentKind(document); kindErr != nil || kind != goapidigest.KindMutation {
		return fmt.Errorf("case %q names operation %q whose registered document is not a mutation (kind %q, err %v): a query is proven by `prove`", c.Name, c.Operation, kind, kindErr)
	}

	pool, err := openPostgresPool(ctx, f.postgresURI)
	if err != nil {
		return secrets.RedactedConnectError("connect to Postgres", "postgres-uri", postgresURIEnvVar)
	}
	defer pool.Close()

	target, credential, route := f.queryURL, proofCredential, goapiproof.RouteProof
	if f.via == viaEdge {
		target, credential, route = f.edgeURL, edgeCredential, goapiproof.RouteEdge
	}
	run := writeproof.RunTag("gwc-wp-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12])
	fmt.Printf("go-api-prove-write: case=%s operation=%s via=%s org=%s run=%s build=%s\n", c.Name, c.Operation, f.via, f.orgID, run, registry.BuildIdentity)

	// The ONE post. Never retried here or by the client.
	post := func(ctx context.Context, doc, variablesJSON string) (writeproof.Response, error) {
		return postMutation(ctx, client, target, credential, doc, variablesJSON, f.timeout)
	}
	result, execErr := writeproof.Execute(ctx, pool, f.orgID, c, run, document, post, writeproof.WithRequiredBuild(registry.BuildIdentity), writeproof.WithDeferredTeardown())
	if execErr != nil {
		return execErr
	}

	// Teardown is irreversible and every step below can still demote a match (the
	// build moving, the receipt not being recorded): the dataset is torn down only
	// by Settle, LAST, and any early return keeps it.
	settled := false
	defer func() {
		if !settled {
			result.Demote("the run ended before its receipt was recorded")
			fmt.Printf("go-api-prove-write: dataset KEPT for forensics: org=%s run=%s (the run ended before its receipt was recorded)\n", f.orgID, run)
		}
	}()

	binding := goapiproof.EdgeBuildAbsent
	if result.ServedBuild != "" && result.ServedBuild == registry.BuildIdentity {
		binding = goapiproof.EdgeBuildPresent
	}
	stableCtx, cancelStable := bounded()
	stabilityErr := goapiproof.VerifyBuildStable(stableCtx, client, f.buildInfoURL, proofCredential, registry.BuildIdentity)
	cancelStable()
	if stabilityErr != nil {
		// The build moved under the run: what was measured is not evidence for
		// the build the receipt would name.
		result.Demote("build not stable: " + stabilityErr.Error())
	}

	identity, err := goapiproof.RequestIdentity(f.orgID, goapiproof.AuthContext{PrincipalKind: f.principalKind, Audience: f.audience, KeyID: f.keyID},
		map[string]any{"case": c.Name, "via": f.via})
	if err != nil {
		return err
	}
	receipt, err := result.Receipt(writeproof.ReceiptInput{
		SchemaDigest: registry.SchemaDigest, DocumentDigest: digest, CandidateBuild: registry.BuildIdentity,
		RequestIdentity: identity, Org: f.orgID, RecordedBy: f.recordedBy, ReviewEvidence: f.reviewEvidence,
		Route: route, BuildBinding: binding, ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	written := false
	if !f.dryRun {
		if _, err := goapiproof.WriteAtomic(ctx, pool, receipt); err != nil {
			return fmt.Errorf("record the receipt: %w", err)
		}
		written = true
	}
	// The receipt is durable (or this is a dry run): only now may a match's
	// dataset go.
	result.Settle(ctx, pool)
	settled = true
	fmt.Printf("go-api-prove-write: %s posts=%d state=%s digest=%s baseline=%s build_binding=%s route=%s receipt_written=%t kept=%t\n",
		c.Name, result.Posts, result.TerminalState, result.Digest, result.BaselineDigest, binding, route, written, result.Kept != nil)
	if result.Kept != nil {
		fmt.Printf("go-api-prove-write: dataset KEPT for forensics: org=%s run=%s (the next run's cleanup or an operator removes it)\n", result.Kept.Org, result.Kept.Run)
	}
	if result.TeardownErr != nil {
		return fmt.Errorf("the proof matched but teardown of run %s failed: %w", run, result.TeardownErr)
	}
	if result.TerminalState != writeproof.StateMatch {
		return fmt.Errorf("write proof for %q ended %s: %s", c.Name, result.TerminalState, result.Detail)
	}
	return nil
}

// postMutation sends the document and the case's variables VERBATIM (the raw JSON
// text is spliced into the body, never decoded and re-encoded) exactly once.
func postMutation(ctx context.Context, client *goapiproof.LegClient, url string, credential *goapiproof.Credential, document, variablesJSON string, timeout time.Duration) (writeproof.Response, error) {
	encodedDocument, err := json.Marshal(document)
	if err != nil {
		return writeproof.Response{}, fmt.Errorf("encode the document: %w", err)
	}
	body := append(append(append([]byte(`{"query":`), encodedDocument...), []byte(`,"variables":`)...), append([]byte(variablesJSON), '}')...)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return writeproof.Response{}, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if err := credential.Apply(ctx, request); err != nil {
		return writeproof.Response{}, err
	}
	sent := goapiproof.SecretsOnRequest(request.Header)
	leg, err := client.Do(request)
	if err != nil {
		return writeproof.Response{}, fmt.Errorf("post to %s failed", goapiproof.EndpointLabel(url))
	}
	defer func() { _ = leg.Response.Body.Close() }()
	answer, err := io.ReadAll(leg.Response.Body)
	if err != nil {
		return writeproof.Response{}, fmt.Errorf("read the response of %s: %w", goapiproof.EndpointLabel(url), err)
	}
	if sent.ReflectedIn(answer) || sent.HeaderReflects(leg.Response.Header) {
		return writeproof.Response{}, fmt.Errorf("%s answered with a body or header that contains the credential this request sent; refused so it is never stored", goapiproof.EndpointLabel(url))
	}
	return writeproof.Response{
		Status: leg.Response.StatusCode, Body: answer,
		Build:        strings.TrimSpace(leg.Response.Header.Get("x-dev-health-build")),
		WireAttempts: leg.WireAttempts,
	}, nil
}
