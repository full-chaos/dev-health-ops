// Command go-api-routing carries CHAOS-5486's mode-preserving re-point:
// it points every go_api_routing_state row at the build the deployed
// query-api is actually running, without touching any column that decides
// reachability.
//
// WHY A GO BINARY AND NOT A NEW `dev-hops go-api routing` SUBCOMMAND.
// The cutover rule is that no new Python compute is written (chris: "move
// to go, do not straddle"), and team-lead ruling R49 already settled the
// same question for go-api-prove: a Python shim on the critical path of a
// rollout operation is the straddle the rule forbids. The existing Python
// verbs are untouched; this adds the one they cannot express.
//
// WHAT enable/disable COULD NOT DO, measured on the compose stack
// 2026-09-09 (lane-stack-owner, JOB 4 and JOB 5 prep):
//
//   - `routing enable` writes current_candidate_build but requires
//     --mode canary|primary, so re-pointing a shadow row would also make
//     that operation Go-serving through the product edge;
//   - `routing disable` accepts --mode shadow but its --candidate-build is
//     a guard documented "Never written -- disable changes mode only".
//
// Three shadow rows were therefore stuck naming an old build while the
// process ran another, and the proof runner refused on all fifteen
// operations because it inspects every row regardless of mode.
//
// SAFETY. The build written is READ from the deployed process's
// /buildinfo and never accepted from a flag: --expect-build is a
// cross-check that can only FAIL a run. Every write records who ran it and
// why. The whole re-point is one transaction, because a partial re-point
// is precisely the state the proof runner refuses on.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// bearerEnvVar names the environment variable carrying the effective-
// principal envelope /buildinfo checks. The VALUE never appears in output.
//
// It is the ENVELOPE, not the edge access token: measured on the deployed
// stack, an access token gets 401 on /buildinfo and 200 on the Python
// edge, and the envelope gets the reverse. They are different credential
// kinds checked by different verifiers.
const bearerEnvVar = "GO_API_ROUTING_BEARER"

type flags struct {
	registryURL    string
	buildInfoURL   string
	postgresURI    string
	operations     string
	recordedBy     string
	reviewEvidence string
	expectBuild    string
	dryRun         bool
	timeout        time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "go-api-routing: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() (flags, error) {
	var f flags
	flag.StringVar(&f.registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api -- the only authority on which schema digest is live")
	flag.StringVar(&f.buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build every row is pointed at")
	flag.StringVar(&f.postgresURI, "postgres-uri", os.Getenv("POSTGRES_URI"), "domain Postgres DSN holding go_api_routing_state")
	flag.StringVar(&f.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	flag.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row touched (required)")
	flag.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row touched (required)")
	flag.StringVar(&f.expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written")
	flag.BoolVar(&f.dryRun, "dry-run", false, "report what would change and write NOTHING")
	flag.DurationVar(&f.timeout, "timeout", 30*time.Second, "per-request timeout")
	flag.Parse()

	missing := map[string]string{
		"-recorded-by":                    f.recordedBy,
		"-review-evidence":                f.reviewEvidence,
		"-postgres-uri (or POSTGRES_URI)": f.postgresURI,
	}
	var absent []string
	for name, value := range missing {
		if strings.TrimSpace(value) == "" {
			absent = append(absent, name)
		}
	}
	if len(absent) > 0 {
		return f, fmt.Errorf("required flags are missing: %v", absent)
	}
	return f, nil
}

// errEmptyOperationFilter refuses an --operations value that names nothing.
//
// A nil return from requestedOperations means "every row at the digest", and
// this is a WRITE verb: silently widening `--operations ","` (or any
// separators-only string) from "the operator named something" to "re-point
// everything" is the failure this refuses. `all-registered` and an OMITTED
// flag are the only ways to ask for every row, and both say so out loud.
var errEmptyOperationFilter = errors.New("--operations names no operation: pass 'all-registered' to re-point every row")

func requestedOperations(raw string) ([]string, error) {
	trimmedRaw := strings.TrimSpace(raw)
	if trimmedRaw == "" || trimmedRaw == "all-registered" {
		return nil, nil
	}
	var operations []string
	for _, name := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			operations = append(operations, trimmed)
		}
	}
	if len(operations) == 0 {
		return nil, fmt.Errorf("%w (got %q)", errEmptyOperationFilter, raw)
	}
	return operations, nil
}

func run() error {
	f, err := parseFlags()
	if err != nil {
		return err
	}
	bearer := os.Getenv(bearerEnvVar)
	if bearer == "" {
		return fmt.Errorf("no credential: set %s to an effective-principal ENVELOPE, which is what /buildinfo checks (the VALUE is never printed by this command)", bearerEnvVar)
	}

	ctx := context.Background()
	client := &http.Client{Timeout: f.timeout}
	// A Credential rather than a header map: FetchBuildIdentity's parameter
	// changed in CHAOS-5479, because one credential cannot satisfy both
	// planes -- an access token gets 200 on the Python edge and 401 on
	// /buildinfo, and the envelope gets the reverse. This command already
	// documents that at bearerEnvVar and already carries the ENVELOPE, so
	// this is the same value in the type the function now takes.
	//
	// StaticCredential also refuses an empty or whitespace-only value at
	// the moment of use, so the check above gains a second floor rather
	// than losing one, and its `kind` names the credential in a 401
	// without printing it.
	credential := goapiproof.StaticCredential("Authorization", "effective-principal envelope", "Bearer "+bearer)

	registry, err := goapiproof.FetchRegistry(ctx, client, f.registryURL)
	if err != nil {
		return err
	}
	running, err := goapiproof.FetchBuildIdentity(ctx, client, f.buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return fmt.Errorf("%w\n  the deployed query-api must identify its build at %s before any row can be pointed at it", err, f.buildInfoURL)
		}
		return err
	}

	pool, err := pgxpool.New(ctx, f.postgresURI)
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer pool.Close()

	operations, err := requestedOperations(f.operations)
	if err != nil {
		return err
	}

	outcomes, err := goapiproof.Repoint(ctx, pool, goapiproof.RepointRequest{
		SchemaDigest:   registry.SchemaDigest,
		RunningBuild:   running,
		ExpectBuild:    f.expectBuild,
		Operations:     operations,
		RecordedBy:     f.recordedBy,
		ReviewEvidence: f.reviewEvidence,
		DryRun:         f.dryRun,
	})
	if err != nil {
		return err
	}

	summary := goapiproof.Summarize(outcomes)
	verb := "repointed"
	if f.dryRun {
		verb = "would repoint"
	}
	// Every counter prints, including the zeros: "no row needed changing"
	// and "nobody looked" must not read alike.
	fmt.Printf("go-api-routing: schema_digest=%s running_build=%s dry_run=%t\n", registry.SchemaDigest, running, f.dryRun)
	fmt.Printf("go-api-routing: %s total=%d changed=%d unchanged=%d\n", verb, summary.Total, summary.Changed, summary.Unchanged)
	for _, outcome := range outcomes {
		state := "unchanged"
		if outcome.Changed {
			state = "repointed"
		}
		fmt.Printf("go-api-routing:   %-24s mode=%-8s %s  %s -> %s\n",
			outcome.Operation, outcome.ModeAfter, state, outcome.BuildFrom, outcome.BuildTo)
	}
	return nil
}
