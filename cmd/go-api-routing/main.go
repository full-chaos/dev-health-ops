// Command go-api-routing is the operator surface for Go-API rollout,
// in Go.
//
// Four verbs, and the split between them is the same one
// `dev-hops go-api routing` draws:
//
//	repoint  provenance only -- point rows at the build that is actually
//	         running, touching no column that decides reachability.
//	enable   turn operations ON. Refuses on any doubt at all.
//	disable  turn operations OFF. Deliberately FEWER preflights: it must
//	         work when the planes disagree and when query-api is down,
//	         which is exactly when it is needed.
//	status   a diagnostic that never refuses and never fails on an
//	         unhealthy state.
//
// WHY A GO BINARY AND NOT A NEW `dev-hops go-api routing` SUBCOMMAND.
// The cutover rule is that no new Python compute is written (chris:
// "move to go, do not straddle"), and team-lead ruling R49 already
// settled the same question for go-api-prove: a Python shim on the
// critical path of a rollout operation is the straddle the rule forbids.
// The Python verbs are UNTOUCHED and stay in place until they are
// retired on their own ticket -- this is the Go implementation of the
// same contract, not a deletion of the old one.
//
// SAFETY, common to every write verb. The candidate build is READ from
// the deployed process's /buildinfo and never accepted from a flag
// (team-lead ruling R51): `enable --candidate-build` in Python is
// documented "by CONVENTION, unverified", and the fifteen live rows
// carry a sha nothing ever checked. Every write records who ran it and
// why, durably, on the row.
//
// THREE DELIBERATE TIGHTENINGS over the Python verbs, all documented in
// the runbook and all pinned by tests. Anything else that differs from
// `go_api_cli.py` is a defect, not a decision:
//
//  1. The candidate build cannot be typed (above).
//  2. `disable` keys its UPDATE on the ROW's own document digest, so the
//     off-ramp still works on a row whose digest has drifted from the
//     catalog. In Python that write silently matches nothing.
//  3. -recorded-by and -review-evidence are REQUIRED on every write.
//     Python derives `recorded_by` from $DEV_HOPS_OPERATOR/$SUDO_USER/
//     $USER, falling back to the literal "unknown", and permits an absent
//     reason for a proven enablement. A rollout decision attributed to
//     "unknown", with no reason, is precisely the state this surface
//     exists to end -- and CHAOS-5505 now lands an append-only audit row
//     carrying both, so the weakest row would become the common one.
//     Raised by codex r1 (F3) as an undeclared divergence, which it was;
//     declared here rather than relaxed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// bearerEnvVar names the environment variable carrying the effective-
// principal envelope /buildinfo checks. The VALUE never appears in
// output.
//
// It is the ENVELOPE, not the edge access token: measured on the deployed
// stack, an access token gets 401 on /buildinfo and 200 on the Python
// edge, and the envelope gets the reverse. They are different credential
// kinds checked by different verifiers.
//
// It is an environment variable and not a flag because a flag value
// reaches `ps`, /proc/<pid>/cmdline and shell history (the class
// CHAOS-5511 tracks for the prover).
const bearerEnvVar = "GO_API_ROUTING_BEARER"

const usage = `go-api-routing <verb> [flags]

verbs:
  repoint   point every routing row at the build /buildinfo reports, modes untouched
  enable    turn operations ON (mode canary|primary), with every preflight
  disable   turn operations OFF (mode python|disabled|shadow); mode only, never the build
  status    report both planes' digests and every operation's row; never fails

Run "go-api-routing <verb> -h" for that verb's flags.`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(stderr, "go-api-routing: %v\n", err)
		os.Exit(exitCodeFor(err))
	}
}

// errRefused marks a REFUSAL -- a state the operator must resolve --
// rather than a crash. It exits 2, matching the Python CLI's convention
// (`_refuse` returns 2) so a calling script can tell "I would not do
// that" from "I broke".
var errRefused = errors.New("refused")

// errInternal marks the opposite: a defect in THIS program, not a state
// an operator can fix. It exits 1.
var errInternal = errors.New("internal error")

// errHelpRequested marks `-h`/`-help` on a VERB's own flag set -- see
// parseVerbFlags. It is not a refusal and not a crash:
// asking what a command does must exit 0, matching this binary's own
// top-level `-h` (run's "help" case, below) and Python's argparse `-h`.
// parseVerbFlags returns this so the verb's own early
// `if err != nil { return err }` still stops the verb (nothing past the
// flag parse runs, e.g. no "-mode is required"), and run()'s
// helpAsSuccess turns it back into a plain nil before it ever reaches a
// caller -- captureVerb/run() sees a HELP request as a SUCCESS, exactly
// like the top-level case already does.
var errHelpRequested = errors.New("help requested")

// THE DEFAULT IS REFUSAL, and that inversion is deliberate.
//
// Every failure this command can produce is environmental -- an
// unreachable service, a malformed DSN, a missing flag, an unproven
// operation, a digest that moved. The set of genuine internal defects is
// tiny and nameable. Classifying crash-by-default and requiring each site
// to opt IN to being a refusal is the wrong shape for this surface: an
// unknown verb, several raw returns in `repoint`, a malformed
// `-postgres-uri`, and a non-sentinel `/buildinfo` failure are all
// independent instances of the SAME class of miss -- one class of miss
// recurring independently is the default being wrong, not separate
// oversights.
//
// So classification happens ONCE, here, and an unclassified error is a
// refusal. Forgetting to mark something now costs a script a retryable
// exit 2 instead of a spurious alert; the previous default cost the
// reverse, repeatedly. `errInternal` is the explicit opt-out.
func exitCodeFor(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errInternal):
		return 1
	default:
		return 2
	}
}

func refuse(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errRefused}, args...)...)
}

func internal(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errInternal}, args...)...)
}

// classifyWriteError distinguishes a genuine INTERNAL database failure --
// a deadlock (SQLSTATE 40P01, see F1's own reproduction), a commit
// failure, any other unclassified error the SERVER itself raised inside
// an already-open transaction -- from an operator-actionable refusal.
//
// Without this, EVERY error out of
// goapiproof.Enable/Repoint/Disable's write path exits 2, exactly like
// an ordinary "-mode is required" typo -- so a script reading "2 means
// fix your input" could not tell a deadlock, a commit failure or a Go
// panic (which exits 2 by the RUNTIME's own default, not 1) from a
// missing flag. The documented contract (this package's own doc comment,
// and docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md)
// is "1 for a crash" -- this is what makes that true for the one class of
// crash this binary can actually distinguish: an error the POSTGRES
// SERVER raised (`*pgconn.PgError`) reaching here from inside a
// transaction that had already passed every preflight. A hand-authored
// refusal (a guard mismatch, a RowsAffected()==0 check, a validation
// error) is never wrapped in a *pgconn.PgError, so it is untouched by
// this and keeps its exit 2.
//
// Deliberately NOT applied to connectPostgres's own errors (a malformed
// DSN, a dead connection dial): those are operator-fixable by design
// (r2 R2-01/R2-02) and stay refusals.
func classifyWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return internal("%v", err)
	}
	return refuse("%v", err)
}

func run(argv []string) error {
	verb, rest := splitVerb(argv)
	switch verb {
	case "repoint":
		return helpAsSuccess(runRepoint(rest))
	case "enable":
		return helpAsSuccess(runEnable(rest))
	case "disable":
		return helpAsSuccess(runDisable(rest))
	case "status":
		return helpAsSuccess(runStatus(rest))
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, usage)
		return nil
	default:
		// A REFUSAL, not a crash: a calling script has to be able to tell
		// "I would not do that" from "I broke", and the Python CLI sets
		// that convention with exit 2. Routed through refuse() rather than
		// fmt.Errorf so the split this command already implements applies
		// to it.
		return refuse("unknown verb %q\n\n%s", verb, usage)
	}
}

// helpAsSuccess turns errHelpRequested into a plain nil -- a verb
// asked to explain itself already printed its usage text via
// flag.ContinueOnError, and asking is not a failure.  Any other error
// (including nil) passes through unchanged.
func helpAsSuccess(err error) error {
	if errors.Is(err, errHelpRequested) {
		return nil
	}
	return err
}

// splitVerb picks the verb off the front of the argument list.
//
// A first argument beginning with "-" is a FLAG, not a verb, so it means
// the caller used the flat pre-verb form this command shipped with
// (#2415, and the form JOB 5's step list quotes verbatim). That form
// still works and still means `repoint` -- silently breaking a recipe an
// operator is holding is not an acceptable cost for a nicer CLI -- but it
// says so on stderr rather than defaulting quietly, because a verb that
// guesses what you meant is the shape of every other defect on this
// surface.
func splitVerb(argv []string) (string, []string) {
	if len(argv) == 0 || strings.HasPrefix(argv[0], "-") {
		if len(argv) > 0 && (argv[0] == "-h" || argv[0] == "--help") {
			return "help", nil
		}
		fmt.Fprintln(stderr,
			`go-api-routing: no verb given; running "repoint" (the only verb this command had before CHAOS-5486). Pass "repoint" explicitly -- this compatibility path will not survive the next change to this command.`)
		return "repoint", argv
	}
	return argv[0], argv[1:]
}

// commonFlags are the flags every verb shares. Registered per-verb rather
// than globally so `-h` on a verb lists exactly what that verb honours,
// and so `status` cannot silently accept a `-review-evidence` it would
// never write.
type commonFlags struct {
	postgresURI    string
	operations     string
	catalogPath    string
	recordedBy     string
	reviewEvidence string
	timeout        time.Duration
}

// buildInfoCredential is the credential this command presents to
// /buildinfo, in one place so a test can exercise the PRODUCTION
// construction rather than rebuild it and prove only that it agrees with
// itself.
//
// A Credential rather than a header map: FetchBuildIdentity's parameter
// changed in CHAOS-5479, because one credential cannot satisfy both
// planes -- an access token gets 200 on the Python edge and 401 on
// /buildinfo, and the envelope gets the reverse. This command already
// documents that at bearerEnvVar and already carries the ENVELOPE, so
// this is the same value in the type the function now takes.
//
// Three things must all be right and each is independently wrong-able:
// the header NAME (/buildinfo reads Authorization), the `Bearer ` scheme
// prefix (without it the value is not a bearer credential at all), and
// the `kind` string, which is what names the credential in a 401 without
// printing it. StaticCredential also refuses an empty or whitespace-only
// value at the moment of use, so the flag check above gains a second
// floor rather than losing one.
func buildInfoCredential(bearer string) *goapiproof.Credential {
	return goapiproof.StaticCredential("Authorization", "effective-principal envelope", "Bearer "+bearer)
}

// requireProvenance refuses a WRITE with no durable who/why, and
// NORMALISES both values in place.
//
// Trimming here rather than only inside the presence check matters:
// the Python verb strips before persisting, and storing a
// whitespace-padded identity makes two records of the same operator
// compare unequal for a reason nobody can see.
func (c *commonFlags) requireProvenance() error {
	c.recordedBy = strings.TrimSpace(c.recordedBy)
	c.reviewEvidence = strings.TrimSpace(c.reviewEvidence)
	var absent []string
	if c.recordedBy == "" {
		absent = append(absent, "-recorded-by")
	}
	if c.reviewEvidence == "" {
		absent = append(absent, "-review-evidence")
	}
	if len(absent) > 0 {
		return refuse("required flags are missing: %v -- a routing write is a decision, and a decision with no durable record is unreadable weeks later", absent)
	}
	return nil
}

// postgresURIEnvVar is the ONLY place the DSN may come from other than
// the flag, and it is named rather than inlined so the usage text can
// name the variable without ever naming its value.
const postgresURIEnvVar = "POSTGRES_URI"

// bindPostgresURI registers -postgres-uri with an EMPTY default and
// leaves the environment fallback to resolvePostgresURI, AFTER parsing.
//
// This is a credential leak, found by running the real binary rather than
// by reading it. `flag`'s usage text prints every flag's DEFAULT VALUE,
// so registering the flag as
//
//	set.StringVar(&c.postgresURI, "postgres-uri", os.Getenv("POSTGRES_URI"), ...)
//
// makes the flag's default the DSN itself -- password included -- and
// `flag` then prints it in full on any usage dump: a mistyped flag, a
// missing value, `-h`. Measured against a real database before the fix:
//
//	-postgres-uri string
//	  domain Postgres DSN holding go_api_routing_state (default
//	  "postgresql://postgres:<the actual password>@127.0.0.1:55437/devhealth")
//
// A password reaching stderr is a password reaching CI logs, scrollback
// and anything that scrapes them. It is the same class as the -bearer
// flag this command already refuses to have (see bearerEnvVar) and the
// same class as the URL leaks r3/r4 found in the endpoint sanitiser: a
// value nobody chose to print, printed by a helper that does not know
// what it is holding.
//
// An empty default prints as nothing at all, and the usage text names the
// variable instead. Behaviour is unchanged: the flag still wins, the
// environment is still the fallback.
func (c *commonFlags) bindPostgresURI(set *flag.FlagSet, usage string) {
	set.StringVar(&c.postgresURI, "postgres-uri", "",
		usage+" (falls back to the "+postgresURIEnvVar+" environment variable, whose VALUE is never printed)")
}

// resolvePostgresURI applies the environment fallback and normalises.
// Called after Parse, by every verb, whether or not the DSN is required.
func (c *commonFlags) resolvePostgresURI() {
	c.postgresURI = strings.TrimSpace(c.postgresURI)
	if c.postgresURI == "" {
		c.postgresURI = strings.TrimSpace(os.Getenv(postgresURIEnvVar))
	}
}

// queryAPIURLEnvVar is the base-URL fallback Python's `go_api_cli.py`
// reads (`_query_api_url`, `GO_API_QUERY_API_URL`) when no
// `--query-api-url` flag is given -- ONE base URL serving BOTH of its
// reads (/registry and /buildinfo). Named so usage text and refusals can
// name the variable.
const queryAPIURLEnvVar = "GO_API_QUERY_API_URL"

// resolveEndpointURL answers ONE of query-api's two routes: the explicit
// flag if the operator set one, otherwise queryAPIURLEnvVar with path
// appended, otherwise "nothing is configured at all" (ok == false).
//
// `-registry-url` and `-buildinfo-url` must never
// default to a HARDCODED `http://localhost:8090/...`, INDEPENDENTLY of
// each other -- an operator who forgets just ONE of the two flags would
// otherwise silently send that half of the preflight, and for
// `-buildinfo-url` specifically the effective-principal envelope too, to
// whatever happens to be listening on localhost:8090 (a port-forward to
// another environment, a stray local process, or on a shared host any
// unprivileged process bound to 127.0.0.1:8090) rather than a refusal.
// Python's identical CLI has ONE `--query-api-url` (or
// GO_API_QUERY_API_URL) serving BOTH reads and NO default at all --
// unset, it refuses outright.
//
// Executed against the pre-fix shape: two stub query-apis, one at the URL
// GO_API_QUERY_API_URL named (build aaaa...), one answering on a
// hardcoded localhost:8090 default (build cccc..., nothing "deployed"
// there). `enable` with no URL flags at all wrote `flowMatrix|cccc...`,
// mode=canary, exit=0 -- the row named a build the deployed process never
// ran, and the 8090 stub's log showed the effective-principal envelope
// reached IT (`/buildinfo auth=yes`), not the process
// GO_API_QUERY_API_URL pointed at.
//
// Deriving BOTH routes from the SAME env-var base closes the silent-
// default half of this. The other half: naming ONE route explicitly
// while GO_API_QUERY_API_URL is set to something ELSE also splits the
// two routes across different processes, silently, with no second flag
// named on the command line at all. Executed: `-registry-url <A>` with
// GO_API_QUERY_API_URL=<B> read the registry from A and sent the
// effective-principal envelope to, and took the build from, B -- `enable`
// wrote a row naming B's build under A's digests, exit 0. This low-level
// function is called ONLY through resolveQueryAPIEndpoints (below),
// which refuses that exact mix; it is kept as the single-route primitive
// `status` still uses (status has no /buildinfo route to split against).
func resolveEndpointURL(explicit, path string) (resolved string, ok bool) {
	if explicit != "" {
		return explicit, true
	}
	if base := strings.TrimSpace(os.Getenv(queryAPIURLEnvVar)); base != "" {
		return strings.TrimRight(base, "/") + path, true
	}
	return "", false
}

// resolveQueryAPIEndpoints resolves BOTH of query-api's routes together,
// for the two write verbs -- see resolveEndpointURL's
// doc comment for the executed repro. It refuses the one
// shape `resolveEndpointURL` alone cannot catch: ONE route named
// explicitly and the OTHER left to a DIFFERENT source (the env var, or
// nothing) -- that is not a deliberate two-URL override, it is a half-set
// flag quietly picking up whatever else is lying around in the
// environment. Only two shapes are accepted: BOTH routes explicit (a
// genuine, deliberate override -- an operator who typed two flags
// clearly meant it, even if they happen to name different processes),
// or NEITHER explicit (both derived from the SAME GO_API_QUERY_API_URL
// base, so they cannot diverge by construction).
func resolveQueryAPIEndpoints(explicitRegistry, explicitBuildInfo string) (registryURL, buildInfoURL string, err error) {
	switch {
	case explicitRegistry != "" && explicitBuildInfo != "":
		return explicitRegistry, explicitBuildInfo, nil
	case explicitRegistry == "" && explicitBuildInfo == "":
		var ok bool
		if registryURL, ok = resolveEndpointURL("", "/registry"); !ok {
			return "", "", refuse("no query-api URL: pass -registry-url and -buildinfo-url, or set %s. A measurement that did not happen is not a pass.", queryAPIURLEnvVar)
		}
		if buildInfoURL, ok = resolveEndpointURL("", "/buildinfo"); !ok {
			return "", "", refuse("no query-api URL: pass -registry-url and -buildinfo-url, or set %s. A measurement that did not happen is not a pass.", queryAPIURLEnvVar)
		}
		return registryURL, buildInfoURL, nil
	default:
		named, other := "-registry-url", "-buildinfo-url"
		if explicitBuildInfo != "" {
			named, other = "-buildinfo-url", "-registry-url"
		}
		return "", "", refuse("%s was named but %s was not: naming just one route lets the OTHER silently fall back to %s (or nothing) and split the preflight across two different processes. Name BOTH explicitly, or neither (let %s serve both).",
			named, other, queryAPIURLEnvVar, queryAPIURLEnvVar)
	}
}

// stdout and stderr are where this command writes.
//
// Package variables rather than os.Stdout / os.Stderr reached directly,
// so a test can drive a REAL verb through `run(argv)` and read exactly
// what an operator would see. r1's P3 is the reason: three separate
// mutations -- the DSN-leaking flag default restored only inside
// `runEnable`, the schema-agreement preflight disabled, the
// enabled_unproven WARNING suppressed -- all SURVIVED both package
// suites, because nothing in the suite ever ran a verb and read its
// output. A guard is pinned where it is installed or it is not pinned.
var (
	stdout io.Writer = os.Stdout
	stderr io.Writer = os.Stderr
)

// verbFlagOutput is where a verb's flag set writes its usage text.
//
// A package variable rather than a parameter so a test can drive the REAL
// verb -- `run([]string{"enable", "-h"})` -- and read what a real operator
// would see. That is the whole point: r1's M10 restored the DSN-leaking
// default in `runEnable`'s OWN flag set and BOTH package suites stayed
// green, because the usage test built a flag set of its own with the same
// arguments and so only ever proved that the test agrees with itself.
// A guard is only pinned where it is actually installed.
var verbFlagOutput io.Writer = os.Stderr //nolint:gochecknoglobals // see stdout/stderr above

// newVerbFlagSet builds a verb's flag set.
//
// ContinueOnError, not ExitOnError, and that is a fix rather than a
// convenience. ExitOnError calls os.Exit(2) from inside `flag`, which
// (a) makes the verb's own exit-code classification unreachable on a
// parse error -- the 2 it produces is `flag`'s constant, right by
// coincidence rather than by this command's rule -- and (b) makes the
// real parser untestable, since any test that drove it would kill the
// test binary. Both halves of r1's M10 finding follow from that.
func newVerbFlagSet(name string) *flag.FlagSet {
	set := flag.NewFlagSet(name, flag.ContinueOnError)
	set.SetOutput(verbFlagOutput)
	return set
}

// parseVerbFlags parses a verb's argv and REFUSES ANY UNCONSUMED
// ARGUMENT.
//
// This is r1's first P1, and it is the worst defect this command has had.
// Go's `flag` stops at the first non-flag operand and leaves the REST in
// `set.Args()` -- unparsed, unreported, silently ignored. So a stray word
// anywhere in a command line silently deletes every flag after it,
// including the ones whose entire job is to prevent a write. Executed
// against a real database, on a row that was `python`:
//
//	$ go-api-routing disable -operations flowMatrix -mode shadow -apply //	    -recorded-by lane -review-evidence why //	    UNEXPECTED-OPERAND -candidate-build 0000000000000000000000000000000000000000
//	applied: 1 row(s) now mode=shadow
//	exit=0
//	SELECT ... -> flowMatrix|shadow
//
// The same `-candidate-build` guard, WITHOUT the stray word, refuses that
// exact command and writes nothing. The operand deleted the guard, the
// verb reported success, and the row moved. The same shape drops
// `-dry-run` from `enable` (a "write nothing" run that writes),
// `-expect-build` from `repoint`, and `-json` from `status`.
//
// It also swallows the most likely spelling mistake on this surface:
// `flag` needs `-acknowledge-unproven=false`, so `-acknowledge-unproven
// false` makes `false` an OPERAND -- an operator trying to turn the
// acknowledgement OFF turns it on and loses every later flag too.
//
// Refused for EVERY verb, before any preflight and before anything is
// read, so a mistyped command line cannot reach the database at all.
// Named in full, because "unexpected argument" without the word tells an
// operator nothing about which word broke their command.
func parseVerbFlags(set *flag.FlagSet, argv []string) error {
	if err := set.Parse(argv); err != nil {
		// `-h`/`-help` on any of the four verbs must never
		// fall through to the refuse() below like any other malformed flag
		// -- falling through there would print the usage text (via
		// ContinueOnError) and then STILL exit 2, "refused: flag: help
		// requested". That would be inconsistent with THIS binary's own top-level
		// `-h` (run's "help" case, above: prints usage, returns nil, exit
		// 0) and with Python's `-h` (argparse: exit 0). A script piping
		// `<verb> -h --help-only` into a "did it work" check saw a refusal
		// for asking what a flag does. `flag.ErrHelp` is the one Parse
		// error that is not a mistake -- the usage text it already printed
		// (ContinueOnError) IS the requested output, so this returns
		// errHelpRequested (see its own doc comment) rather than wrapping
		// it as a refusal: the verb still stops HERE, but run()'s
		// helpAsSuccess turns it into a plain nil before any caller sees
		// it.
		if errors.Is(err, flag.ErrHelp) {
			return errHelpRequested
		}
		// ContinueOnError has already written the usage text; this turns
		// `flag`'s error into THIS command's classification rather than
		// letting the process exit with `flag`'s own constant.
		return refuse("%v", err)
	}
	if set.NArg() > 0 {
		return refuse("unexpected argument(s) after the flags: %q.\n"+
			"  Go's flag parser STOPS at the first one, so every flag after it was silently ignored -- including guards like -candidate-build, -expect-build and -dry-run.\n"+
			"  If you meant to turn a boolean off, spell it -flag=false (one word, with the equals sign).",
			set.Args())
	}
	return nil
}

// requirePositiveTimeout refuses a -timeout that switches its own bound
// OFF.
//
// Executed evidence (CHAOS-5486's input-domain sweep, through the real
// binary): `-timeout 0` and `-timeout -1s` were both ACCEPTED, exit 0.
// `http.Client` treats a non-positive Timeout as "no timeout at all", and
// `status`'s whole contract is that a blackholed endpoint must make
// this verb say "unreachable" rather than hang forever -- so the two
// values an operator is most likely to type when they mean "don't wait"
// are exactly the two that mean "wait indefinitely". A flag whose value
// silently disables the guard the flag exists to set is the same
// two-states-one-silence shape the rest of this command refuses.
//
// A DSN dial with a non-positive timeout fails the opposite way: the
// context deadline is already past, so the dial cannot succeed at all.
// One value, two opposite wrong behaviours on the two legs, neither of
// them what was typed. Refused on every verb, including `status`, which
// never refuses on an unhealthy STATE but does refuse on an unusable
// FLAG.
func (c *commonFlags) requirePositiveTimeout() error {
	if c.timeout <= 0 {
		return refuse("-timeout must be greater than zero (got %s): a non-positive value does not mean \"do not wait\", it means the HTTP leg waits forever and the Postgres dial cannot succeed at all", c.timeout)
	}
	return nil
}

func (c *commonFlags) requirePostgres() error {
	c.resolvePostgresURI()
	if c.postgresURI == "" {
		return refuse("required flag is missing: -postgres-uri (or %s)", postgresURIEnvVar)
	}
	return nil
}

// envelopeCredential reads the ENVELOPE from its named environment
// variable and hands back the credential every /buildinfo read carries.
//
// The env-var check stays here rather than being left to the credential's
// own empty-value refusal, because the two failures are different
// sentences to an operator: "you never set GO_API_ROUTING_BEARER" is a
// setup mistake with a named fix, where the credential's refusal at the
// moment of use reads as a transport problem. Both floors are kept.
//
// STATIC rather than minted, and that is a CONDITION on this file: each
// verb reads /buildinfo exactly ONCE, before it opens Postgres, so a
// value captured here cannot go stale mid-run. If a second /buildinfo
// read is ever added to `enable` or `repoint`, this must become a minted
// credential with a refresh, and a static one becomes a defect.
//
// CHAOS-5505 does NOT widen this to also return the raw bearer: the
// credential's whole design (internal/goapiproof/credential.go) is that
// its value is asked for per request and never handed back to a caller.
// A verb that needs the envelope's `sub` for its audit row calls
// Credential.EnvelopeSubject, which stays inside package goapiproof --
// the raw token still never reaches cmd/go-api-routing.
func envelopeCredential() (*goapiproof.Credential, error) {
	bearer := os.Getenv(bearerEnvVar)
	if bearer == "" {
		return nil, refuse("no credential: set %s to an effective-principal ENVELOPE, which is what /buildinfo checks (the VALUE is never printed by this command)", bearerEnvVar)
	}
	return buildInfoCredential(bearer), nil
}

func httpClient(timeout time.Duration) *http.Client { return &http.Client{Timeout: timeout} }

// ErrURLCarriesCredentials refuses a -registry-url or -buildinfo-url that
// carries anything a credential can hide in.
//
// THREE ROUNDS FOUND THREE VARIANTS OF ONE LEAK, and the third is why
// this is now an ALLOWLIST rather than a list of things to reject:
//
//  1. r3 SEC-01: `http://alice:secret@host/registry` -- userinfo, printed
//     in full by `goapiproof.FetchRegistry`'s error, which interpolates
//     the URL it was handed. Go's `url.Error` masks the password in the
//     NESTED error, so the redacted copy sat beside the un-redacted one.
//  2. The no-`//` form (lane-5425-prove, measured): `url.Parse` reads the
//     USERNAME as the scheme, `User` is nil, and `Redacted()` returns the
//     password verbatim. The first gate here refused it -- and named the
//     offending scheme, which IS the username.
//  3. r4 CRED-01: `http://host/registry?token=secret` -- a userinfo-only
//     check accepts it, and it reaches the same interpolation.
//
// Each fix rejected the shape that had just been found and left the next
// one open, because "where can a credential hide in a URL" is not a
// question with an enumerable answer. go_api_cli.py reached the same
// conclusion after five rounds and its `_endpoint_label` records it:
// redaction is the wrong operation; rebuild the safe parts instead.
//
// So: these routes take NO userinfo, NO query and NO fragment -- they are
// two fixed GET endpoints authenticated by a header. Anything carrying
// one is refused, and what is passed onward is REBUILT from scheme, host
// and path rather than forwarded as the operator typed it.
var ErrURLCarriesCredentials = errors.New("the URL carries something a credential can hide in")

// sanitizeEndpointURL refuses an endpoint URL that could leak and returns
// the REBUILT one to use in its place.
//
// It never echoes any part of the input -- not even the scheme, because
// on the no-`//` form the scheme is the username (see variant 2 above).
func sanitizeEndpointURL(flagName, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)

	// Checked BEFORE the parser and deliberately without it: an `@` ahead
	// of the first `/` is userinfo in every URL shape, whatever Go makes
	// of it. The parser is the thing that surprised us, so the first gate
	// does not consult it.
	authority := trimmed
	if slash := strings.Index(authority, "/"); slash >= 0 {
		authority = authority[:slash]
	}
	if strings.Contains(authority, "@") {
		return "", refuse("%w: %s carries userinfo. These routes authenticate with the effective-principal envelope in %s, and a URL credential would be printed verbatim by any transport error on the way out",
			ErrURLCarriesCredentials, flagName, bearerEnvVar)
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", refuse("%s could not be parsed as a URL. Its text is deliberately not echoed -- it may carry a credential", flagName)
	}
	switch {
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		// The scheme is NOT reported: on a URL with no "//" it IS the
		// username.
		return "", refuse(`%s must be http:// or https://. Its text is deliberately not echoed -- on a URL with no "//" Go parses the USERNAME as the scheme, so naming it would print a credential`, flagName)
	case parsed.Opaque != "":
		return "", refuse("%s is not an absolute URL with a host. Its text is deliberately not echoed", flagName)
	case parsed.Host == "":
		return "", refuse("%s names no host", flagName)
	case parsed.User != nil:
		return "", refuse("%w: %s carries userinfo. These routes authenticate with the effective-principal envelope in %s",
			ErrURLCarriesCredentials, flagName, bearerEnvVar)
	case parsed.RawQuery != "" || parsed.ForceQuery:
		return "", refuse("%w: %s carries a query string. GET /registry and GET /buildinfo take no parameters, so a query can only be carrying something -- and it would be printed verbatim by any transport error",
			ErrURLCarriesCredentials, flagName)
	case parsed.Fragment != "":
		return "", refuse("%w: %s carries a fragment. It is never sent to the server, so it can only be carrying something -- and it would still be printed by a transport error",
			ErrURLCarriesCredentials, flagName)
	}

	// The HOST is rebuilt from VALIDATED PIECES, not carried across.
	//
	// The confirmation pass found the hole the first rebuild left: an IPv6
	// ZONE IDENTIFIER, `http://[fe80::1%25zone-secret]:8090/registry`.
	// Every component check above passes -- scheme http, no userinfo, no
	// query, no fragment -- and `parsed.Host` carries the zone text
	// through the rebuild untouched, into the error that interpolates it.
	//
	// The lesson, and the reason this is not just one more shape struck
	// off a list: the first rebuild reconstructed the STRUCTURE of the URL
	// but copied each component's CONTENTS verbatim. A component you copy
	// is a component you have not validated. So the host is now taken
	// apart and put back together from a hostname that must be an IP
	// literal or a DNS name, and a port that must be digits.
	//
	// A zone identifier is meaningless here regardless: it names a local
	// interface for a link-local address, which cannot be the address of a
	// deployed query-api.
	host := parsed.Hostname()
	if host == "" {
		return "", refuse("%s names no host", flagName)
	}
	if port := parsed.Port(); port != "" {
		for _, r := range port {
			if r < '0' || r > '9' {
				return "", refuse("%s has a non-numeric port. Its text is deliberately not echoed", flagName)
			}
		}
		// A port outside 1..65535 cannot be dialled, so accepting it only
		// defers the failure to a place with less context. Not a leak
		// vector -- digits carry nothing -- but a validated component
		// should be validated, not merely character-checked.
		if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
			return "", refuse("%s has a port outside 1..65535", flagName)
		}
	}
	// Two behaviours of this reassembly that are deliberate, recorded here
	// because both were found by probing rather than designed, and an
	// undocumented deliberate choice is indistinguishable from an
	// oversight:
	//
	//   - An IPv4-mapped IPv6 literal is NORMALISED: `[::ffff:127.0.0.1]`
	//     comes back as `127.0.0.1`. Same address, one spelling. The URL
	//     used is then not byte-identical to the one typed, which is the
	//     point of a rebuild.
	//   - A non-ASCII host is REFUSED rather than punycoded. Go's HTTP
	//     client does not IDNA-encode a host, so a Unicode one would fail
	//     to dial anyway; refusing it also means a homoglyph cannot carry
	//     text into an error. The deployed query-api is reached at
	//     `localhost`, a container IP or an internal DNS name, none of
	//     which are IDN. Pass punycode (`xn--...`) if that ever changes.
	rebuiltHost := ""
	if ip := net.ParseIP(host); ip != nil {
		if strings.Contains(host, "%") {
			// net.ParseIP rejects a zone, so reaching here with one would
			// mean the parser changed underneath us. Belt to the check
			// below's braces.
			return "", refuse("%w: %s carries an IPv6 zone identifier, which names a local interface and cannot address a deployed service",
				ErrURLCarriesCredentials, flagName)
		}
		rebuiltHost = ip.String()
		if ip.To4() == nil {
			rebuiltHost = "[" + rebuiltHost + "]"
		}
	} else {
		if strings.Contains(host, "%") {
			return "", refuse("%w: %s carries a zone identifier or percent-escape in its host, which cannot address a deployed service",
				ErrURLCarriesCredentials, flagName)
		}
		for _, r := range host {
			isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
			isDigit := r >= '0' && r <= '9'
			if !isLetter && !isDigit && r != '.' && r != '-' {
				return "", refuse("%s is not a hostname or IP literal. Its text is deliberately not echoed -- anything it carries would be printed by a transport error", flagName)
			}
		}
		rebuiltHost = host
	}
	if port := parsed.Port(); port != "" {
		rebuiltHost += ":" + port
	}

	// REBUILT from validated components, never forwarded as typed.
	// Anything this function did not explicitly account for cannot survive
	// into the value that reaches the code which interpolates it.
	safe := url.URL{Scheme: parsed.Scheme, Host: rebuiltHost, Path: parsed.Path}
	return safe.String(), nil
}

// connectPostgres applies the command's own -timeout to CONNECTING, which
// a bare context.Background() would not.
//
// pgxpool.New does not dial; the first Acquire does. So a blackholed
// database left `status` -- the one verb whose whole contract is that it
// always answers -- hanging indefinitely, and the write verbs hanging
// before they had done anything. The returned context bounds the
// connection attempt only; it is cancelled once the pool is live, so the
// work that follows is not cut short by a flag meant for a dial.
func connectPostgres(ctx context.Context, uri string, timeout time.Duration) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		// A malformed DSN, which the operator fixes (r2 R2-01). It reads
		// as a parse error from pgx, so it used to fall through to the
		// crash default.
		//
		// pgx's own message EMBEDS THE DSN (`cannot parse \`...\``), and a
		// real DSN carries a password -- so the underlying error is
		// deliberately NOT wrapped here. Same rule go_api_cli.py's
		// `_transport_failure` follows for URLs: report the class, never
		// the string. Caught while verifying this very fix; the first
		// version of it wrapped %w and would have leaked the password of
		// any DSN malformed enough to fail parsing.
		return nil, refuse("the -postgres-uri could not be parsed as a Postgres DSN. Its text is deliberately not echoed -- it carries a password")
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(dialCtx); err != nil {
		pool.Close()
		// The dial error names host/port, not the password, and an
		// operator needs to know WHICH endpoint went dark -- so this one
		// IS wrapped. Asserted by TestConnectPostgresBoundsTheDialAndSaysSo.
		//
		// Exit 1, not 2, matching Python's identical command (an unhandled
		// `ConnectionRefusedError`, exit 1): a
		// syntactically valid DSN naming an endpoint that will not answer
		// is not something the operator TYPED wrong (that is the parse
		// error above, still exit 2) -- it is the database itself being
		// unreachable, the same class of failure `classifyWriteError`
		// already exits 1 for once inside a write transaction. `status`
		// is unaffected: it catches this error and reports it as
		// `registry_db_error`, never propagating it as an exit code, so
		// this classification only affects enable/disable/repoint.
		return nil, internal("Postgres did not answer within %s: %w", timeout, err)
	}
	return pool, nil
}

// requestedOperations forwards to the package parser so the four verbs
// share ONE definition of what an --operations value means. Kept as a
// named function here because this command's own tests pin its
// behaviour, and because a second copy of the rule is exactly what
// this exists to prevent.
func requestedOperations(raw string) ([]string, error) {
	return goapiproof.SplitOperations(raw)
}

var errEmptyOperationFilter = goapiproof.ErrEmptyOperationFilter
