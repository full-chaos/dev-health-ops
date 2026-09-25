package synccli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The `sync <target>` verbs (git, prs, blame, cicd, deployments, incidents,
// security, tests) are the Go home of `dev-hops sync <target>`. This file is
// the request layer they share: the flags, the credential-shape rules, the
// sink/window/limit resolution and the exit-code contract of the Python CLI,
// producing a Plan. Running a Plan is the Executor's job; the executors that
// call the provider-unit routes land per provider, and until one is wired a
// verb REFUSES (exit 3, nothing written) rather than pretending to sync.
//
// Everything here is proven against the real Python producer -- argparse, the
// preflight, and run_sync_target with only its I/O seams replaced -- by the
// live oracle in live_python_oracle_test.go.

// Targets is the sync target list, in dev-hops' order.
var Targets = []string{"git", "prs", "blame", "cicd", "deployments", "incidents", "security", "tests"}

var targetSummaries = map[string]string{
	"git":         "sync commits and commit stats",
	"prs":         "sync pull/merge requests",
	"blame":       "sync blame data only",
	"cicd":        "sync CI/CD runs and pipelines",
	"deployments": "sync deployments",
	"incidents":   "sync incidents",
	"security":    "sync security and dependency alerts",
	"tests":       "sync CI test results and coverage (TestOps)",
}

// Calls name what a Plan asks the executor to run, mirroring the Python
// processor entry points.
const (
	CallLocalRepo    = "local_repo"
	CallLocalBlame   = "local_blame"
	CallGitHubSingle = "github_single"
	CallGitHubBatch  = "github_batch"
	CallGitLabSingle = "gitlab_single"
	CallGitLabBatch  = "gitlab_batch"
	CallSynthetic    = "synthetic"
)

// Org sources: where the organization id came from.
const (
	OrgFromFlag    = "flag"
	OrgFromEnv     = "env"
	OrgFromDBFirst = "db-first" // none given: the executor resolves the first org in Postgres
)

// Credential modes of a GitHub plan.
const (
	CredentialPAT = "pat"
	CredentialApp = "app"
	CredentialDB  = "db" // none on the command line or env: the executor looks them up for --org
)

// GitHubCredentials is the credential a GitHub plan carries. Values are
// secrets: they are never printed.
type GitHubCredentials struct {
	Mode           string
	Name           string // "cli" or "environment" ("" for the database lookup)
	BaseURL        *string
	Token          string
	AppID          string
	PrivateKey     string
	InstallationID string
}

// Plan is one validated `sync <target>` request.
type Plan struct {
	Target   string
	Provider string
	Call     string

	SinkURI   string
	Org       *string
	OrgSource string
	DB        *string

	Since      *time.Time
	MaxCommits *int64

	SyncGit, SyncPrs, SyncCICD, SyncDeployments   bool
	SyncIncidents, SyncSecurity, SyncTests, Blame bool

	RepoPath string

	Owner, Repo string
	ProjectID   *int64
	GitLabURL   string
	GitLabToken string

	Group          *string
	Search         string
	BatchSize      int64
	MaxConcurrent  int64
	RateLimitDelay float64
	MaxRepos       *int64
	UseAsync       bool

	GitHub *GitHubCredentials
}

// Refusal is a request the Python CLI also refuses. Code is the exit code:
// 2 for a usage error (argparse and the preflight), 1 for SystemExit(message).
type Refusal struct {
	Code    int
	Message string
	// Type names the uncaught Python exception (Stage "error").
	Type string
	// Stage is where Python refuses: "argparse", "preflight" or "exit"
	// (a SystemExit(message) inside run_sync_target).
	Stage string
}

func (r *Refusal) Error() string { return r.Message }

// Executor runs a validated Plan. Its error is the verb's failure: a Refusal
// keeps its code, anything else is a runtime failure (exit 1).
type Executor func(ctx context.Context, plan Plan, env cli.Env) error

// ErrNotAvailable is what the default executor returns: the target/provider
// pair has no Go executor wired yet.
var ErrNotAvailable = errors.New("not available in dho yet")

func notAvailableExecutor(_ context.Context, plan Plan, _ cli.Env) error {
	return fmt.Errorf("dho sync %s --provider %s is %w; run dev-hops sync %s meanwhile", plan.Target, plan.Provider, ErrNotAvailable, plan.Target)
}

var syncSpecs = []optSpec{
	{strs: []string{"-h", "--help"}, dest: "help", kind: optHelp},
	{strs: []string{"--sink"}, dest: "sink", kind: optValue},
	{strs: []string{"--provider"}, dest: "provider", kind: optValue},
	{strs: []string{"--auth"}, dest: "auth", kind: optValue},
	{strs: []string{"--github-app-id"}, dest: "github_app_id", kind: optValue},
	{strs: []string{"--github-app-key-path"}, dest: "github_app_key_path", kind: optValue},
	{strs: []string{"--github-app-installation-id"}, dest: "github_app_installation_id", kind: optValue},
	{strs: []string{"--repo-path"}, dest: "repo_path", kind: optValue},
	{strs: []string{"--owner"}, dest: "owner", kind: optValue},
	{strs: []string{"--repo"}, dest: "repo", kind: optValue},
	{strs: []string{"--project-id"}, dest: "project_id", kind: optValue},
	{strs: []string{"--gitlab-url"}, dest: "gitlab_url", kind: optValue},
	{strs: []string{"--group"}, dest: "group", kind: optValue},
	{strs: []string{"-s", "--search"}, dest: "search", kind: optValue},
	{strs: []string{"--batch-size"}, dest: "batch_size", kind: optValue},
	{strs: []string{"--max-concurrent"}, dest: "max_concurrent", kind: optValue},
	{strs: []string{"--rate-limit-delay"}, dest: "rate_limit_delay", kind: optValue},
	{strs: []string{"--max-repos"}, dest: "max_repos", kind: optValue},
	{strs: []string{"--use-async"}, dest: "use_async", kind: optFlag},
	{strs: []string{"--max-commits-per-repo"}, dest: "max_commits_per_repo", kind: optValue},
	{strs: []string{"--repo-name"}, dest: "repo_name", kind: optValue},
	{strs: []string{"--defer-finalize"}, dest: "defer_finalize", kind: optFlag},
	{strs: []string{"--since"}, dest: "since", kind: optValue},
	{strs: []string{"--backfill"}, dest: "backfill", kind: optValue},
	{strs: []string{"--before"}, dest: "before", kind: optValue},
	{strs: []string{"--day"}, dest: "day", kind: optValue},
	{strs: []string{"--date"}, dest: "date", kind: optValue},
	// dev-hops re-adds its global flags on every leaf.
	{strs: []string{"--log-level"}, dest: "log_level", kind: optValue},
	{strs: []string{"--db"}, dest: "db", kind: optValue},
	{strs: []string{"--analytics-db"}, dest: "analytics_db", kind: optValue},
	{strs: []string{"--org"}, dest: "org", kind: optValue},
	{strs: []string{"-l", "--llm-provider"}, dest: "llm_provider", kind: optValue},
	{strs: []string{"-m", "--model"}, dest: "model", kind: optValue},
}

var providers = []string{"local", "github", "gitlab", "synthetic"}

func usageRefusal(stage, format string, args ...any) *Refusal {
	return &Refusal{Code: cli.ExitUsage, Message: fmt.Sprintf(format, args...), Stage: stage}
}

func exitRefusal(message string) *Refusal {
	return &Refusal{Code: cli.ExitFailure, Message: message, Stage: "exit"}
}

// Lookup reads one environment variable the way os.LookupEnv does.
type Lookup func(string) (string, bool)

// KeyReader reads a GitHub App private key file.
type KeyReader func(path string) (string, error)

// Now is the clock the default --before is computed from.
type Now func() time.Time

// Inputs are the outside facts a request depends on, replaceable in tests.
type Inputs struct {
	Lookup  Lookup
	ReadKey KeyReader
	Now     Now
}

func (in Inputs) env(name string) (string, bool) {
	if in.Lookup == nil {
		return "", false
	}
	return in.Lookup(name)
}

func (in Inputs) envOr(name string) string {
	value, _ := in.env(name)
	return value
}

// argAct converts each option value the way argparse does, at the moment the
// option is consumed, and reports the since/backfill conflict.
type argState struct {
	sinceGiven    bool
	backfillGiven bool
}

func typeError(spec *optSpec, value string) *argError {
	name := spec.strs[len(spec.strs)-1]
	kinds := map[string]string{
		"project_id": "int", "batch_size": "int", "max_concurrent": "int", "max_repos": "int",
		"max_commits_per_repo": "int", "backfill": "int", "rate_limit_delay": "float",
		"since": "date", "before": "date", "day": "date", "date": "date",
	}
	return &argError{fmt.Sprintf("argument %s: invalid %s value: %q", name, kinds[spec.dest], value)}
}

func convert(state *argState) func(spec *optSpec, value string) *argError {
	return func(spec *optSpec, value string) *argError {
		switch spec.dest {
		case "project_id", "batch_size", "max_concurrent", "max_repos", "max_commits_per_repo":
			if _, ok := pyInt(value); !ok {
				return typeError(spec, value)
			}
		case "backfill":
			n, ok := pyInt(value)
			if !ok {
				return typeError(spec, value)
			}
			_ = n
			// Any explicit --backfill conflicts with --since, even one equal
			// to the default (argparse 3.14: the value is a fresh object).
			state.backfillGiven = true
			if state.sinceGiven {
				return &argError{"argument --backfill: not allowed with argument --since"}
			}
		case "rate_limit_delay":
			if _, ok := pyFloat(value); !ok {
				return typeError(spec, value)
			}
		case "since":
			if _, ok := pyDate(value); !ok {
				return typeError(spec, value)
			}
			state.sinceGiven = true
			if state.backfillGiven {
				return &argError{"argument --since: not allowed with argument --backfill"}
			}
		case "before", "day", "date":
			if _, ok := pyDate(value); !ok {
				return typeError(spec, value)
			}
		case "provider":
			for _, p := range providers {
				if value == p {
					return nil
				}
			}
			return &argError{fmt.Sprintf("argument --provider: invalid choice: %q (choose from local, github, gitlab, synthetic)", value)}
		}
		return nil
	}
}

func isNonEmpty(values map[string]string, key string) bool { return values[key] != "" }

// BuildPlan validates one `sync <target>` command line exactly as dev-hops
// does and returns the Plan, or a *Refusal carrying the exit code Python
// exits with. help reports a -h/--help request (Python prints help, exit 0).
func BuildPlan(target string, args []string, in Inputs) (plan Plan, help bool, err *Refusal) {
	state := &argState{}
	parsed, perr := newParser(syncSpecs).parse(args, convert(state))
	if perr != nil {
		return Plan{}, false, usageRefusal("argparse", "%s", perr.msg)
	}
	if parsed.help {
		return Plan{}, true, nil
	}
	if _, ok := parsed.values["provider"]; !ok {
		return Plan{}, false, usageRefusal("argparse", "the following arguments are required: --provider")
	}
	if len(parsed.unrecognized) > 0 {
		return Plan{}, false, usageRefusal("argparse", "unrecognized arguments: %s", strings.Join(parsed.unrecognized, " "))
	}
	v := parsed.values

	// main(): --org typed wins, else ORG_ID (set-but-empty counts as set),
	// else the executor resolves the first organization in Postgres.
	var org *string
	orgSource := OrgFromDBFirst
	if value, ok := v["org"]; ok {
		org, orgSource = &value, OrgFromFlag
	} else if value, ok := in.env("ORG_ID"); ok {
		org, orgSource = &value, OrgFromEnv
	}

	// Preflight: a ClickHouse URI from --analytics-db or CLICKHOUSE_URI, with
	// a supported scheme.
	sinkURI := firstNonEmpty(v["analytics_db"], in.envOr("CLICKHOUSE_URI"))
	if sinkURI == "" {
		return Plan{}, false, usageRefusal("preflight", "missing required input(s): ClickHouse analytics database: pass --analytics-db or set CLICKHOUSE_URI")
	}
	if scheme := urlScheme(sinkURI); !clickhouseScheme(scheme) {
		return Plan{}, false, usageRefusal("preflight", "Unknown or unsupported sink scheme '%s'. Only ClickHouse is supported.", scheme)
	}

	providerName := strings.ToLower(v["provider"])
	plan = Plan{Target: target, Provider: providerName, SinkURI: sinkURI, Org: org, OrgSource: orgSource}
	plan.DB = dbValue(v, in)

	switch providerName {
	case "local":
		if target != "git" && target != "prs" && target != "blame" {
			return Plan{}, false, exitRefusal("Local provider supports only git, prs, or blame targets.")
		}
	case "github":
		creds, cerr := githubCredentials(v, plan, in)
		if cerr != nil {
			return Plan{}, false, cerr
		}
		plan.GitHub = creds
	case "gitlab":
		token := firstNonEmpty(v["auth"], in.envOr("GITLAB_TOKEN"))
		if token == "" {
			return Plan{}, false, exitRefusal("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")
		}
		plan.GitLabToken = token
	case "synthetic":
		// Synthetic git/prs/blame has no Go path (undecided) and the other
		// targets are `dho fixtures load-synthetic`: the request is valid as
		// far as the shared flags go, and the executor decides.
		plan.Call = CallSynthetic
		return plan, false, nil
	}

	if serr := validateSink(v["sink"], v); serr != nil {
		return Plan{}, false, serr
	}
	if !clickhouseDBType(sinkURI) {
		// detect_db_type raises ValueError: an uncaught traceback, exit 1.
		return Plan{}, false, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: "ValueError", Message: "Could not detect database type from the ClickHouse connection string (ValueError)"}
	}
	since, backfill, overflow := resolveWindow(v, in)
	if overflow {
		return Plan{}, false, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: "OverflowError", Message: "date value out of range (OverflowError)"}
	}
	plan.Since = since
	plan.MaxCommits = resolveMaxCommits(v, since != nil || backfill > 1)

	fillFlags(&plan)
	if rerr := fillMode(&plan, v, in); rerr != nil {
		return Plan{}, false, rerr
	}
	return plan, false, nil
}

// dbValue is ns.db: the leaf --db when typed, else the root default
// `POSTGRES_URI or DATABASE_URI` (nil when neither is set).
func dbValue(v map[string]string, in Inputs) *string {
	if db, ok := v["db"]; ok {
		return &db
	}
	if pg := in.envOr("POSTGRES_URI"); pg != "" {
		return &pg
	}
	if db, ok := in.env("DATABASE_URI"); ok {
		return &db
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func fillFlags(plan *Plan) {
	t := plan.Target
	plan.SyncGit, plan.SyncPrs = t == "git", t == "prs"
	plan.SyncCICD, plan.SyncDeployments = t == "cicd", t == "deployments"
	plan.SyncIncidents, plan.SyncSecurity = t == "incidents", t == "security"
	plan.SyncTests, plan.Blame = t == "tests", t == "blame"
}

func optionalInt(v map[string]string, key string) *int64 {
	text, ok := v[key]
	if !ok {
		return nil
	}
	n, _ := pyInt(text)
	return &n
}

func intOr(v map[string]string, key string, def int64) int64 {
	if n := optionalInt(v, key); n != nil {
		return *n
	}
	return def
}

// fillMode is the per-provider handler of run_sync_target: which processor a
// request reaches and with what.
func fillMode(plan *Plan, v map[string]string, in Inputs) *Refusal {
	switch plan.Provider {
	case "local":
		plan.RepoPath = valueOr(v, "repo_path", ".")
		if plan.Target == "blame" {
			plan.Call = CallLocalBlame
		} else {
			plan.Call = CallLocalRepo
		}
	case "github":
		plan.Search = v["search"]
		if plan.Search != "" {
			plan.Call = CallGitHubBatch
			plan.Group = optionalString(v, "group")
			plan.Owner = v["owner"]
			fillBatch(plan, v)
			return nil
		}
		if v["owner"] == "" || v["repo"] == "" {
			return exitRefusal("GitHub sync requires --owner and --repo (or --search for batch).")
		}
		plan.Call, plan.Owner, plan.Repo = CallGitHubSingle, v["owner"], v["repo"]
	case "gitlab":
		plan.GitLabURL = valueOr(v, "gitlab_url", envDefault(in, "GITLAB_URL", "https://gitlab.com"))
		plan.Search = v["search"]
		plan.Group = optionalString(v, "group")
		if plan.Search != "" {
			plan.Call = CallGitLabBatch
			fillBatch(plan, v)
			return nil
		}
		plan.ProjectID = optionalInt(v, "project_id")
		if plan.ProjectID == nil {
			return exitRefusal("GitLab sync requires --project-id (or --search for batch).")
		}
		plan.Call = CallGitLabSingle
	}
	return nil
}

func fillBatch(plan *Plan, v map[string]string) {
	plan.BatchSize = intOr(v, "batch_size", 10)
	plan.MaxConcurrent = intOr(v, "max_concurrent", 4)
	plan.RateLimitDelay = 1.0
	if text, ok := v["rate_limit_delay"]; ok {
		plan.RateLimitDelay, _ = pyFloat(text)
	}
	plan.MaxRepos = optionalInt(v, "max_repos")
	_, plan.UseAsync = v["use_async"]
}

func optionalString(v map[string]string, key string) *string {
	if value, ok := v[key]; ok {
		return &value
	}
	return nil
}

// envDefault is os.getenv(name, default): a set-but-empty variable is empty.
func envDefault(in Inputs, name, def string) string {
	if value, ok := in.env(name); ok {
		return value
	}
	return def
}

func valueOr(v map[string]string, key, def string) string {
	if value, ok := v[key]; ok {
		return value
	}
	return def
}

// validateSink is utils.cli.validate_sink.
func validateSink(raw string, v map[string]string) *Refusal {
	sink := "clickhouse"
	if _, ok := v["sink"]; ok && raw != "" {
		sink = raw
	}
	sink = strings.ToLower(strings.TrimFunc(sink, isPySpace))
	switch sink {
	case "mongo", "sqlite", "postgres", "both":
		return exitRefusal(fmt.Sprintf("Backend '%s' is no longer supported for analytics. "+
			"ClickHouse is the only supported analytics backend. "+
			"Set CLICKHOUSE_URI and use --sink clickhouse (or omit --sink).", sink))
	case "clickhouse", "auto":
		return nil
	}
	return exitRefusal(fmt.Sprintf("Unknown sink '%s'. Only 'clickhouse' is supported.", sink))
}

// isPySpace is str.isspace's set: unicode White_Space plus the ASCII
// separators U+001C..U+001F.
func isPySpace(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }

// pyAddDays is date + timedelta(days=n): ok=false where Python raises
// OverflowError (a result outside years 1..9999, or |n| beyond timedelta).
func pyAddDays(t time.Time, n int64) (time.Time, bool) {
	const maxSpan = 3652059 // days between 0001-01-01 and 9999-12-31
	if n > maxSpan || n < -maxSpan {
		return time.Time{}, false
	}
	result := t.AddDate(0, 0, int(n))
	if result.Year() < 1 || result.Year() > 9999 {
		return time.Time{}, false
	}
	return result, true
}

// resolveWindow is utils.cli.resolve_since_datetime, with the deprecated
// --day/--date translated into --before as _handle_deprecated_day_flag does
// (--day wins over --date; an explicit --before wins over both). overflow
// reports the OverflowError Python raises for a date outside years 1..9999.
func resolveWindow(v map[string]string, in Inputs) (since *time.Time, backfill int64, overflow bool) {
	backfill = intOr(v, "backfill", 1)
	var before *time.Time
	if text, ok := v["before"]; ok {
		t, _ := pyDate(text)
		before = &t
	}
	deprecated, hasDeprecated := v["day"]
	if !hasDeprecated {
		deprecated, hasDeprecated = v["date"]
	}
	if hasDeprecated && before == nil {
		t, _ := pyDate(deprecated)
		next, ok := pyAddDays(t, 1)
		if !ok {
			return nil, backfill, true
		}
		before = &next
	}
	if text, ok := v["since"]; ok {
		t, _ := pyDate(text)
		return &t, backfill, false
	}
	if backfill > 1 {
		if before == nil {
			now := in.Now
			if now == nil {
				now = time.Now
			}
			today := now().UTC()
			tomorrow := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
			before = &tomorrow
		}
		start, ok := pyAddDays(*before, -backfill)
		if !ok {
			return nil, backfill, true
		}
		return &start, backfill, false
	}
	return nil, backfill, false
}

// resolveMaxCommits is utils.cli.resolve_max_commits.
func resolveMaxCommits(v map[string]string, hasDateConstraint bool) *int64 {
	given := optionalInt(v, "max_commits_per_repo")
	if hasDateConstraint {
		return given
	}
	if given != nil && *given != 0 {
		return given
	}
	def := int64(100)
	return &def
}

// githubCredentials is _resolve_github_sync_credentials up to the point it
// would open Postgres: CLI, then environment, then the database lookup.
func githubCredentials(v map[string]string, plan Plan, in Inputs) (*GitHubCredentials, *Refusal) {
	fromCLI, err := githubModeCredentials(v["auth"], v["github_app_id"], v["github_app_key_path"], v["github_app_installation_id"], nil, "cli", in)
	if err != nil {
		return nil, err
	}
	if fromCLI != nil {
		return fromCLI, nil
	}
	baseURL := firstNonEmpty(in.envOr("GITHUB_URL"), in.envOr("GITHUB_BASE_URL"))
	var base *string
	if baseURL != "" {
		base = &baseURL
	}
	fromEnv, err := githubModeCredentials(in.envOr("GITHUB_TOKEN"), in.envOr("GITHUB_APP_ID"),
		in.envOr("GITHUB_APP_PRIVATE_KEY_PATH"), in.envOr("GITHUB_APP_INSTALLATION_ID"), base, "environment", in)
	if err != nil {
		return nil, err
	}
	if fromEnv != nil {
		return fromEnv, nil
	}
	dbURL := ""
	if plan.DB != nil {
		dbURL = *plan.DB
	}
	dbURL = firstNonEmpty(dbURL, in.envOr("POSTGRES_URI"), in.envOr("DATABASE_URI"))
	// The organization is truthy when typed or in ORG_ID, and when none was
	// given the first organization in Postgres is used (the executor resolves
	// it), so the lookup is attempted.
	orgKnown := plan.OrgSource == OrgFromDBFirst || (plan.Org != nil && *plan.Org != "")
	if dbURL != "" && orgKnown {
		return &GitHubCredentials{Mode: CredentialDB}, nil
	}
	return nil, exitRefusal("Missing GitHub credentials (pass --auth, set GITHUB_TOKEN, configure GitHub App flags/env vars, or configure DB credentials).")
}

func githubModeCredentials(token, appID, keyPath, installationID string, base *string, name string, in Inputs) (*GitHubCredentials, *Refusal) {
	hasToken := token != ""
	hasAny := appID != "" || keyPath != "" || installationID != ""
	hasAll := appID != "" && keyPath != "" && installationID != ""
	if hasToken && hasAny {
		return nil, exitRefusal("GitHub auth must use exactly one mode: PAT (--auth/GITHUB_TOKEN) XOR GitHub App.")
	}
	if hasAny && !hasAll {
		return nil, exitRefusal("GitHub App auth requires app id, private key path, and installation id.")
	}
	if hasToken {
		return &GitHubCredentials{Mode: CredentialPAT, Name: name, BaseURL: base, Token: token}, nil
	}
	if hasAll {
		read := in.ReadKey
		if read == nil {
			read = func(path string) (string, error) {
				data, err := readFile(path)
				return string(data), err
			}
		}
		key, err := read(keyPath)
		if err != nil {
			return nil, exitRefusal(fmt.Sprintf("Unable to read GitHub App private key file: %s", keyPath))
		}
		return &GitHubCredentials{Mode: CredentialApp, Name: name, BaseURL: base, AppID: appID, PrivateKey: key, InstallationID: installationID}, nil
	}
	return nil, nil
}

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

// urlScheme is urllib.parse.urlsplit(uri).scheme, lower-cased.
func urlScheme(uri string) string {
	uri = strings.TrimLeftFunc(uri, func(r rune) bool { return r <= 0x20 })
	uri = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(uri)
	i := strings.Index(uri, ":")
	if i <= 0 {
		return ""
	}
	first := rune(uri[0])
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		return ""
	}
	for _, c := range uri[:i] {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.') {
			return ""
		}
	}
	return strings.ToLower(uri[:i])
}

// clickhouseDBType is storage.detect_db_type(uri) == "clickhouse": the
// case-insensitive literal prefixes, no urlsplit tolerance.
func clickhouseDBType(uri string) bool {
	lower := strings.ToLower(uri)
	for _, prefix := range []string{"clickhouse://", "clickhouse+http://", "clickhouse+https://", "clickhouse+native://"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func clickhouseScheme(scheme string) bool {
	switch scheme {
	case "clickhouse", "clickhouse+native", "clickhouse+http", "clickhouse+https":
		return true
	}
	return false
}

// runTarget is the verb body every target shares.
func runTarget(ctx context.Context, env cli.Env, target string, exec Executor, now Now) int {
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	plan, help, refusal := BuildPlan(target, env.Args, Inputs{Lookup: Lookup(env.Lookup), Now: now})
	if refusal != nil {
		writeLine(env.Stderr, fmt.Sprintf("dho sync %s: %s", target, refusal.Message))
		return refusal.Code
	}
	if help {
		writeLine(env.Stdout, targetUsage(target))
		return cli.ExitOK
	}
	if err := exec(ctx, plan, env); err != nil {
		var refused *Refusal
		if errors.As(err, &refused) {
			writeLine(env.Stderr, fmt.Sprintf("dho sync %s: %s", target, refused.Message))
			return refused.Code
		}
		writeLine(env.Stderr, fmt.Sprintf("dho sync %s: %s", target, err))
		if errors.Is(err, ErrNotAvailable) {
			return cli.ExitRefused
		}
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func writeLine(w io.Writer, line string) {
	if w != nil {
		fmt.Fprintln(w, line)
	}
}

func targetUsage(target string) string {
	return fmt.Sprintf("usage: dho sync %s --provider {local,github,gitlab,synthetic} [options]\n\n%s.\n\n"+
		"--provider is required. github needs --owner and --repo (or --search); gitlab needs --project-id\n"+
		"(or --search); local reads --repo-path (default .). ClickHouse comes from --analytics-db or CLICKHOUSE_URI.\n"+
		"Window: --since YYYY-MM-DD | --backfill N (default 1), --before YYYY-MM-DD. Exit codes: 0 ok, 1 failure,\n"+
		"2 usage error, 3 not available in dho yet.", target, targetSummaries[target])
}

// TargetCommands returns the `sync <target>` verbs, run by exec (the not-yet-
// available executor when nil).
func TargetCommands(exec Executor) []cli.Command {
	if exec == nil {
		exec = notAvailableExecutor
	}
	commands := make([]cli.Command, 0, len(Targets))
	for _, target := range Targets {
		target := target
		commands = append(commands, cli.Command{
			Name:    target,
			Summary: targetSummaries[target],
			Kind:    cli.Verb,
			Run: func(ctx context.Context, env cli.Env) int {
				return runTarget(ctx, env, target, exec, nil)
			},
		})
	}
	return commands
}
