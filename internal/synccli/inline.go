package synccli

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// The inline executor runs a github or gitlab single-repository `dho sync
// <target>` request the way `dev-hops sync <target>` always did: in this
// process, with no scheduler, sync configuration or sync_run rows. It does NOT
// reimplement any fetch or write: each dataset of the target is the worker's own
// route, selected by providersync.SelectGitFamilyRoute and run through the
// worker's own CompleteRouteExecutor with in-process collaborators
// (providersync.RunInProcess).
//
// What it deliberately does not do, each as a named refusal so a script never
// mistakes it for a sync:
//   - a sync for which Postgres holds no organization (dblookup.go: the first
//     organization and the GitHub credential are read from Postgres like
//     Python does; with no organization at all the sync is refused);
//   - --provider synthetic (its own ticket).
//
// `--search` batch mode is batch.go: the listing, then this same per-dataset
// run for each listed repository.

// githubIncidentsRefusal is the Python message, verbatim
// (processors/github.py process_github_repo).
const githubIncidentsRefusal = "GitHub does not expose a native incident source; sync work items instead"

// Tickets the refusals point at.
const (
	ticketDBLookups = "CHAOS-6710"
)

// InlineDeps are the executor's outside connections, replaceable in tests.
type InlineDeps struct {
	OpenStore func(ctx context.Context, dsn string) (driver.Conn, error)
	Run       func(ctx context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error)
	List      func(ctx context.Context, run providersync.InProcessRun, listing providersync.InProcessListing) ([]providersync.ListedRepository, error)
	Now       func() time.Time

	// The two Postgres reads (CHAOS-6710); nil uses the real ones.
	FirstOrg         func(ctx context.Context, dbURL string) (string, bool)
	GitHubCredential func(ctx context.Context, dbURL, orgID string, env cli.Env) (*GitHubCredentials, error)

	// Local syncs a local repository (CHAOS-6775): the store connection is open.
	Local func(ctx context.Context, plan Plan, conn driver.Conn, env cli.Env, now time.Time) error
}

func defaultInlineDeps() InlineDeps {
	return InlineDeps{
		OpenStore: func(ctx context.Context, dsn string) (driver.Conn, error) {
			return clickhousestore.Open(ctx, clickhousestore.DefaultConfig(dsn))
		},
		Run:  providersync.RunInProcess,
		List: providersync.ListInProcess,
		Now:  time.Now,

		FirstOrg:         firstOrganization,
		GitHubCredential: githubCredentialFromDB,
		Local:            syncLocal,
	}
}

// InlineExecutor runs the requests it supports and refuses the rest with a
// message that names where each one lands.
func InlineExecutor(deps InlineDeps) Executor {
	defaults := defaultInlineDeps()
	if deps.OpenStore == nil {
		deps.OpenStore = defaults.OpenStore
	}
	if deps.Run == nil {
		deps.Run = defaults.Run
	}
	if deps.List == nil {
		deps.List = defaults.List
	}
	if deps.Now == nil {
		deps.Now = defaults.Now
	}
	if deps.Local == nil {
		deps.Local = defaults.Local
	}
	if deps.FirstOrg == nil {
		deps.FirstOrg = defaults.FirstOrg
	}
	if deps.GitHubCredential == nil {
		deps.GitHubCredential = defaults.GitHubCredential
	}
	lookups := dbLookups{FirstOrg: deps.FirstOrg, GitHubCredential: deps.GitHubCredential}
	return func(ctx context.Context, plan Plan, env cli.Env) error {
		if plan.Call == CallLocalRepo || plan.Call == CallLocalBlame {
			return runLocalRepo(ctx, deps, lookups, plan, env)
		}
		datasets, refusal := inlineDatasets(plan)
		if refusal != nil {
			return refusal
		}
		plan, refusal = resolveDBLookups(ctx, lookups, plan, env)
		if refusal != nil {
			return refusal
		}
		run, refusal := inlineRun(plan, deps.Now())
		if refusal != nil {
			return refusal
		}
		conn, err := deps.OpenStore(ctx, plan.SinkURI)
		if err != nil {
			return fmt.Errorf("open ClickHouse: %w", errors.New(redactDSN(err.Error(), plan.SinkURI)))
		}
		if closer, ok := conn.(interface{ Close() error }); ok {
			defer func() { _ = closer.Close() }()
		}
		run.Conn = conn
		if plan.Call == CallGitHubBatch || plan.Call == CallGitLabBatch {
			return runBatch(ctx, deps, plan, datasets, run, env)
		}
		for _, dataset := range datasets {
			run.Dataset = dataset
			if _, err := deps.Run(ctx, run); err != nil {
				return fmt.Errorf("%s/%s: %w", plan.Provider, dataset, err)
			}
		}
		return nil
	}
}

func redactDSN(text, dsn string) string {
	if dsn == "" {
		return text
	}
	return strings.ReplaceAll(text, dsn, "<clickhouse dsn>")
}

func notYet(plan Plan, what, ticket string) *Refusal {
	return &Refusal{
		Code:  cli.ExitRefused,
		Stage: "exit",
		Message: fmt.Sprintf("%s is not available in dho yet (%s); run dev-hops sync %s meanwhile",
			what, ticket, plan.Target),
	}
}

// unappliedFlags refuses a flag that the command line accepts (dev-hops does apply it)
// and that the worker's routes do not: a run that ignored it would look like a sync
// that honoured it. It is a usage error (exit 2) that names the flag and writes
// nothing. Only what Python applies is refused: --max-commits-per-repo caps the
// commits, their stats and the blame backfill of `git` and `blame`; --rate-limit-delay
// seeds the pull-request backoff of a batch that syncs pull requests. (--use-async is
// accepted and ignored by dev-hops itself, so ignoring it is parity.)
func unappliedFlags(plan Plan) *Refusal {
	refuse := func(flag, why string) *Refusal {
		return &Refusal{
			Code:  cli.ExitUsage,
			Stage: "exit",
			Message: fmt.Sprintf("%s is not supported by the Go routes (%s); drop the flag, or run dev-hops sync %s",
				flag, why, plan.Target),
		}
	}
	if plan.MaxCommitsGiven && (plan.Target == "git" || plan.Target == "blame") {
		return refuse("--max-commits-per-repo", "they fetch the whole window and stop at their own page caps")
	}
	batch := plan.Call == CallGitHubBatch || plan.Call == CallGitLabBatch
	if plan.RateLimitDelayGiven && batch && plan.Target == "prs" {
		return refuse("--rate-limit-delay", "they back off on the provider's own rate-limit signals")
	}
	return nil
}

// inlineDatasets maps the target to the worker datasets it runs, or refuses.
func inlineDatasets(plan Plan) ([]string, *Refusal) {
	switch plan.Call {
	case CallGitHubSingle, CallGitLabSingle, CallGitHubBatch, CallGitLabBatch:
	default:
		return nil, notYet(plan, "this provider", "chris-pending")
	}
	if refusal := unappliedFlags(plan); refusal != nil {
		return nil, refusal
	}
	switch plan.Target {
	case "incidents":
		if plan.Provider == "github" {
			// process_github_repo(sync_incidents=True) raises this ValueError: an
			// uncaught traceback, exit 1. GitHub has no native incident source.
			return nil, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: "ValueError", Message: githubIncidentsRefusal}
		}
	}
	var datasets []string
	for _, capability := range providersync.Capabilities(plan.Provider) {
		if capability.WorkItemDataset || capability.FeatureFlagDataset || !contains(capability.LegacyTargets, plan.Target) {
			continue
		}
		descriptor, known := providersync.Descriptor(plan.Provider, capability.Dataset)
		if !known || !descriptor.RouteReady {
			continue
		}
		// A dataset that is only an alias of another (pr-reviews and pr-comments
		// of prs, tests of cicd) is written by its canonical dataset: the worker
		// plans a `tests` unit as its `cicd` route, whose one sink writes the CI
		// runs and the test results together, so the alias the target names runs
		// as the canonical route, once.
		dataset := capability.Dataset
		if descriptor.CanonicalDataset != capability.Dataset {
			if capability.Dataset != plan.Target {
				continue
			}
			dataset = descriptor.CanonicalDataset
		}
		if !contains(datasets, dataset) {
			datasets = append(datasets, dataset)
		}
	}
	if len(datasets) == 0 {
		return nil, notYet(plan, "target "+plan.Target, "no worker dataset serves it")
	}
	// Repository metadata first: it is the row every other dataset hangs off.
	sort.SliceStable(datasets, func(i, j int) bool {
		return datasets[i] == "repo-metadata" && datasets[j] != "repo-metadata"
	})
	return datasets, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// inlineRun builds the provider-neutral part of the run from the plan.
func inlineRun(plan Plan, now time.Time) (providersync.InProcessRun, *Refusal) {
	if plan.Org == nil || *plan.Org == "" {
		return providersync.InProcessRun{}, notYet(plan, "resolving the organization from PostgreSQL (pass --org or set ORG_ID)", ticketDBLookups)
	}
	run := providersync.InProcessRun{
		OrgID: *plan.Org, Provider: plan.Provider, SinceAt: plan.Since, BeforeAt: now.UTC(),
		Config: map[string]string{},
	}
	switch plan.Call {
	case CallGitHubSingle, CallGitHubBatch:
		if plan.Call == CallGitHubSingle {
			run.SourceExternalID = plan.Owner + "/" + plan.Repo
			run.SourceName = run.SourceExternalID
		}
		creds := plan.GitHub
		switch {
		case creds == nil || creds.Mode == CredentialDB:
			return providersync.InProcessRun{}, notYet(plan, "GitHub credentials from the database (pass --auth, --github-app-*, or set GITHUB_TOKEN)", ticketDBLookups)
		case creds.Mode == CredentialApp:
			run.Credential = map[string]string{"app_id": creds.AppID, "private_key": creds.PrivateKey, "installation_id": creds.InstallationID}
		default:
			run.Credential = map[string]string{"token": creds.Token}
		}
		if creds.BaseURL != nil {
			run.Config["base_url"] = *creds.BaseURL
		}
	case CallGitLabSingle, CallGitLabBatch:
		if plan.Call == CallGitLabSingle {
			run.SourceExternalID = plan.ProjectID.String()
			run.SourceName = run.SourceExternalID
		}
		run.Credential = map[string]string{"token": plan.GitLabToken}
		run.Config["base_url"] = plan.GitLabURL
	}
	return run, nil
}
