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
//   - credentials from the database and the first organization in Postgres
//     (both are Postgres reads): pass --auth/--github-app-* (or GITHUB_TOKEN /
//     GITLAB_TOKEN) and --org (or ORG_ID);
//   - the chunked targets cicd and tests (they need a chunk store the
//     in-process ledger does not implement yet);
//   - incidents, --search batch and --provider local|synthetic (their own
//     tickets).

// Tickets the refusals point at.
const (
	ticketDBLookups = "CHAOS-6710"
	ticketChunked   = "CHAOS-6711"
	ticketIncidents = "CHAOS-6683"
	ticketBatch     = "CHAOS-6684"
	ticketLocal     = "CHAOS-6685"
)

// InlineDeps are the executor's outside connections, replaceable in tests.
type InlineDeps struct {
	OpenStore func(ctx context.Context, dsn string) (driver.Conn, error)
	Run       func(ctx context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error)
	Now       func() time.Time
}

func defaultInlineDeps() InlineDeps {
	return InlineDeps{
		OpenStore: func(ctx context.Context, dsn string) (driver.Conn, error) {
			return clickhousestore.Open(ctx, clickhousestore.DefaultConfig(dsn))
		},
		Run: providersync.RunInProcess,
		Now: time.Now,
	}
}

// InlineExecutor runs the requests it supports and refuses the rest with a
// message that names where each one lands.
func InlineExecutor(deps InlineDeps) Executor {
	if deps.OpenStore == nil || deps.Run == nil || deps.Now == nil {
		defaults := defaultInlineDeps()
		if deps.OpenStore == nil {
			deps.OpenStore = defaults.OpenStore
		}
		if deps.Run == nil {
			deps.Run = defaults.Run
		}
		if deps.Now == nil {
			deps.Now = defaults.Now
		}
	}
	return func(ctx context.Context, plan Plan, env cli.Env) error {
		datasets, refusal := inlineDatasets(plan)
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

// inlineDatasets maps the target to the worker datasets it runs, or refuses.
func inlineDatasets(plan Plan) ([]string, *Refusal) {
	switch plan.Call {
	case CallGitHubSingle, CallGitLabSingle:
	case CallGitHubBatch, CallGitLabBatch:
		return nil, notYet(plan, "--search batch mode", ticketBatch)
	case CallLocalRepo, CallLocalBlame:
		return nil, notYet(plan, "the local provider", ticketLocal)
	default:
		return nil, notYet(plan, "this provider", "chris-pending")
	}
	switch plan.Target {
	case "incidents":
		return nil, notYet(plan, "incidents", ticketIncidents)
	case "cicd", "tests":
		return nil, notYet(plan, "the chunked CI/CD and test routes", ticketChunked)
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
		// of prs) is written by its canonical dataset.
		if descriptor.CanonicalDataset != capability.Dataset && capability.Dataset != plan.Target {
			continue
		}
		if descriptor.Chunked {
			return nil, notYet(plan, "the chunked "+capability.Dataset+" route", ticketChunked)
		}
		datasets = append(datasets, capability.Dataset)
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
	if plan.OrgSource == OrgFromDBFirst || plan.Org == nil || *plan.Org == "" {
		return providersync.InProcessRun{}, notYet(plan, "resolving the organization from PostgreSQL (pass --org or set ORG_ID)", ticketDBLookups)
	}
	run := providersync.InProcessRun{
		OrgID: *plan.Org, Provider: plan.Provider, SinceAt: plan.Since, BeforeAt: now.UTC(),
		Config: map[string]string{},
	}
	switch plan.Call {
	case CallGitHubSingle:
		run.SourceExternalID = plan.Owner + "/" + plan.Repo
		run.SourceName = run.SourceExternalID
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
	case CallGitLabSingle:
		run.SourceExternalID = plan.ProjectID.String()
		run.SourceName = run.SourceExternalID
		run.Credential = map[string]string{"token": plan.GitLabToken}
		run.Config["base_url"] = plan.GitLabURL
	}
	return run, nil
}
