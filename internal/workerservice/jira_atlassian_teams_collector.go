package workerservice

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// jiraCombinedTeamCatalogCollector composes the existing project-as-team
// catalog (ProjectAsTeam, unchanged, always runs) with a real Atlassian
// Teams collection (internal/atlassianteams). Before this (D2770/CHAOS-7002),
// internal/atlassianteams.Collect was only ever reachable from the standalone
// `dho sync teams --provider jira` CLI verb -- a normal scheduled jira sync
// never produced real ARI-shaped Atlassian Teams rows, only the generic
// project-as-team fallback, unless a human ran the CLI verb by hand. This
// type is registered under the "jira" key of the production Native map
// (sync_dispatch.go) in ProjectAsTeam's place, so both writers run from the
// same automatic post-sync/reference-discovery dispatch.
//
// The Atlassian Teams step is additive: it always runs alongside ProjectAsTeam
// whenever the org has selected team import at all (selections.Any(), the
// existing auto_import_teams/auto_import_projects/auto_import_members gate --
// unchanged by this collector). atlassian_organization_id, when the resolved
// credential's config carries one, is an OVERRIDE; otherwise it is derived
// live via the AGG tenantContexts query (atlassianteams.ResolveOrganizationID,
// D2817/CHAOS-7020) using the same stored credential -- no manual config step
// is required for a jira integration to get real ARI-shaped Atlassian Teams
// rows. A resolution failure (e.g. no organization context, or no permission)
// degrades the same as any other Atlassian Teams read failure: non-strict
// logs and keeps ProjectAsTeam's result, strict propagates. Both writers
// target the SAME physical tables
// (teams / team_memberships / team_project_ownership); team-attribution's
// specificity ranking (110 for a real Atlassian team vs 100 for the
// project-as-team fallback) resolves precedence automatically -- see
// .github/docs-legacy/architecture/team-attribution.md §0.2a.
type jiraCombinedTeamCatalogCollector struct {
	// ProjectAsTeam is the interface, not the concrete
	// providersync.JiraTeamCatalogCollector, purely so a unit test can
	// inject a fake for it without a real ClickHouse connection; production
	// always wires the real one (sync_dispatch.go).
	ProjectAsTeam providersync.TeamCatalogCollector
	Conn          driver.Conn
	// Doer builds the Atlassian Teams (AGG GraphQL gateway) HTTP client; a
	// nil Doer, matching CollectTeamCatalog's other providers, defaults to a
	// short-timeout *http.Client at call time.
	Doer providerfoundation.HTTPDoer
	// NewClient builds the Atlassian Teams gateway client from a resolved
	// gateway URL + auth; nil (production) builds the real *graph.Client,
	// exactly as internal/synccli's own defaultDeps().newClient does. Tests
	// replace it with a fake atlassianteams.Client, the same seam the CLI
	// verb's own test harness uses, so this collector needs no live tenant
	// or AGG GraphQL response fixture to unit test.
	NewClient func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client
	// NewOrganizationResolver builds the AGG gateway client used to derive
	// atlassian_organization_id when the resolved credential's config
	// doesn't carry an override (D2817/CHAOS-7020); nil (production) builds
	// the real *graph.Client, exactly like NewClient. Tests replace it with a
	// fake atlassianteams.OrganizationResolver.
	NewOrganizationResolver func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver
}

func (collector jiraCombinedTeamCatalogCollector) newClient(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client {
	if collector.NewClient != nil {
		return collector.NewClient(gatewayURL, auth)
	}
	return &graph.Client{
		BaseURL: gatewayURL, Auth: auth, Strict: true,
		HTTPClient: atlassianteams.GatewayHTTPClient(45 * time.Second),
	}
}

func (collector jiraCombinedTeamCatalogCollector) newOrganizationResolver(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver {
	if collector.NewOrganizationResolver != nil {
		return collector.NewOrganizationResolver(gatewayURL, auth)
	}
	return &graph.Client{
		BaseURL: gatewayURL, Auth: auth, Strict: true,
		HTTPClient: atlassianteams.GatewayHTTPClient(45 * time.Second),
	}
}

func (collector jiraCombinedTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref providersync.TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	normalizedAt time.Time,
) (providersync.TeamCatalogResult, error) {
	result, err := collector.ProjectAsTeam.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		return result, err
	}
	// The two legs are INDEPENDENT (D2778): a Skipped project-as-team walk
	// (e.g. a transient Jira REST failure) must never suppress the real
	// Atlassian Teams leg, which authenticates and reads through its own
	// client/gateway and has no dependency on the project-as-team walk
	// having succeeded. Only "nothing selected" (nothing for either leg to
	// do) short-circuits here.
	if !selections.Any() {
		return result, nil
	}
	if client == nil || client.BaseURL == nil {
		// A skipped Atlassian Teams leg must say so (CHAOS-7132): this return used to leave no line,
		// so an org with no real Atlassian Teams rows could not be told from one with none to write.
		slog.Default().WarnContext(ctx, "jira_atlassian_teams_walk_skipped", "org_id", ref.OrgID, "sync_run_id", ref.SyncRunID, "reason", "no_base_url")
		return result, nil
	}
	atlassianResult, err := collector.collectAtlassianTeams(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		// The Atlassian Teams leg is ADDITIVE and independent (D2778): its failure must neither fail
		// reference discovery (strict) nor undo the project-as-team write above. It is never silent
		// and never a clean success (CHAOS-7132): a Warn line and a degraded leg that the discovery
		// ledger and the run's result carry, with a value-free reason.
		slog.Default().WarnContext(ctx, "jira_atlassian_teams_walk_skipped", "org_id", ref.OrgID, "sync_run_id", ref.SyncRunID,
			"strict", ref.Strict, "reason", atlassianLegReason(err), "error", syncdispatchruntime.SanitizeErrorText(err.Error()))
		result.DegradedLegs = append(result.DegradedLegs, newDegradedAtlassianLeg(err))
		return result, nil
	}
	// Every count/key below comes from atlassianteams.Write's own Result --
	// what it actually persisted after its sync_policy/membership-conflict
	// guards, keyed by native_team_key (the ARI ClickHouse stores), never
	// the pre-guard collected snapshot or the bare TeamRow.ID.
	result.TeamsWritten += atlassianResult.TeamsWritten
	result.MembershipsWritten += atlassianResult.MembershipsWritten
	result.OwnershipWritten += atlassianResult.OwnershipWritten
	result.MembersWritten += atlassianResult.MembersWritten
	result.TeamKeys = append(result.TeamKeys, atlassianResult.TeamKeys...)
	return result, nil
}

// newDegradedAtlassianLeg records a failed Atlassian Teams leg. Detail is err.Error(): for a provider
// failure that is its class, status and request path only (ProviderError.Error never formats the
// response body), for the gateway's GraphQL errors the gateway's own message (e.g. "Invalid Organization
// Ari: <uuid>"). The recorder bounds and sanitizes it again before it is stored.
func newDegradedAtlassianLeg(err error) providersync.DegradedLeg {
	return providersync.DegradedLeg{
		Dataset: "teams", Leg: "jira_atlassian_teams", Outcome: "failed",
		Reason: atlassianLegReason(err), Detail: syncdispatchruntime.SanitizeErrorText(err.Error()),
	}
}

// atlassianLegReason is the fixed-vocabulary, value-free reason an Atlassian Teams leg failed.
func atlassianLegReason(err error) string {
	switch {
	case errors.Is(err, atlassianteams.ErrOrganizationPermission):
		return "organization_permission"
	case errors.Is(err, atlassianteams.ErrOrganizationNotFound):
		return "organization_not_found"
	case errors.Is(err, atlassianteams.ErrConfiguration), errors.Is(err, providersync.ErrInvalidConfiguration):
		return "configuration"
	}
	if reason := providerfoundation.FailureReason(err); reason != "" {
		return reason
	}
	return "unclassified"
}

// redactedLegError is an Atlassian-leg error whose text has the credential removed: the gateway can
// echo the credential it was sent (CHAOS-7132). It unwraps to the original, so the reason and the
// sentinel checks (errors.Is) are unchanged.
type redactedLegError struct {
	text  string
	cause error
}

func (e *redactedLegError) Error() string { return e.text }
func (e *redactedLegError) Unwrap() error { return e.cause }

func redactLegError(err error, values ...string) error {
	if err == nil {
		return nil
	}
	// The existing value-based primitive (secrets.Boundary, the one every dho verb redacts through),
	// fed the credential this leg actually authenticated with.
	text := secrets.NewBoundaryWith("", values...).RedactText(err.Error())
	if text == err.Error() {
		return err
	}
	return &redactedLegError{text: text, cause: err}
}

func (collector jiraCombinedTeamCatalogCollector) collectAtlassianTeams(
	ctx context.Context,
	ref providersync.TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	normalizedAt time.Time,
) (result atlassianteams.Result, err error) {
	if collector.Conn == nil {
		return atlassianteams.Result{}, providersync.ErrInvalidConfiguration
	}
	tenant, err := normalizeAtlassianTenantURL(client.BaseURL.String())
	if err != nil {
		return atlassianteams.Result{}, err
	}
	cloudID := strings.TrimSpace(credential.Config["atlassian_cloud_id"])
	doer := collector.Doer
	if doer == nil {
		doer = &http.Client{Timeout: 45 * time.Second, CheckRedirect: atlassianteams.RefuseRedirects}
	}
	if cloudID == "" {
		cloudID, err = atlassianteams.ResolveCloudID(ctx, doer, tenant)
		if err != nil {
			return atlassianteams.Result{}, err
		}
	}
	email, _ := credential.Secret("email")
	token := ""
	for _, alias := range []string{"api_token", "apiToken", "token"} {
		if value, ok := credential.Secret(alias); ok && value.Configured() {
			token = value.Reveal()
			break
		}
	}
	if !email.Configured() || token == "" {
		return atlassianteams.Result{}, providersync.ErrInvalidConfiguration
	}
	// From here on any error can carry what the gateway echoes back: strip the credential at the source.
	defer func() { err = redactLegError(err, token, email.Reveal()) }()
	gatewayURL := tenant.String() + "/gateway/api"
	auth := atlassian.BasicAPITokenAuth{Email: email.Reveal(), Token: token}

	// atlassian_organization_id is an OVERRIDE only (D2817/CHAOS-7020):
	// when the resolved credential's config doesn't carry one, derive it
	// live via the AGG tenantContexts query using this same credential --
	// no manual config step is required.
	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	if organizationID == "" {
		organizationID, err = atlassianteams.ResolveOrganizationID(ctx, collector.newOrganizationResolver(gatewayURL, auth), cloudID)
		if err != nil {
			return atlassianteams.Result{}, err
		}
	}
	gatewayClient := collector.newClient(gatewayURL, auth)
	atlassianSelections := atlassianteams.Selections{
		Structure: selections.Teams, Members: selections.Members, Projects: selections.Projects,
	}
	rows, err := atlassianteams.Collect(ctx, gatewayClient, atlassianteams.Params{
		OrgID: ref.OrgID, OrganizationID: organizationID, SiteID: cloudID,
		Selections: atlassianSelections,
		Now:        normalizedAt,
	})
	if err != nil {
		return atlassianteams.Result{}, err
	}
	if len(rows.Teams) == 0 {
		// An empty Atlassian Teams answer is far more often a permissions or
		// configuration problem than a real empty organization (see the CLI
		// verb's identical refusal); the automatic path is silent-by-default
		// (no --allow-empty escape hatch here), so it simply writes nothing
		// rather than retracting the project-as-team fallback's members and
		// links -- Write's own retraction logic only ever acts on the
		// Atlassian-ARI-keyed rows it owns, never on project-as-team rows.
		return atlassianteams.Result{}, nil
	}
	return atlassianteams.Write(ctx, collector.Conn, ref.OrgID, rows, atlassianSelections)
}

// normalizeAtlassianTenantURL forces https and strips to scheme+host,
// mirroring internal/synccli's normalizeBase: client.BaseURL is whatever the
// stored credential's base_url alias held, verbatim (providerfoundation.
// NewJiraClient does not itself normalize it), and the AGG gateway path is
// joined onto it by plain string concatenation.
func normalizeAtlassianTenantURL(value string) (*url.URL, error) {
	value = strings.TrimRight(strings.TrimSpace(value), "/")
	switch {
	case strings.HasPrefix(value, "http://"):
		value = "https://" + strings.TrimPrefix(value, "http://")
	case !strings.HasPrefix(value, "https://"):
		value = "https://" + strings.TrimLeft(value, "/")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return &url.URL{Scheme: parsed.Scheme, Host: parsed.Host}, nil
}
