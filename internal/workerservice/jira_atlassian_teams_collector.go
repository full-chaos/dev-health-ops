package workerservice

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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
// The Atlassian Teams step is additive and org-opt-in: it runs only when the
// resolved credential's config carries atlassian_organization_id (set on the
// same jira integration row the credential itself lives on -- see
// internal/synccli/jira_stored_credential.go's identical read). An org that
// has not configured it yet is completely unaffected: ProjectAsTeam's result
// passes through unchanged. Both writers target the SAME physical tables
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
}

func (collector jiraCombinedTeamCatalogCollector) newClient(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client {
	if collector.NewClient != nil {
		return collector.NewClient(gatewayURL, auth)
	}
	return &graph.Client{
		BaseURL: gatewayURL, Auth: auth, Strict: true,
		HTTPClient: &http.Client{Timeout: 45 * time.Second, Transport: atlassianteams.CompletePagesOnly(nil)},
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
	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	if organizationID == "" || client == nil || client.BaseURL == nil {
		return result, nil
	}
	atlassianResult, err := collector.collectAtlassianTeams(ctx, ref, credential, client, selections, normalizedAt, organizationID)
	if err != nil {
		if ref.Strict {
			return result, err
		}
		// Non-strict (post-sync dispatch): the project-as-team write above
		// already succeeded (or was itself skipped) and must not be undone
		// by an Atlassian Teams failure -- log and keep that result,
		// mirroring every other collector's non-strict walk-failure
		// discipline in this package.
		slog.Default().WarnContext(ctx, "jira_atlassian_teams_walk_skipped", "org_id", ref.OrgID, "error", err)
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

func (collector jiraCombinedTeamCatalogCollector) collectAtlassianTeams(
	ctx context.Context,
	ref providersync.TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	normalizedAt time.Time,
	organizationID string,
) (atlassianteams.Result, error) {
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
		doer = &http.Client{Timeout: 45 * time.Second}
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
	gatewayClient := collector.newClient(tenant.String()+"/gateway/api", atlassian.BasicAPITokenAuth{Email: email.Reveal(), Token: token})
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
