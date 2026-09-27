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

// jiraCombinedTeamCatalogCollector makes real Atlassian Teams collection
// (internal/atlassianteams) the PRIMARY output of the jira team step for an
// integration whose credential config carries atlassian_organization_id
// (org-opt-in, set on the same jira integration row the credential lives
// on -- see internal/synccli/jira_stored_credential.go's identical read),
// falling back to the existing project-as-team catalog (ProjectAsTeam) only
// when the Teams API is unavailable for this tenant this run (an error, or
// a suspiciously empty answer -- see collectAtlassianTeams's doc comment).
// An integration with no atlassian_organization_id configured, or with
// auto_import_teams off, runs ProjectAsTeam exactly as before, unaffected.
//
// D2775 (lead ruling): before this, internal/atlassianteams.Collect was only
// ever reachable from the standalone `dho sync teams --provider jira` CLI
// verb -- a normal scheduled jira sync never produced real ARI-shaped
// Atlassian Teams rows, only the generic project-as-team fallback, unless a
// human ran the CLI verb by hand. This type is registered under the "jira"
// key of the production Native map (sync_dispatch.go) in ProjectAsTeam's
// place, so the SAME credential seam the CLI verb fix resolves now also
// drives the automatic post-sync/reference-discovery dispatch.
//
// Provenance: exactly one of the two writers runs per invocation, and each
// writes into a disjoint row set of the SAME physical tables (teams /
// team_memberships / team_project_ownership) -- Atlassian Teams rows carry
// `native_team_key` = the team's full ARI (`ari:cloud:identity::team/...`),
// project-as-team rows carry the Jira project key instead (never an ARI).
// A row's own native_team_key IS its provenance: which path produced it is
// directly queryable, no separate marker column needed. Read-time
// team-attribution precedence (specificity 110 for a real Atlassian team vs
// 100 for a project-as-team row) remains the safety net for any rows a
// PRIOR run left behind under the other path (e.g. a run where Atlassian
// Teams was temporarily unavailable, or before an org configured
// atlassian_organization_id at all) -- see
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
	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	// auto_import_teams (selections.Teams) gates this exactly the way it
	// gates every other provider's Teams surface -- an integration with it
	// off never attempts Atlassian Teams at all, matching D2775's "for jira
	// integrations with auto_import_teams on".
	attemptAtlassian := organizationID != "" && selections.Teams && client != nil && client.BaseURL != nil

	if attemptAtlassian {
		rows, collectErr := collector.collectAtlassianTeams(ctx, ref, credential, client, selections, normalizedAt, organizationID)
		switch {
		case collectErr == nil && len(rows.Teams) > 0:
			// Primary: write real Atlassian Teams rows. A write failure here
			// is an infrastructure fault (not "the tenant's Teams API is
			// unavailable"), so it is never a fallback candidate -- it
			// propagates exactly like ProjectAsTeam's own write failures do.
			if _, err := atlassianteams.Write(ctx, collector.Conn, ref.OrgID, rows, atlassianteams.Selections{
				Structure: selections.Teams, Members: selections.Members, Projects: selections.Projects,
			}); err != nil {
				return providersync.TeamCatalogResult{}, err
			}
			result := providersync.TeamCatalogResult{
				TeamsWritten:       len(rows.Teams),
				MembershipsWritten: len(rows.Memberships),
				OwnershipWritten:   len(rows.Ownership),
				MembersWritten:     len(distinctAtlassianTeamsMembers(rows.Memberships)),
			}
			for _, team := range rows.Teams {
				result.TeamKeys = append(result.TeamKeys, team.ID)
			}
			return result, nil
		case collectErr != nil && ref.Strict:
			// Strict (reference discovery) propagates failures exactly like
			// every other collector -- no silent fallback under strict.
			return providersync.TeamCatalogResult{}, collectErr
		default:
			// Unavailable this run: either the read failed, or it returned
			// suspiciously empty (far more often a permissions/configuration
			// problem than a real zero-team organization, same caution the
			// CLI verb's own --allow-empty refusal applies). Fall back to
			// the project-as-team catalog and say why -- this IS the
			// "provenance" of a fallback run: it is distinguishable in logs
			// from an integration that simply never configured
			// atlassian_organization_id (which never logs this line at all).
			reason := "atlassian_teams_unavailable"
			if collectErr == nil {
				reason = "atlassian_teams_empty_result"
			}
			slog.Default().WarnContext(ctx, "jira_atlassian_teams_unavailable_fallback_to_project_as_team",
				"org_id", ref.OrgID, "reason", reason, "error", collectErr)
		}
	}
	return collector.ProjectAsTeam.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
}

// collectAtlassianTeams reads (never writes) the Atlassian Teams collection
// for this tenant. The caller decides fallback-vs-hard-error from the
// returned error: a read failure here means the Teams API was unavailable
// for this tenant this run, which is a legitimate fallback trigger; a WRITE
// failure (handled by the caller, not here) is not.
func (collector jiraCombinedTeamCatalogCollector) collectAtlassianTeams(
	ctx context.Context,
	ref providersync.TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	normalizedAt time.Time,
	organizationID string,
) (atlassianteams.Rows, error) {
	if collector.Conn == nil {
		return atlassianteams.Rows{}, providersync.ErrInvalidConfiguration
	}
	tenant, err := normalizeAtlassianTenantURL(client.BaseURL.String())
	if err != nil {
		return atlassianteams.Rows{}, err
	}
	cloudID := strings.TrimSpace(credential.Config["atlassian_cloud_id"])
	doer := collector.Doer
	if doer == nil {
		doer = &http.Client{Timeout: 45 * time.Second}
	}
	if cloudID == "" {
		cloudID, err = atlassianteams.ResolveCloudID(ctx, doer, tenant)
		if err != nil {
			return atlassianteams.Rows{}, err
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
		return atlassianteams.Rows{}, providersync.ErrInvalidConfiguration
	}
	gatewayClient := collector.newClient(tenant.String()+"/gateway/api", atlassian.BasicAPITokenAuth{Email: email.Reveal(), Token: token})
	return atlassianteams.Collect(ctx, gatewayClient, atlassianteams.Params{
		OrgID: ref.OrgID, OrganizationID: organizationID, SiteID: cloudID,
		Selections: atlassianteams.Selections{
			Structure: selections.Teams, Members: selections.Members, Projects: selections.Projects,
		},
		Now: normalizedAt,
	})
}

func distinctAtlassianTeamsMembers(memberships []atlassianteams.MembershipRow) []string {
	seen := make(map[string]struct{}, len(memberships))
	for _, membership := range memberships {
		seen[membership.MemberID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for member := range seen {
		out = append(out, member)
	}
	return out
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
