package integrationsadmin

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// recordEnableRejectedAtRepoLimit counts one enable refused at max_repos:
// IntegrationSourceService.set_enabled's
// outcome="rejected_at_enable_repo_limit" when an operator's enable of a Jira
// source would exceed the org's max_repos. The counter itself is the one
// jira_project_discovery_total instrument in providerfoundation, shared with
// the discovery service.
func recordEnableRejectedAtRepoLimit(ctx context.Context) {
	providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryRejectedAtEnable, 1)
}
