package workerservice

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
)

// issuePRFallbackRetractionPreStep removes the explicit_text and heuristic
// issue <-> pull request links a native link outranks, from
// work_graph_issue_pr and work_graph_edges, for the whole organization. It
// runs after issue_pr_links (it reads the native rows that step writes) and
// before every step that reads work_graph_issue_pr. Every build of an
// organization re-applies the rule, so rows written before it and rows a
// native link arrived after are both cleaned by the next build.
type issuePRFallbackRetractionPreStep struct {
	service *issuepredges.Service
}

func newIssuePRFallbackRetractionPreStep(service *issuepredges.Service) (*issuePRFallbackRetractionPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePRFallbackRetractionPreStep{service: service}, nil
}

func (step *issuePRFallbackRetractionPreStep) Name() string { return "issue_pr_fallback_retraction" }

func (step *issuePRFallbackRetractionPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	outcome, err := step.service.RetractOutrankedFallbacks(ctx, claim.Request.OrganizationID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"links_read":      outcome.LinksRead,
		"links_retracted": outcome.LinksRetracted,
		"edges_retired":   outcome.EdgesRetired,
	}, nil
}
