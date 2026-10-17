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

// issuePRFallbackInvariantPreStep checks the native-first rule after the last
// step that writes work_graph_issue_pr (issue_pr_edges_heuristic): no
// explicit_text or heuristic link may sit beside a native one. A breach is
// reported (WARN, gauge, outcome) and not repaired, and it does not fail the
// build, so the later graph steps still run.
type issuePRFallbackInvariantPreStep struct {
	service *issuepredges.Service
}

func newIssuePRFallbackInvariantPreStep(service *issuepredges.Service) (*issuePRFallbackInvariantPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePRFallbackInvariantPreStep{service: service}, nil
}

func (step *issuePRFallbackInvariantPreStep) Name() string { return "issue_pr_fallback_invariant" }

func (step *issuePRFallbackInvariantPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	outcome, err := step.service.CheckFallbackInvariant(ctx, claim.Request.OrganizationID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"links_read":             outcome.LinksRead,
		"fallback_beside_native": outcome.Beside.Total,
	}, nil
}
