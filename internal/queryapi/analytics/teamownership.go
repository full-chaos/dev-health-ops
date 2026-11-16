package analytics

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// resolveFilterEmptyReason is the reason of the whole analytics answer (CHAOS-13868): team_owns_no_repository when the
// request's team scope selects teams that together own no repository. It costs one statement, sent only when the
// operation selects filterEmptyReason and the batch has a team scope whose every read is by ownership. nil otherwise,
// also when the selected teams own a repository and the window has no rows.
func resolveFilterEmptyReason(ctx context.Context, client QueryClient, orgID string, batch model.AnalyticsRequestInput, useInvestment, selected bool) (*string, error) {
	if !selected {
		return nil, nil
	}
	teamIDs := scopedTeamIDs(batch.Filters)
	if len(teamIDs) == 0 {
		return nil, nil
	}
	ownership, err := batchTeamScopeReadsOwnership(batch, useInvestment)
	if err != nil || !ownership {
		return nil, err
	}
	reason, err := teamscope.TeamOwnsNoRepository(ctx, client, orgID, teamIDs, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return teamscope.EmptyReasonText(reason), nil
}

// batchTeamScopeReadsOwnership says every read of the batch resolves a team scope as the repositories the team owns: a
// TestOps measure off the investment path (testopsOwner) or a read on the investment path. A read of the stored team
// column (any other measure off the investment path) is empty for another reason than ownership, and a sankey or a flow
// matrix is not traced here: either one leaves the batch without a reason. A batch with nothing to read has none.
func batchTeamScopeReadsOwnership(batch model.AnalyticsRequestInput, useInvestment bool) (bool, error) {
	if batch.Sankey != nil || batch.FlowMatrix != nil || len(batch.Timeseries)+len(batch.Breakdowns) == 0 {
		return false, nil
	}
	reads := func(dim Dimension, measure Measure) bool {
		return useInvestment || canonicalOnlyDimension(dim) || isTestopsMeasure(measure)
	}
	for _, input := range batch.Timeseries {
		req, err := TimeseriesRequestFromInput(input)
		if err != nil {
			return false, err
		}
		if !reads(req.Dimension, req.Measure) {
			return false, nil
		}
	}
	for _, input := range batch.Breakdowns {
		req, err := BreakdownRequestFromInput(input)
		if err != nil {
			return false, err
		}
		if !reads(req.Dimension, req.Measure) {
			return false, nil
		}
	}
	return true, nil
}
