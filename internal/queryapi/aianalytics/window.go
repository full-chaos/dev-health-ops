package aianalytics

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

// windowToday is the clock of every window of this package when no GraphQL operation carries one: the one UTC day of
// timewindow. A test replaces it.
var windowToday = func() time.Time { return time.Now().UTC() }

// clippedRange validates the named range, then keeps its end to the last final day: an end of today or later is
// yesterday (D6083, D6101). A start after the clipped end leaves an empty window, never a fallback to yesterday. The
// returned range is what every loader binds and what the result names.
func clippedRange(ctx context.Context, dr model.AIDateRangeInput) (model.AIDateRangeInput, error) {
	if err := validateRange(dr); err != nil {
		return dr, err
	}
	end, err := timewindow.ClipLastDay(dr.EndDate.Time(), timewindow.RequestToday(ctx, windowToday))
	if err != nil {
		return dr, fmt.Errorf("AI analytics date range end_date is out of range: %w", err)
	}
	dr.EndDate = graphqldate.New(end)
	return dr, nil
}
