package graph

import (
	"context"
	"log/slog"

	"github.com/99designs/gqlgen/graphql"
)

// operatingReviewSized reports whether the Operating Review document selects, under sections, the measure and the
// whole sizing coverage of the investment section (measure, sizingCoverage, unsizedUnitCount, workUnitCount), so a
// client never receives issue-size values it cannot label (D6027, D6032, CHAOS-9873). Every other document, and a
// call that is not a GraphQL resolution, reads the section in work units.
func operatingReviewSized(ctx context.Context) bool {
	if graphql.GetFieldContext(ctx) == nil || !graphql.HasOperationContext(ctx) {
		return false
	}
	opCtx := graphql.GetOperationContext(ctx)
	selected := map[string]bool{}
	for _, section := range graphql.CollectFieldsCtx(ctx, nil) {
		if section.Name != "sections" {
			continue
		}
		for _, sizing := range graphql.CollectFields(opCtx, section.Selections, nil) {
			if sizing.Name != "sizing" {
				continue
			}
			for _, inner := range graphql.CollectFields(opCtx, sizing.Selections, nil) {
				selected[inner.Name] = true
			}
		}
	}
	sized := selected["measure"] && selected["sizingCoverage"] && selected["unsizedUnitCount"] && selected["workUnitCount"]
	slog.DebugContext(ctx, "operatingReview: investment section measure", "issue_size", sized)
	return sized
}
