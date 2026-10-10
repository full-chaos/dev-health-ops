package graph

import (
	"context"

	"github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// analyticsSelection reads, from the operation that is being resolved, which
// parts of the analytics answer it selected, for the parts that cost a
// ClickHouse statement of their own (analytics.Selection). The batch resolver
// then sends only the statements of the parts that were asked for.
//
// It reads the selection set of the `analytics` field as gqlgen collected it:
// fragments and inline fragments are followed, an alias does not hide a field
// (the schema name is compared), and a field under @skip / @include is in or
// out by the operation's variables. A field that is selected more than once
// (two aliases) is selected.
//
// When there is no field being resolved on the context (a call that is not a
// GraphQL resolution), the selection is not known and EVERYTHING is selected:
// a part is left out only on the positive knowledge that nobody asked for it.
func analyticsSelection(ctx context.Context) analytics.Selection {
	if graphql.GetFieldContext(ctx) == nil || !graphql.HasOperationContext(ctx) {
		return analytics.EverythingSelected()
	}
	var selected analytics.Selection
	for _, field := range graphql.CollectFieldsCtx(ctx, nil) {
		switch field.Name {
		case "breakdowns":
			selected.Breakdowns = true
		case "evidenceQualityStats", "evidenceQualityDistribution":
			// The distribution is the band counts of the stats statement.
			selected.EvidenceQualityStats = true
		case "evidenceQualityByGroup":
			selected.EvidenceQualityByGroup = true
		case "sankey":
			for _, inner := range graphql.CollectFields(graphql.GetOperationContext(ctx), field.Selections, nil) {
				if inner.Name == "coverage" {
					selected.SankeyCoverage = true
				}
			}
		}
	}
	return selected
}
