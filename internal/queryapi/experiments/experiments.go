// Package experiments serves the experiments GraphQL field: the suggested
// experiments of the opportunity cards for an org, each promoted to a typed
// experiment record.
//
// The cards come from the opportunities package (the same build the REST
// opportunities route serves), so the two surfaces agree. Nothing is stored:
// an experiment is derived at read time, and its id is a SHA-256 prefix of the
// metric and the suggestion text, so it does not move when the card's rank
// does.
//
// The read scope is the scope level and ids of the request filter plus a fixed
// 30-day window with a 30-day comparison; the who, what, why and how filters
// are not consulted. When the opportunity build fails the field answers an
// empty item list with derivedFromOpportunities false, and logs the failure.
package experiments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
)

const (
	defaultRangeDays   = 30
	defaultCompareDays = 30
)

// Builder builds the opportunity cards for an org and filter.
type Builder func(ctx context.Context, orgID string, f home.Filters, now time.Time) (*opportunities.Response, error)

// NewBuilder returns the production builder over a ClickHouse client.
func NewBuilder(client home.QueryClient) Builder {
	return func(ctx context.Context, orgID string, f home.Filters, now time.Time) (*opportunities.Response, error) {
		return opportunities.BuildResponse(ctx, client, orgID, f, now)
	}
}

// FiltersFrom reads the scope of a request filter. An absent filter or scope
// is the org scope with no ids.
func FiltersFrom(in *model.FilterInput) home.Filters {
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: defaultRangeDays, CompareDays: defaultCompareDays},
		Scope: home.ScopeFilter{Level: "org", IDs: []string{}},
	}
	if in == nil || in.Scope == nil {
		return f
	}
	if level := strings.ToLower(string(in.Scope.Level)); level != "" {
		f.Scope.Level = level
	}
	if len(in.Scope.Ids) > 0 {
		f.Scope.IDs = append([]string{}, in.Scope.Ids...)
	}
	return f
}

// Resolve answers the experiments field for the org.
func Resolve(ctx context.Context, build Builder, orgID string, in *model.FilterInput, now time.Time) *model.ExperimentsResult {
	resp, err := build(ctx, orgID, FiltersFrom(in), now)
	if err != nil || resp == nil {
		slog.ErrorContext(ctx, "query-api: experiments opportunity build failed, answering empty",
			"operation", "experiments", "error", err)
		return &model.ExperimentsResult{Items: []model.Experiment{}, DerivedFromOpportunities: false}
	}
	return FromResponse(resp)
}

// FromResponse promotes every suggested experiment of every card.
func FromResponse(resp *opportunities.Response) *model.ExperimentsResult {
	items := []model.Experiment{}
	for _, card := range resp.Items {
		metric := metricFromTitle(card.Title)
		for _, suggestion := range card.SuggestedExperiments {
			items = append(items, model.Experiment{
				ID:            StableID(metric, suggestion),
				OpportunityID: card.ID,
				Hypothesis:    suggestion,
				Metric:        metric,
				Owner:         "",
				StopCondition: "",
				Status:        model.ExperimentStatusSuggested,
			})
		}
	}
	return &model.ExperimentsResult{Items: items, DerivedFromOpportunities: true}
}

// StableID is the first 16 hex digits of SHA-256 over "metric:suggestion".
func StableID(metric, suggestion string) string {
	sum := sha256.Sum256([]byte(metric + ":" + suggestion))
	return hex.EncodeToString(sum[:])[:16]
}

// metricFromTitle reverses the "Reduce <label>" card title into a metric key:
// the label lower-cased with spaces as underscores. Any other title is no
// metric.
func metricFromTitle(title string) string {
	if !strings.HasPrefix(title, "Reduce ") {
		return ""
	}
	label := title[strings.Index(title, " ")+1:]
	return strings.ReplaceAll(strings.ToLower(label), " ", "_")
}
