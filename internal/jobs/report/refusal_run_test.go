package report

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A plan that charts a metric the reader refuses ends its run once, with its
// own stored code, and is not retried: the job error is of the permanent
// category. An outage of the same step keeps the code query_failed and stays
// retryable. Nothing is rendered, stored or notified in either case.
func TestRunOfARefusedChartMetricFailsOncePermanentlyWithItsOwnCode(t *testing.T) {
	cases := []struct {
		name         string
		queryErr     error
		wantCode     string
		wantCategory string
	}{
		{"refused metric", &ChartMetricError{Metric: "assignee", Kind: "text"}, "chart_metric_refused", "permanent"},
		{"undeclared table", &ChartMetricError{Metric: "commits_count", Kind: kindUndeclaredTable, Table: "user_metrics_daily"}, "chart_metric_refused", "permanent"},
		{"outage", ErrDependencyUnavailable, "query_failed", "retryable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeRunStore{claim: true, complete: true, notificationClaim: true}
			rendered := 0
			dependencies := Dependencies{
				Runs:  store,
				Query: queryFunc(func(context.Context, QueryInput) (QueryResult, error) { return QueryResult{}, tc.queryErr }),
				Renderer: rendererFunc(func(context.Context, QueryResult) (Artifact, error) {
					rendered++
					return Artifact{}, nil
				}),
				Artifacts:     artifactFunc(func(_ context.Context, _ string, artifact Artifact) (Artifact, error) { return artifact, nil }),
				Notifications: notificationFunc(func(context.Context, string, string) error { return nil }),
			}
			err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", dependencies)
			if err == nil || !strings.Contains(err.Error(), "category: "+tc.wantCategory) {
				t.Fatalf("err = %v, want category %s", err, tc.wantCategory)
			}
			if !errors.Is(err, tc.queryErr) {
				t.Fatalf("err = %v does not wrap %v", err, tc.queryErr)
			}
			if len(store.failCodes) != 1 || store.failCodes[0] != tc.wantCode {
				t.Fatalf("stored failure codes = %v, want [%s]", store.failCodes, tc.wantCode)
			}
			if rendered != 0 || store.completed != 0 || store.notificationsCompleted != 0 {
				t.Fatalf("rendered=%d completed=%d notified=%d, want none", rendered, store.completed, store.notificationsCompleted)
			}
		})
	}
}
