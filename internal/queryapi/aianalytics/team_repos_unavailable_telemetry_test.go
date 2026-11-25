package aianalytics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

const (
	unavailableOrgID  = "org-synthetic-14677"
	unavailableTeamID = "team-synthetic-14677"
	unavailableRepoID = "14677aaa-0000-4000-8000-00000000000a"
)

// teamReadFailingClient answers the repository catalogue with one repository and fails the read failAt names, with an error
// whose text quotes the org, team and repository ids the way a driver message can quote a statement.
type teamReadFailingClient struct{ failAt string }

func (c teamReadFailingClient) Query(_ context.Context, st string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	leak := errors.New("statement failed: org_id = '" + unavailableOrgID + "' AND team_id IN ['" + unavailableTeamID + "'] AND repo_id = '" + unavailableRepoID + "'")
	switch {
	case strings.Contains(st, teamscope.Marker):
		if c.failAt == "ownership" {
			return nil, leak
		}
	case strings.Contains(st, "AS full_name"):
		if c.failAt == "repos" {
			return nil, leak
		}
		return &scriptedRows{rows: [][]any{{unavailableRepoID, "acme/alpha"}}}, nil
	}
	return nil, errors.New("teamReadFailingClient: unscripted statement")
}

// CHAOS-14677: an unreadable team ownership keeps its WARN (event name unchanged), logs the error class and Go type only, never
// the error text with its ids, and counts once per failed read by operation and read.
func TestTeamReposUnavailableLogsTheClassOnlyAndCounts(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	reader := sdkmetric.NewManualReader()
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })

	for _, read := range []string{"repos", "ownership"} {
		ids, ok := teamRepoIDs(context.Background(), teamReadFailingClient{failAt: read}, unavailableOrgID, unavailableTeamID, "aiAttributionOverview")
		if ok || ids != nil {
			t.Fatalf("read %s failed: teamRepoIDs = %v, ok %v; want nil, false", read, ids, ok)
		}
	}

	out := logs.String()
	for _, read := range []string{"read=repos", "read=ownership"} {
		line := ""
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, "query_api.ai_analytics.team_repos_unavailable") && strings.Contains(l, read) {
				line = l
			}
		}
		if line == "" {
			t.Fatalf("no team_repos_unavailable WARN with %s, logs:\n%s", read, out)
		}
		for _, want := range []string{"level=WARN", "operation=aiAttributionOverview", "error.class=", "error.type="} {
			if !strings.Contains(line, want) {
				t.Errorf("%s line lacks %q: %s", read, want, line)
			}
		}
	}
	for _, id := range []string{unavailableOrgID, unavailableTeamID, unavailableRepoID, "statement failed"} {
		if strings.Contains(out, id) {
			t.Fatalf("the error text reached the logs (%q):\n%s", id, out)
		}
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, sm := range collected.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "devhealth_query_api_ai_analytics_team_repos_unavailable_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric data is %T, want Sum[int64]", m.Data)
			}
			for _, point := range sum.DataPoints {
				op, _ := point.Attributes.Value("operation")
				read, _ := point.Attributes.Value("read")
				counts[op.AsString()+"/"+read.AsString()] += point.Value
			}
		}
	}
	if counts["aiAttributionOverview/repos"] != 1 || counts["aiAttributionOverview/ownership"] != 1 || len(counts) != 2 {
		t.Fatalf("team_repos_unavailable counts = %v, want aiAttributionOverview/repos=1 and aiAttributionOverview/ownership=1", counts)
	}
}
