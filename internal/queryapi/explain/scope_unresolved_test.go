package explain

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type rn13705Rows struct {
	hits  []string
	index int
}

func (r *rn13705Rows) Next() bool { r.index++; return r.index <= len(r.hits) }
func (r *rn13705Rows) Scan(dest ...any) error {
	if p, ok := dest[0].(*string); ok {
		*p = r.hits[r.index-1]
	}
	return nil
}
func (r *rn13705Rows) Err() error   { return nil }
func (r *rn13705Rows) Close() error { return nil }

type rn13705Client struct {
	knownRepo  bool
	statements []string
}

func (c *rn13705Client) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	if c.knownRepo && strings.Contains(statement, "id_count") {
		return &rn13705Rows{hits: []string{"repo-1"}}, nil
	}
	return &rn13705Rows{}, nil
}

func explainParams(metric, level string, ids ...string) Params {
	end := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)
	return Params{Metric: metric, StartDay: end.AddDate(0, 0, -7), EndDay: end, CompareStart: end.AddDate(0, 0, -14), CompareEnd: end.AddDate(0, 0, -7), ScopeLevel: level, ScopeIDs: ids}
}

// metricReads counts the reads of the metric table, split by whether they carry AND 1 = 0.
func metricReads(c *rn13705Client, table string) (empty, unfiltered int) {
	for _, s := range c.statements {
		if !strings.Contains(s, table) {
			continue
		}
		if strings.Contains(s, "AND 1 = 0") {
			empty++
		} else {
			unfiltered++
		}
	}
	return empty, unfiltered
}

// An unresolved repository, service or developer scope reads nothing on a team-keyed and a repo-keyed metric (CHAOS-13705, R335).
func TestAnUnresolvedScopeReadsNothingOnEveryMetricScope(t *testing.T) {
	for _, metric := range []struct{ name, table string }{{"cycle_time", "work_item_metrics_daily"}, {"review_latency", "repo_metrics_daily"}} {
		for _, level := range []string{"repo", "service", "developer"} {
			c := &rn13705Client{}
			reader, err := NewReader(c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := BuildExplainResponse(context.Background(), reader, "org-a", explainParams(metric.name, level, "nope")); err != nil {
				t.Fatalf("%s %s: %v", metric.name, level, err)
			}
			if empty, open := metricReads(c, metric.table); empty == 0 || open != 0 {
				t.Errorf("%s %s: %d reads with AND 1 = 0, %d without, want some with and none without", metric.name, level, empty, open)
			}
		}
	}
}

// A known repo on a team-keyed metric keeps today's answer (no filter); an org scope reads as before.
func TestAKnownRepoOnATeamMetricAndAnOrgScopeKeepTheirAnswer(t *testing.T) {
	c := &rn13705Client{knownRepo: true}
	reader, _ := NewReader(c)
	if _, err := BuildExplainResponse(context.Background(), reader, "org-a", explainParams("cycle_time", "repo", "repo-1")); err != nil {
		t.Fatal(err)
	}
	if empty, open := metricReads(c, "work_item_metrics_daily"); empty != 0 || open == 0 {
		t.Errorf("known repo: %d reads with AND 1 = 0, %d without, want none with", empty, open)
	}
	c = &rn13705Client{}
	reader, _ = NewReader(c)
	if _, err := BuildExplainResponse(context.Background(), reader, "org-a", explainParams("cycle_time", "org")); err != nil {
		t.Fatal(err)
	}
	if empty, _ := metricReads(c, "work_item_metrics_daily"); empty != 0 {
		t.Errorf("org scope: %d reads carry AND 1 = 0", empty)
	}
}
