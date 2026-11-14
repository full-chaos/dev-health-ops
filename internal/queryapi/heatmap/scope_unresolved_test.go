package heatmap

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type rn13705Rows struct{}

func (rn13705Rows) Next() bool        { return false }
func (rn13705Rows) Scan(...any) error { return nil }
func (rn13705Rows) Err() error        { return nil }
func (rn13705Rows) Close() error      { return nil }

type rn13705Client struct{ statements []string }

func (c *rn13705Client) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	return rn13705Rows{}, nil
}

func (c *rn13705Client) filtered() (withNothing, without int) {
	for _, s := range c.statements {
		if !strings.Contains(s, "max_execution_time") {
			continue
		}
		if strings.Contains(s, "AND 1 = 0") {
			withNothing++
		} else {
			without++
		}
	}
	return withNothing, without
}

// A service scope that names an id answers nothing: every metric read carries AND 1 = 0 (CHAOS-13705, R335). An org scope reads as before.
func TestAServiceScopeThatNamesAnIDReadsNothing(t *testing.T) {
	for _, metric := range []struct{ typ, name string }{{"temporal_load", "review_wait_density"}, {"context_switch", "repo_touchpoints"}, {"risk", "hotspot_risk"}} {
		c := &rn13705Client{}
		if _, err := BuildResponse(context.Background(), c, "org-a", Params{Type: metric.typ, Metric: metric.name, ScopeType: "service", ScopeID: "svc-1", RangeDays: 30, Limit: 50}); err != nil {
			t.Fatalf("%s: %v", metric.typ, err)
		}
		if with, without := c.filtered(); with == 0 || without != 0 {
			t.Errorf("%s: %d reads with AND 1 = 0, %d metric reads without, want some with and none without", metric.typ, with, without)
		}
	}
	c := &rn13705Client{}
	if _, err := BuildResponse(context.Background(), c, "org-a", Params{Type: "context_switch", Metric: "repo_touchpoints", ScopeType: "org", RangeDays: 30, Limit: 50}); err != nil {
		t.Fatal(err)
	}
	if with, _ := c.filtered(); with != 0 {
		t.Errorf("org scope: %d reads carry AND 1 = 0", with)
	}
}

// An unresolved service scope says service_not_found on the served answer (CHAOS-13707); an org scope serves no reason.
func TestAnUnresolvedServiceScopeServesServiceNotFound(t *testing.T) {
	for _, metric := range []struct{ typ, name string }{{"temporal_load", "review_wait_density"}, {"context_switch", "repo_touchpoints"}, {"risk", "hotspot_risk"}} {
		resp, err := BuildResponse(context.Background(), &rn13705Client{}, "org-a", Params{Type: metric.typ, Metric: metric.name, ScopeType: "service", ScopeID: "svc-1", RangeDays: 30, Limit: 50})
		if err != nil {
			t.Fatalf("%s: %v", metric.typ, err)
		}
		if resp.FilterEmptyReason == nil || *resp.FilterEmptyReason != "service_not_found" {
			t.Errorf("%s: filter_empty_reason %v, want service_not_found", metric.typ, resp.FilterEmptyReason)
		}
	}
	resp, err := BuildResponse(context.Background(), &rn13705Client{}, "org-a", Params{Type: "context_switch", Metric: "repo_touchpoints", ScopeType: "org", RangeDays: 30, Limit: 50})
	if err != nil || resp.FilterEmptyReason != nil {
		t.Errorf("org scope: reason %v err %v, want null", resp.FilterEmptyReason, err)
	}
}
