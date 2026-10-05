package throughputforecast

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-8727: a requested team answers only through ownership rows.

func TestResolveATeamWithNoOwnershipRowsIsMissingAndReadsNothingElse(t *testing.T) {
	client := &fakeClient{owners: []string{}}
	got, err := Resolve(context.Background(), client, "org-7", model.ThroughputForecastInput{TeamIds: []string{"team-unowned"}, HistoryWeeks: 8}, mustDay(t, "2026-09-01"))
	if err != nil || got != nil {
		t.Fatalf("an unowned team: forecast %v, err %v; want a nil forecast", got, err)
	}
	if len(client.statements) != 0 {
		t.Fatalf("a team_id-keyed table was read for an unowned team: %d statements", len(client.statements))
	}
	if org, _ := bindingValueOf(client.ownershipBindings[0], teamscope.BindingOrgID); org != "org-7" {
		t.Fatalf("the ownership read is bound to org %v, want org-7", org)
	}
}

func TestResolveReadsOnlyTheOwnedTeamsOfSeveral(t *testing.T) {
	client := &fakeClient{owners: []string{"team-b", "team-a"}, errs: nil, responses: []*fakeRowScanner{{}}}
	_, _ = Resolve(context.Background(), client, "org-7", model.ThroughputForecastInput{TeamIds: []string{"team-a", "team-unowned", "team-b"}, HistoryWeeks: 8}, mustDay(t, "2026-09-01"))
	if len(client.bindings) == 0 {
		t.Fatal("the owned teams were not read")
	}
	value, _ := bindingValueOf(client.bindings[0], "team_ids")
	ids, _ := value.([]string)
	if len(ids) != 2 || ids[0] != "team-a" || ids[1] != "team-b" {
		t.Fatalf("throughput read team_ids %v, want the owned teams in requested order [team-a team-b]", value)
	}
}

func TestResolveWithNoTeamReadsNoOwnership(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{}}}
	_, _ = Resolve(context.Background(), client, "org-7", model.ThroughputForecastInput{HistoryWeeks: 8}, mustDay(t, "2026-09-01"))
	if len(client.ownershipBindings) != 0 {
		t.Fatalf("an org-wide request read ownership: %v", client.ownershipBindings)
	}
}

func TestResolveFailsClosedWhenTheOwnershipReadFails(t *testing.T) {
	_, err := Resolve(context.Background(), failingClient{}, "org-7", model.ThroughputForecastInput{TeamIds: []string{"team-a"}, HistoryWeeks: 8}, mustDay(t, "2026-09-01"))
	if err == nil {
		t.Fatal("a failed ownership read was treated as an answer")
	}
}

type failingClient struct{}

func (failingClient) Query(context.Context, string, []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return nil, errors.New("boom")
}

func bindingValueOf(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

// Only the ownership read fails; every other read would answer. The resolver must stop with an error: an
// unscoped or team_id-keyed answer from a failed ownership check would be fail-open.
func TestResolveFailsClosedWhenOnlyTheOwnershipReadFails(t *testing.T) {
	client := &fakeClient{ownershipErr: errors.New("ownership down"), responses: []*fakeRowScanner{{}, {}, {}, {}, {}, {}, {}}}
	got, err := Resolve(context.Background(), client, "org-7", model.ThroughputForecastInput{TeamIds: []string{"team-a"}, HistoryWeeks: 8}, mustDay(t, "2026-09-01"))
	if err == nil || got != nil {
		t.Fatalf("forecast %v, err %v; want an error and no forecast", got, err)
	}
	if len(client.statements) != 0 {
		t.Fatalf("a team_id-keyed table was read after the ownership read failed: %d statements", len(client.statements))
	}
}
