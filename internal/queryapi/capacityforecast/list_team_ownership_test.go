package capacityforecast

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-8727: the capacityForecasts list answers a requested team only through ownership rows.

func TestResolveForecastsATeamWithNoOwnershipRowsIsAnEmptyConnectionAndReadsNoForecasts(t *testing.T) {
	team := "team-unowned"
	client := &fakeClient{owners: []string{}}
	got, err := ResolveForecasts(context.Background(), client, "org-7", &model.CapacityForecastFilterInput{TeamID: &team, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Edges == nil || len(got.Edges) != 0 || got.PageInfo == nil || got.TotalCount != 0 {
		t.Fatalf("connection %+v, want an empty, non-nil one", got)
	}
	if len(client.statements) != 0 {
		t.Fatalf("capacity_forecasts was read for an unowned team: %v", client.statements)
	}
	if org, _ := bindingValue(client.ownershipBindings[0], teamscope.BindingOrgID); org != "org-7" {
		t.Fatalf("the ownership read is bound to org %v, want org-7", org)
	}
}

func TestResolveForecastsAnOwnedTeamIsReadAndNoTeamReadsNoOwnership(t *testing.T) {
	team := "team-a"
	owned := &fakeClient{owners: []string{"team-a"}, responses: []*fakeRowScanner{{}}}
	if _, err := ResolveForecasts(context.Background(), owned, "org-7", &model.CapacityForecastFilterInput{TeamID: &team, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if len(owned.statements) != 1 {
		t.Fatalf("an owned team: %d forecast reads, want 1", len(owned.statements))
	}
	orgWide := &fakeClient{responses: []*fakeRowScanner{{}}}
	if _, err := ResolveForecasts(context.Background(), orgWide, "org-7", &model.CapacityForecastFilterInput{Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if len(orgWide.ownershipBindings) != 0 || len(orgWide.statements) != 1 {
		t.Fatalf("an unfiltered list: ownership reads %d, forecast reads %d; want 0 and 1", len(orgWide.ownershipBindings), len(orgWide.statements))
	}
}

func TestResolveForecastsFailsClosedWhenOnlyTheOwnershipReadFails(t *testing.T) {
	team := "team-a"
	client := &fakeClient{ownershipErr: errors.New("ownership down"), responses: []*fakeRowScanner{{}}}
	got, err := ResolveForecasts(context.Background(), client, "org-7", &model.CapacityForecastFilterInput{TeamID: &team, Limit: 10})
	if err == nil || got != nil {
		t.Fatalf("connection %v, err %v; want an error and no connection", got, err)
	}
	if len(client.statements) != 0 {
		t.Fatalf("capacity_forecasts was read after the ownership read failed: %v", client.statements)
	}
}
