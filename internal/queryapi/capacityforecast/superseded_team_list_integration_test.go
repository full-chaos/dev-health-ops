//go:build integration

package capacityforecast

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestForecastListHoldsNoSupersededTeam lists the stored forecasts of an
// organization. With no team named, the list holds the forecasts of the active
// teams and those with no team (org-wide); a forecast stored under a team id
// that is inactive now (a superseded or retired id) is not listed. A named team
// is read as named, also a retired one.
func TestForecastListHoldsNoSupersededTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const org = "org-9055"
	at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	teams := `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, generateUUIDv4(), ?, ?, 'linear', ?, ?, ?)`
	for _, team := range []struct {
		id     string
		active uint8
	}{{"team-active", 1}, {"team-retired", 0}} {
		if err := admin.Exec(ctx, teams, team.id, team.id, org, team.active, at, at); err != nil {
			t.Fatalf("insert team %s: %v", team.id, err)
		}
	}
	empty := ""
	active, retired := "team-active", "team-retired"
	forecast := `INSERT INTO capacity_forecasts
		(forecast_id, computed_at, team_id, backlog_size, history_days, simulation_count, throughput_mean, throughput_stddev, org_id)
		VALUES (?, ?, ?, 5, 30, 1000, 1.5, 0.5, ?)`
	for index, row := range []struct {
		id   string
		team *string
	}{
		{"f-active", &active},
		{"f-retired", &retired},
		{"f-org-null", nil},
		{"f-org-empty", &empty},
	} {
		if err := admin.Exec(ctx, forecast, row.id, at.Add(time.Duration(index)*time.Minute), row.team, org); err != nil {
			t.Fatalf("insert forecast %s: %v", row.id, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	list := func(filters *model.CapacityForecastFilterInput) []string {
		t.Helper()
		connection, err := ResolveForecasts(ctx, client, org, filters)
		if err != nil {
			t.Fatalf("ResolveForecasts(%+v): %v", filters, err)
		}
		var ids []string
		for _, edge := range connection.Edges {
			ids = append(ids, edge.Node.ForecastID)
		}
		sort.Strings(ids)
		return ids
	}

	if got, want := list(nil), []string{"f-active", "f-org-empty", "f-org-null"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("list with no team = %v, want %v (f-retired is stored under an inactive team id)", got, want)
	}
	if got, want := list(&model.CapacityForecastFilterInput{TeamID: &empty, Limit: 10}), []string{"f-active", "f-org-empty", "f-org-null"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("list with an empty team = %v, want %v (an empty team is no team named)", got, want)
	}
	if got, want := list(&model.CapacityForecastFilterInput{TeamID: &retired, Limit: 10}), []string{"f-retired"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("list of the named retired team = %v, want %v", got, want)
	}
}
