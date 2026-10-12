//go:build integration

package capacityforecast

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A retried partition re-inserts forecasts under fresh forecast_ids. The list
// must hold one row per slice (the newest) so the copies cannot fill the page.
func TestForecastListCollapsesRetryDuplicatesBeforeLimit(t *testing.T) {
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

	const org = "org-4151"
	at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	insert := `INSERT INTO capacity_forecasts
		(forecast_id, computed_at, team_id, backlog_size, history_days, simulation_count, throughput_mean, throughput_stddev, org_id)
		VALUES (?, ?, NULL, 5, 30, 1000, 1.5, 0.5, ?)`
	// Org-wide slice of day 1 written three times (two retries), the slice of
	// day 0 once, and day 2 once. Newest first: d2, d1-retry2, d1-retry1, d1, d0.
	for _, row := range []struct {
		id string
		at time.Time
	}{
		{"d0", at},
		{"d1", at.Add(24 * time.Hour)},
		{"d1-retry1", at.Add(24*time.Hour + 5*time.Minute)},
		{"d1-retry2", at.Add(24*time.Hour + 10*time.Minute)},
		{"d2", at.Add(48 * time.Hour)},
	} {
		if err := admin.Exec(ctx, insert, row.id, row.at, org); err != nil {
			t.Fatalf("insert forecast %s: %v", row.id, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	connection, err := ResolveForecasts(ctx, client, org, &model.CapacityForecastFilterInput{Limit: 3})
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	var got []string
	for _, edge := range connection.Edges {
		got = append(got, edge.Node.ForecastID)
	}
	if want := []string{"d2", "d1-retry2", "d0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("forecast list = %v, want %v (retry copies must not consume the LIMIT)", got, want)
	}
}
