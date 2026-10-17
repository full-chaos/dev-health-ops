//go:build integration

package backfillrun

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// TestBackfillPlansTheScheduledSourceSet: for one planner-managed configuration,
// a backfill that names no sources plans exactly the sources a scheduled run of
// the configuration plans (CHAOS-10064, chris 2026-10-17: for one window, a
// backfill covers the hourly run's scope). Enabled sources the configuration
// does not select (untagged, or tagged for another configuration) and disabled
// ones are out of that set, and an explicit id of an unselected enabled source
// is refused with nothing started.
func TestBackfillPlansTheScheduledSourceSet(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	integration, config := uuidN(0x1a, 900), uuidN(0xc0, 900)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'parity', '{}'::json, true, now(), now())`, integration, testOrg)
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'parity', 'github', '["prs"]'::json, '{}'::json, true, true, $3::uuid, now(), now())`, config, testOrg, integration)
	exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES (gen_random_uuid(), $1, $2::uuid, 'prs', true, '{}'::json)`, testOrg, integration)
	tag := func(configID string) string { return fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, configID) }
	sources := []struct {
		name, metadata string
		enabled        bool
	}{
		{"acme/a-selected", tag(config), true},
		{"acme/b-selected", tag(config), true},
		{"acme/c-untagged", `{}`, true},
		{"acme/d-selected-disabled", tag(config), false},
		{"acme/e-other-config", tag(uuidN(0xc0, 901)), true},
	}
	ids := map[string]string{}
	for index, source := range sources {
		id := uuidN(0x5a, 9000+index)
		ids[source.name] = id
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'github', 'repository', $4, $4, $4, $5::json, $6, now(), now())`, id, testOrg, integration, source.name, source.metadata, source.enabled)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	cfg, err := LoadConfig(ctx, tx, config)
	if err != nil || cfg == nil {
		t.Fatalf("load config: %v", err)
	}
	scheduled, err := schedsync.ScheduledSourceIDs(ctx, tx, testOrg, integration, config, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{ids["acme/a-selected"], ids["acme/b-selected"]}
	if !reflect.DeepEqual(scheduled, want) {
		t.Fatalf("scheduled source set = %v, want the two selected enabled sources %v", scheduled, want)
	}

	validated, err := Validate(ctx, tx, cfg, Params{ConfigID: config})
	if err != nil {
		t.Fatalf("Validate with no --source-ids: %v", err)
	}
	if !reflect.DeepEqual(validated.SourceIDs, scheduled) {
		t.Fatalf("backfill source set = %v, scheduled source set = %v: they must be equal", validated.SourceIDs, scheduled)
	}
	if validated.SelectedSources != 2 || validated.EnabledSources != 4 {
		t.Fatalf("selected %d enabled %d, want 2 and 4", validated.SelectedSources, validated.EnabledSources)
	}

	// An explicit unselected source is refused when the trigger is written
	// (synchandoff.Mint, the one scope guard), and nothing is written.
	window := Window{Since: mustDay(t, "2026-09-01"), Before: mustDay(t, "2026-09-02")}
	for _, name := range []string{"acme/c-untagged", "acme/e-other-config"} {
		params := Params{ConfigID: config, Window: window, SourceIDs: []string{ids["acme/a-selected"], ids[name]}}
		explicit, err := Validate(ctx, tx, cfg, params)
		if err != nil {
			t.Fatalf("Validate explicit %s: %v", name, err)
		}
		_, err = Mint(ctx, tx, cfg, params, explicit, mustDay(t, "2026-10-17"))
		var refused RefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Message, ids[name]) || !strings.Contains(refused.Message, "does not select") {
			t.Fatalf("explicit %s: err %v, want a refusal naming it", name, err)
		}
	}
	// The trigger write's guard accepts exactly the scheduled set: each enabled
	// source alone is written iff a scheduled run plans it.
	for index, source := range sources {
		if !source.enabled {
			continue
		}
		params := Params{ConfigID: config, Window: window, SourceIDs: []string{ids[source.name]}}
		one, err := Validate(ctx, tx, cfg, params)
		if err != nil {
			t.Fatalf("Validate %s: %v", source.name, err)
		}
		if _, err := tx.Exec(ctx, `SAVEPOINT parity`); err != nil {
			t.Fatal(err)
		}
		_, mintErr := Mint(ctx, tx, cfg, params, one, mustDay(t, "2026-10-17").Add(time.Duration(index+1)*time.Hour))
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT parity`); err != nil {
			t.Fatal(err)
		}
		if accepted, planned := mintErr == nil, slices.Contains(scheduled, ids[source.name]); accepted != planned {
			t.Fatalf("%s: written %v (err %v), planned by a scheduled run %v: they must agree", source.name, accepted, mintErr, planned)
		}
	}
	var written int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM sync_manual_triggers`).Scan(&written); err != nil || written != 0 {
		t.Fatalf("a refused trigger wrote %d rows (err %v)", written, err)
	}
	params := Params{ConfigID: config, Window: window, SourceIDs: []string{ids["acme/b-selected"]}}
	explicit, err := Validate(ctx, tx, cfg, params)
	if err != nil || !reflect.DeepEqual(explicit.SourceIDs, []string{ids["acme/b-selected"]}) {
		t.Fatalf("explicit selected source: %v, %v", explicit.SourceIDs, err)
	}
	if _, err := Mint(ctx, tx, cfg, params, explicit, mustDay(t, "2026-10-17")); err != nil {
		t.Fatalf("Mint an explicit selected source: %v", err)
	}
}

func mustDay(t *testing.T, day string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", day)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
