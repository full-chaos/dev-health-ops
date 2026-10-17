//go:build integration

package synchandoff

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestMintRefusesAnEnabledSourceTheConfigurationDoesNotSelect: Mint is the only
// writer of sync_manual_triggers, so its scope guard covers every trigger
// path. A list naming an enabled source the planner-managed configuration does
// not select is refused before anything is written, however the id is spelled;
// selected, disabled and unknown ids and a nil list pass; a configuration
// pinned to one source, a non-planner-managed one and PagerDuty are not checked.
func TestMintRefusesAnEnabledSourceTheConfigurationDoesNotSelect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	pool, config := startHandoffDatabase(ctx, t, 4)
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	selected, untagged, otherConfig, disabled := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tag := func(configID string) string { return fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, configID) }
	for _, source := range []struct {
		id       uuid.UUID
		metadata string
		enabled  bool
	}{
		{selected, tag(config.ID), true},
		{untagged, `{}`, true},
		{otherConfig, tag(uuid.NewString()), true},
		{disabled, tag(config.ID), false},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3::uuid, 'github', 'repository', $4, $4, $4, $5::json, $6, now(), now())`,
			source.id, config.OrgID, *config.IntegrationID, "acme/"+source.id.String()[:8], source.metadata, source.enabled); err != nil {
			t.Fatal(err)
		}
	}
	mint := func(c Config, ids []string, offset int) (Trigger, error) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		trigger, err := Mint(ctx, tx, &c, MintInput{Mode: "backfill", SourceIDs: ids, TriggeredBy: "backfill", Caller: "guard-test"}, now.Add(time.Duration(offset)*time.Second))
		if err == nil {
			var written int
			if scanErr := tx.QueryRow(ctx, `SELECT count(*) FROM sync_manual_triggers WHERE occurrence_id = $1`, trigger.OccurrenceID).Scan(&written); scanErr != nil || written != 1 {
				t.Fatalf("an accepted mint wrote %d trigger rows (err %v)", written, scanErr)
			}
		}
		return trigger, err
	}

	for index, ids := range map[string][]string{
		"untagged":              {selected.String(), untagged.String()},
		"tagged for another":    {otherConfig.String()},
		"untagged, upper-case":  {strings.ToUpper(untagged.String())},
		"untagged, with braces": {"{" + untagged.String() + "}"},
	} {
		_, err := mint(*config, ids, len(index))
		var unselected UnselectedSourcesError
		if !errors.As(err, &unselected) || unselected.ConfigID != config.ID || len(unselected.SourceIDs) != 1 {
			t.Errorf("%s: err %v, want an UnselectedSourcesError naming one source", index, err)
		}
	}
	warned := strings.Count(logs.String(), `"msg":"sync trigger refused: a source is not selected for the configuration"`)
	if warned != 4 || !strings.Contains(logs.String(), `"caller":"guard-test"`) || !strings.Contains(logs.String(), `"refused_source_count":1`) ||
		!strings.Contains(logs.String(), `"sync_config_id":"`+config.ID+`"`) || !strings.Contains(logs.String(), `"level":"WARN"`) {
		t.Fatalf("refusal log lines = %d, want 4 WARN lines naming the configuration, the caller and the count:\n%s", warned, logs.String())
	}
	var refusedWrites int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_manual_triggers`).Scan(&refusedWrites); err != nil || refusedWrites != 0 {
		t.Fatalf("refused mints left %d trigger rows (err %v)", refusedWrites, err)
	}

	pinned := *config
	pinnedSource := selected.String()
	pinned.SourceID = &pinnedSource
	notManaged := *config
	notManaged.PlannerManaged = false
	pagerDuty := *config
	pagerDuty.Provider = "PagerDuty"
	for offset, c := range []struct {
		name   string
		config Config
		ids    []string
	}{
		{"selected", *config, []string{selected.String()}},
		{"disabled and unknown", *config, []string{disabled.String(), uuid.NewString(), "not-a-uuid"}},
		{"nil list", *config, nil},
		{"empty list", *config, []string{}},
		{"pinned configuration", pinned, []string{untagged.String()}},
		{"not planner-managed", notManaged, []string{untagged.String()}},
		{"PagerDuty", pagerDuty, []string{untagged.String()}},
	} {
		if _, err := mint(c.config, c.ids, 100+offset); err != nil {
			t.Errorf("%s: err %v, want the trigger written", c.name, err)
		}
	}
}
