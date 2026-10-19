//go:build integration

package syncadmin

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type stampedRow struct {
	at *time.Time
	by *string
}

func (v *selectionVenue) stamp(integration uuid.UUID, key string) stampedRow {
	v.t.Helper()
	var out stampedRow
	if err := v.pool.QueryRow(v.ctx, `SELECT state_changed_at, state_changed_by FROM integration_datasets WHERE integration_id = $1 AND dataset_key = $2`,
		integration, key).Scan(&out.at, &out.by); err != nil {
		v.t.Fatalf("read the stamp of %s: %v", key, err)
	}
	return out
}

func (s stampedRow) is(by string) bool { return s.at != nil && s.by != nil && *s.by == by }

// TestDatasetStateWritersStampTheirOwnName is T13 of the provenance changeset:
// a row a writer creates or switches carries that writer's name and a time,
// and a save that changes no row leaves the stamp alone.
func TestDatasetStateWritersStampTheirOwnName(t *testing.T) {
	v := startSelectionVenue(t)
	org := v.orgOn

	t.Run("config save: an added target stamps its rows, a removed one stamps the switch off", func(t *testing.T) {
		v.t = t
		prKeys := targetKeys(t, "github", "prs")
		integration := v.integration(org, "github", []string{"commits"}, nil)
		config := v.config(org, "github", integration, `["git"]`, false)
		for _, key := range prKeys {
			if _, ok := v.states(integration)[key]; ok {
				t.Fatalf("harness: %s already has a row", key)
			}
		}
		shown := v.get(org, config)
		v.save(org, config, listBody(append(append([]string{}, shown...), "prs"), ``))
		for _, key := range prKeys {
			if got := v.stamp(integration, key); !got.is(providersync.DatasetWriterConfigSave) {
				t.Errorf("added %s: stamp %+v, want %s and a time", key, got, providersync.DatasetWriterConfigSave)
			}
		}
		if got := v.stamp(integration, "commits"); got.at != nil || got.by != nil {
			t.Errorf("a row the save did not change has a stamp: %+v", got)
		}
		before := v.stamp(integration, prKeys[0])
		v.save(org, config, listBody(append(append([]string{}, shown...), "prs"), ``))
		if after := v.stamp(integration, prKeys[0]); after.at == nil || before.at == nil || !after.at.Equal(*before.at) {
			t.Errorf("a save that changed no row moved the stamp: %+v -> %+v", before, after)
		}
		v.save(org, config, listBody(shown, ``))
		for _, key := range prKeys {
			if got := v.stamp(integration, key); !got.is(providersync.DatasetWriterConfigSave) {
				t.Errorf("removed %s: stamp %+v, want %s and a time", key, got, providersync.DatasetWriterConfigSave)
			}
		}
	})

	t.Run("dataset endpoint: a created row and a switched row carry the endpoint name", func(t *testing.T) {
		v.t = t
		integration := v.integration(org, "github", nil, []string{"cicd"})
		v.datasetEndpoint(org, integration, "cicd", true)
		if got := v.stamp(integration, "cicd"); !got.is(providersync.DatasetWriterDatasetEndpoint) {
			t.Errorf("switched row: stamp %+v", got)
		}
		v.datasetEndpoint(org, integration, "prs", true)
		if got := v.stamp(integration, "prs"); !got.is(providersync.DatasetWriterDatasetEndpoint) {
			t.Errorf("created row: stamp %+v", got)
		}
	})
}
