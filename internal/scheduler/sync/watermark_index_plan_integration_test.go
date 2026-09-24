//go:build integration

package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// referencePlanWatermarks is loadPlanWatermarks as it was before the
// three-tier loop moved into the shared WatermarkIndex, kept verbatim
// test-side as the reference the refactor is measured against.
func referencePlanWatermarks(ctx context.Context, tx pgx.Tx, orgID string, sources []PlanSource, datasets []PlanDataset) (map[WatermarkKey]time.Time, error) {
	result := make(map[WatermarkKey]time.Time)
	rows, err := tx.Query(ctx, `
SELECT source_id,dataset_key,repo_id,target,last_synced_at
FROM public.sync_watermarks
WHERE org_id=$1 AND last_synced_at IS NOT NULL`, orgID)
	if err != nil {
		return nil, fmt.Errorf("load scheduled sync watermarks: %w", err)
	}
	defer rows.Close()
	type watermarkRow struct {
		sourceID string
		dataset  string
		repoID   string
		target   string
		at       time.Time
	}
	var loaded []watermarkRow
	for rows.Next() {
		var row watermarkRow
		if err := rows.Scan(&row.sourceID, &row.dataset, &row.repoID, &row.target, &row.at); err != nil {
			return nil, err
		}
		loaded = append(loaded, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, source := range sources {
		for _, dataset := range datasets {
			key := WatermarkKey{SourceID: source.ExternalID, Dataset: dataset.Key}
			for _, row := range loaded {
				if row.sourceID == source.ExternalID && row.dataset == dataset.Key {
					result[key] = row.at.UTC()
					break
				}
			}
			if _, ok := result[key]; ok {
				continue
			}
			for _, row := range loaded {
				if row.repoID == source.ExternalID && row.target == dataset.Key {
					result[key] = row.at.UTC()
					break
				}
			}
			if _, ok := result[key]; ok {
				continue
			}
			for _, legacy := range legacyTargetsByDataset[dataset.Key] {
				for _, row := range loaded {
					if row.repoID == source.ExternalID && row.target == legacy && row.dataset == legacy {
						result[key] = row.at.UTC()
						break
					}
				}
				if _, ok := result[key]; ok {
					break
				}
			}
		}
	}
	return result, nil
}

// TestSharedWatermarkIndexLeavesScheduledPlansUnchanged loads the same
// watermark rows through the pre-refactor loop and through the shared
// WatermarkIndex, for every provider's full dataset set with rows in every
// tier (NULL rows too), and requires identical scheduled plans in both
// modes. The watermark maps themselves may differ only for datasets the
// planner never reads a watermark for: the index drops the raw-legacy
// fallback for no-watermark datasets, as watermarks.get_watermark does.
func TestSharedWatermarkIndexLeavesScheduledPlansUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE public.sync_watermarks (
 org_id text NOT NULL, source_id text NOT NULL, dataset_key text NOT NULL,
 repo_id text NOT NULL, target text NOT NULL, last_synced_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	insert := func(org, source, dataset, repo, target string, at *time.Time) {
		if _, err := pool.Exec(ctx, `INSERT INTO sync_watermarks VALUES ($1,$2,$3,$4,$5,$6)`, org, source, dataset, repo, target, at); err != nil {
			t.Fatal(err)
		}
	}
	day := func(n int) *time.Time { at := now.Add(-time.Duration(n) * 24 * time.Hour); return &at }
	digest := sha256.New()
	mapDiffs, datasetsCompared := 0, 0
	for _, provider := range providersync.MatrixProviders() {
		org := "org-" + provider
		var datasets []PlanDataset
		for index, capability := range providersync.Capabilities(provider) {
			datasets = append(datasets, PlanDataset{Key: capability.Dataset})
			// Source A: canonical row; source B: legacy target column;
			// source C: raw legacy row per legacy target; source D: a NULL
			// canonical row over a dated legacy-target row.
			insert(org, "A", capability.Dataset, "rA-"+capability.Dataset, "tA-"+capability.Dataset, day(index+1))
			insert(org, "xB-"+capability.Dataset, "xB", "B", capability.Dataset, day(index+2))
			for _, legacy := range capability.LegacyTargets {
				insert(org, "xC-"+legacy, legacy, "C", legacy, day(index+3))
			}
			insert(org, "D", capability.Dataset, "rD-"+capability.Dataset, "tD-"+capability.Dataset, nil)
			insert(org, "xD-"+capability.Dataset, "xD", "D", capability.Dataset, day(index+4))
			// Source E: a canonical and a legacy-target row (tier order);
			// source F: a legacy-target and a raw legacy row.
			insert(org, "E", capability.Dataset, "rE-"+capability.Dataset, "tE-"+capability.Dataset, day(index+5))
			insert(org, "xE-"+capability.Dataset, "xE", "E", capability.Dataset, day(index+6))
			insert(org, "xF-"+capability.Dataset, "xF", "F", capability.Dataset, day(index+7))
			for _, legacy := range capability.LegacyTargets {
				insert(org, "xF-"+legacy, legacy, "F", legacy, day(index+8))
			}
		}
		sources := []PlanSource{
			{ID: "source-a", ExternalID: "A", Provider: provider, FullName: "A"},
			{ID: "source-b", ExternalID: "B", Provider: provider, FullName: "B"},
			{ID: "source-c", ExternalID: "C", Provider: provider, FullName: "C"},
			{ID: "source-d", ExternalID: "D", Provider: provider, FullName: "D"},
			{ID: "source-e", ExternalID: "E", Provider: provider, FullName: "E"},
			{ID: "source-f", ExternalID: "F", Provider: provider, FullName: "F"},
		}
		load := func(loader func(context.Context, pgx.Tx, string, []PlanSource, []PlanDataset) (map[WatermarkKey]time.Time, error)) map[WatermarkKey]time.Time {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			watermarks, err := loader(ctx, tx, org, sources, datasets)
			if err != nil {
				t.Fatal(err)
			}
			return watermarks
		}
		before, after := load(referencePlanWatermarks), load(loadPlanWatermarks)
		for key, at := range before {
			if other, ok := after[key]; !ok || !other.Equal(at) {
				mapDiffs++
				if providersync.DatasetWatermark(key.Dataset) == providersync.WatermarkIncremental {
					t.Errorf("%s %v: incremental watermark changed: %v -> %v (present %v)", provider, key, at, other, ok)
				}
			}
		}
		for key := range after {
			if _, ok := before[key]; !ok {
				t.Errorf("%s %v: watermark appeared", provider, key)
			}
		}
		for _, mode := range []string{SyncModeIncremental, SyncModeFullResync} {
			plan := func(watermarks map[WatermarkKey]time.Time) string {
				units, err := BuildScheduledPlan(PlannerInput{OrgID: org, IntegrationID: "integration", Mode: mode, Now: now,
					Sources: sources, Datasets: datasets, Watermarks: watermarks})
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(units)
				if err != nil {
					t.Fatal(err)
				}
				return string(encoded)
			}
			old, current := plan(before), plan(after)
			if old != current {
				t.Errorf("%s %s: plans differ\n before %s\n after  %s", provider, mode, old, current)
			}
			fmt.Fprintf(digest, "%s/%s:%s\n", provider, mode, current)
		}
		datasetsCompared += len(datasets)
	}
	if mapDiffs == 0 {
		t.Fatal("no watermark differs between the loaders: the fixture never reaches the no-watermark raw-legacy tier the index changes")
	}
	t.Logf("%d provider datasets, %d no-watermark map differences, plans identical; plan digest %s",
		datasetsCompared, mapDiffs, hex.EncodeToString(digest.Sum(nil)))
}
