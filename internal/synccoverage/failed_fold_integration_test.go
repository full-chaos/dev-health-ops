//go:build integration

package synccoverage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestFailedFoldStaysLinearAndKeepsThePayload holds the per-row fold of streamCompactWindows to a cost that does not
// grow with the pair's open failed windows (CHAOS-14167: the whole fold ran inside the rebuild transaction, and every
// row re-sorted the pair's failed list), and proves the fold and the payload are byte-equal to the fold as it was,
// kept below as referenceStreamCompactWindows.
func TestFailedFoldStaysLinearAndKeepsThePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	now := time.Date(2026, time.November, 19, 0, 0, 0, 0, time.UTC)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}

	small := seedFoldVolume(t, ctx, pool, "org-fold-small", 90, "9 hours 30 minutes", now)
	large := seedFoldVolume(t, ctx, pool, "org-fold-large", 900, "9 hours 30 minutes", now)
	overlapping := seedFoldVolume(t, ctx, pool, "org-fold-overlap", 700, "18 hours 30 minutes", now)

	t.Run("allocated bytes per row do not grow with the open failed windows", func(t *testing.T) {
		smallPerRow, smallRows := foldAllocatedBytesPerRow(t, ctx, pool, small, now)
		largePerRow, largeRows := foldAllocatedBytesPerRow(t, ctx, pool, large, now)
		ratio := largePerRow / smallPerRow
		t.Logf("fold allocated bytes per row: %d rows %.0f, %d rows %.0f, ratio %.2f", smallRows, smallPerRow, largeRows, largePerRow, ratio)
		if ratio > 1.5 {
			t.Fatalf("fold allocated bytes per row grow with volume: %.0f at %d rows, %.0f at %d rows (ratio %.2f, want <= 1.5)",
				smallPerRow, smallRows, largePerRow, largeRows, ratio)
		}
	})

	t.Run("payload is byte-equal to the reference fold", func(t *testing.T) {
		for _, fixture := range []foldVolume{small, large, overlapping} {
			got, err := projector.Rebuild(ctx, fixture.OrgID, fixture.ConfigID)
			if err != nil {
				t.Fatal(err)
			}
			want := referencePayload(t, ctx, pool, fixture, now)
			if !bytes.Equal(got, want) {
				t.Fatalf("%s: payload differs from the reference fold (%d bytes, want %d)", fixture.OrgID, len(got), len(want))
			}
			t.Logf("%s: payload %d bytes equal to the reference fold", fixture.OrgID, len(got))
		}
	})

	t.Run("random unit sequences fold like the reference", func(t *testing.T) {
		compareRandomSequences(t, ctx, pool, now, 10_000)
	})
}

type foldVolume struct {
	OrgID         string
	ConfigID      uuid.UUID
	IntegrationID uuid.UUID
}

// seedFoldVolume seeds one github config: 20 sources, 7 raw dataset keys (14 scope keys), one run every 9 h, a unit
// per source and key per run, about 3.5% of them failed and a tenth of cicd units with an artifact_expired report.
// A 9h30m window leaves each failed window open (the pair's failed list grows with volume); 18h30m overlaps it.
func seedFoldVolume(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string, runs int, window string, now time.Time) foldVolume {
	t.Helper()
	fixture := foldVolume{OrgID: orgID, ConfigID: uuid.New(), IntegrationID: uuid.New()}
	pgseed.Integration(ctx, t, pool, fixture.IntegrationID.String(), orgID, "github")
	statements := []struct {
		SQL  string
		Args []any
	}{
		{`INSERT INTO sync_configurations (id,org_id,name,provider,sync_targets,is_active,planner_managed,integration_id,created_at,updated_at)
VALUES ($1,$2,'sync','github','["git","prs","cicd","tests","work-items"]',TRUE,FALSE,$3,now(),now())`, []any{fixture.ConfigID, orgID, fixture.IntegrationID}},
		{`INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at)
SELECT gen_random_uuid(),$1,$2,'github','repository','acme/r'||g,'r'||g,'acme/r'||g,'{}'::json,TRUE,now(),now() FROM generate_series(1,20) g`, []any{orgID, fixture.IntegrationID}},
		{`INSERT INTO integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options)
SELECT gen_random_uuid(),$1,$2,k,TRUE,'{}'::json FROM unnest(ARRAY['repo-metadata','commits','commit-stats','files','prs','pr-reviews','pr-comments','cicd','tests','work-items','work-item-labels','work-item-projects','work-item-history','work-item-comments']) k`, []any{orgID, fixture.IntegrationID}},
		{`INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at)
SELECT gen_random_uuid(),$1,$2,'schedule','incremental','success',0,0,0,
       $3::timestamptz - (g * interval '9 hours') - interval '20 minutes',
       $3::timestamptz - (g * interval '9 hours'),
       $3::timestamptz - (g * interval '9 hours') - interval '21 minutes'
FROM generate_series(1,$4) g`, []any{orgID, fixture.IntegrationID, now, runs}},
		{`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,result,created_at,updated_at,cost_class,mode,attempts)
SELECT gen_random_uuid(), $1, run.id, $2, src.id, 'github', d.key, d.flags::json,
       run.completed_at - $3::interval, run.completed_at - interval '20 minutes',
       CASE WHEN hashtext(run.id::text || src.id::text || d.key) % 100 IN (0,1,2,3,4,5,6) THEN 'failed' ELSE 'success' END,
       CASE WHEN d.key = 'cicd' AND hashtext(src.id::text || run.id::text) % 10 = 0
            THEN '{"rows_written": 120, "reports_complete": false, "incomplete": [{"component": "report_member", "cause": "artifact_expired"}]}'::json
            ELSE '{"rows_written": 120, "reports_complete": true}'::json END,
       run.created_at, run.completed_at, 'rest_core', 'incremental', 1
FROM sync_runs AS run
CROSS JOIN integration_sources AS src
CROSS JOIN (VALUES
  ('repo-metadata','{}'), ('commits','{}'), ('commit-stats','{}'), ('files','{}'),
  ('prs','{"family_dataset_prs": true, "family_dataset_pr_reviews": true, "family_dataset_pr_comments": true}'),
  ('cicd','{"family_dataset_cicd": true, "family_dataset_tests": true}'),
  ('work-items','{"family_dataset_work_items": true, "family_dataset_work_item_labels": true, "family_dataset_work_item_projects": true, "family_dataset_work_item_history": true, "family_dataset_work_item_comments": true}')
) AS d(key, flags)
WHERE run.org_id = $1 AND src.org_id = $1`, []any{orgID, fixture.IntegrationID, window}},
		{`ANALYZE sync_run_units`, nil},
		{`ANALYZE sync_runs`, nil},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.SQL, statement.Args...); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

// foldAllocatedBytesPerRow counts the heap bytes one streamCompactWindows call allocates per streamed row. The
// allocation count (testing.AllocsPerRun) is logged too; it does not grow, the size of each copy of the failed list does.
func foldAllocatedBytesPerRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture foldVolume, now time.Time) (float64, int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	config, err := loadConfig(ctx, tx, fixture.OrgID, fixture.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resolveScope(ctx, tx, config)
	if err != nil {
		t.Fatal(err)
	}
	truncatedBefore := now.AddDate(0, 0, -HistoryLookbackDays)
	rows := 0
	var foldErr error
	started := time.Now()
	allocations := testing.AllocsPerRun(1, func() {
		_, _, rows, foldErr = streamCompactWindows(ctx, tx, fixture.OrgID, scope, truncatedBefore, now)
	})
	if foldErr != nil {
		t.Fatal(foldErr)
	}
	if rows == 0 {
		t.Fatal("the fold streamed no rows")
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if _, _, _, err := streamCompactWindows(ctx, tx, fixture.OrgID, scope, truncatedBefore, now); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("%s: %d rows, %.0f allocations, %d bytes allocated, three folds in %s", fixture.OrgID, rows, allocations, allocated, time.Since(started).Round(time.Millisecond))
	return float64(allocated) / float64(rows), rows
}

// referencePayload encodes the projection the way Rebuild does, with the reference fold in place of the current one.
func referencePayload(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture foldVolume, now time.Time) json.RawMessage {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	config, err := loadConfig(ctx, tx, fixture.OrgID, fixture.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, _, err := referenceBuildProjection(ctx, tx, config, now)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type randomUnit struct {
	ID, RunID, SourceID uuid.UUID
	Dataset, Flags      string
	Since, Before       time.Time
	Status, Result      string
}

// compareRandomSequences seeds sequences of random units (one org each: random windows on an hour grid with
// microsecond seams and one-microsecond windows, every status, family flags, incomplete tests reports) and requires
// streamCompactWindows to return what the reference fold returns for each.
func compareRandomSequences(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, sequences int) {
	t.Helper()
	random := rand.New(rand.NewSource(14167))
	integrationID := uuid.New()
	pgseed.Integration(ctx, t, pool, integrationID.String(), "org-fold-random", "github")
	sources := []source{{ID: uuid.New(), Name: "a", FullName: "acme/a"}, {ID: uuid.New(), Name: "b", FullName: "acme/b"}}
	for _, item := range sources {
		pgseed.IntegrationSource(ctx, t, pool, item.ID.String(), "org-fold-random", integrationID.String(), "github", item.FullName)
	}
	scope := effectiveScope{IntegrationID: &integrationID, Sources: sources, DatasetKeys: []string{"cicd", "commits", "pr-comments", "pr-reviews", "prs", "tests"}}
	base := now.Add(-30 * 24 * time.Hour)
	truncatedBefore := base.Add(6 * time.Hour)
	datasets := []struct{ Key, Flags string }{
		{"commits", "{}"}, {"tests", "{}"}, {"cicd", "{}"},
		{"cicd", `{"family_dataset_cicd": true, "family_dataset_tests": true}`},
		{"cicd", `{"family_dataset_tests": true}`},
		{"prs", `{"family_dataset_prs": true, "family_dataset_pr_reviews": true}`},
		{"files", "{}"},
	}
	statuses := []string{"success", "success", "success", "failed", "failed", "planned", "running", "retrying", "skipped"}
	results := []string{
		`{"reports_complete": true}`,
		`{"reports_complete": false, "incomplete": [{"component": "report_member", "cause": "artifact_expired"}]}`,
		`{"reports_complete": false, "incomplete": [{"component": "report_member", "cause": "malformed"}, {"component": "report_member", "cause": "unsafe_doctype"}]}`,
		`{"reports_complete": false, "incomplete": [{"component": "report_member", "cause": "rate_limited"}]}`,
		`{"reports_complete": false, "incomplete": []}`,
		`{"reports_complete": false}`,
	}
	lengths := []time.Duration{time.Microsecond, 2 * time.Microsecond, time.Hour, time.Hour + time.Microsecond, 2 * time.Hour, 5 * time.Hour, 12 * time.Hour}
	offsets := []time.Duration{0, time.Microsecond, -time.Microsecond, 2 * time.Microsecond}

	orgs := make([]string, sequences)
	var runRows, unitRows [][]any
	for index := range orgs {
		orgID := fmt.Sprintf("org-fold-random-%05d", index)
		orgs[index] = orgID
		runIDs := make([]uuid.UUID, 1+random.Intn(6))
		for runIndex := range runIDs {
			runIDs[runIndex] = uuid.New()
			completed := base.Add(time.Duration(random.Intn(40*24)) * time.Hour)
			runRows = append(runRows, []any{runIDs[runIndex], orgID, integrationID, "test", "incremental", "success", 0, 0, 0, completed.Add(-time.Hour), completed, completed.Add(-2 * time.Hour)})
		}
		for unitIndex := 0; unitIndex < 2+random.Intn(30); unitIndex++ {
			dataset := datasets[random.Intn(len(datasets))]
			since := base.Add(time.Duration(random.Intn(48))*time.Hour + offsets[random.Intn(len(offsets))])
			before := since.Add(lengths[random.Intn(len(lengths))])
			unitRows = append(unitRows, []any{
				uuid.New(), orgID, runIDs[random.Intn(len(runIDs))], integrationID, sources[random.Intn(len(sources))].ID,
				"github", dataset.Key, dataset.Flags, since, before, statuses[random.Intn(len(statuses))],
				results[random.Intn(len(results))], now, now, "rest_core", "incremental", 1,
			})
		}
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"sync_runs"},
		[]string{"id", "org_id", "integration_id", "triggered_by", "mode", "status", "total_units", "completed_units", "failed_units", "started_at", "completed_at", "created_at"},
		pgx.CopyFromRows(runRows)); err != nil {
		t.Fatal(err)
	}
	columns := []string{"id", "org_id", "sync_run_id", "integration_id", "source_id", "provider", "dataset_key", "processor_flags", "since_at", "before_at", "status", "result", "created_at", "updated_at", "cost_class", "mode", "attempts"}
	for index := range unitRows {
		unitRows[index][7] = json.RawMessage(unitRows[index][7].(string))
		unitRows[index][11] = json.RawMessage(unitRows[index][11].(string))
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"sync_run_units"}, columns, pgx.CopyFromRows(unitRows)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE sync_run_units`); err != nil {
		t.Fatal(err)
	}
	t.Logf("random sequences: %d orgs, %d runs, %d units", sequences, len(runRows), len(unitRows))

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	multiFailed, withWindows := 0, 0
	for _, orgID := range orgs {
		got, gotLatest, gotRows, gotErr := streamCompactWindows(ctx, tx, orgID, scope, truncatedBefore, now)
		want, wantLatest, wantRows, wantErr := referenceStreamCompactWindows(ctx, tx, orgID, scope, truncatedBefore, now)
		if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || gotRows != wantRows {
			t.Fatalf("%s: rows/err = %d/%v, reference %d/%v", orgID, gotRows, gotErr, wantRows, wantErr)
		}
		if !equalTimePointers(gotLatest, wantLatest) {
			t.Fatalf("%s: latest successful = %v, reference %v", orgID, gotLatest, wantLatest)
		}
		gotKeys, wantKeys := windowKeys(got), windowKeys(want)
		if strings.Join(gotKeys, "\n") != strings.Join(wantKeys, "\n") {
			t.Fatalf("%s: windows differ from the reference fold\n got: %v\nwant: %v", orgID, gotKeys, wantKeys)
		}
		failed := 0
		for _, window := range want {
			if window.Status == "failed" {
				failed++
			}
		}
		if failed >= 3 {
			multiFailed++
		}
		if len(want) > 0 {
			withWindows++
		}
	}
	t.Logf("random sequences: %d with windows, %d with three or more failed windows", withWindows, multiFailed)
	if multiFailed < sequences/20 || withWindows < sequences/2 {
		t.Fatalf("random sequences reach too few shapes: %d with windows, %d with >= 3 failed windows", withWindows, multiFailed)
	}
}

func equalTimePointers(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func windowKeys(windows []unitWindow) []string {
	keys := make([]string, 0, len(windows))
	for _, window := range windows {
		keys = append(keys, strings.Join([]string{
			window.SourceID, window.DatasetKey, window.Status,
			window.Since.Format(time.RFC3339Nano), window.Before.Format(time.RFC3339Nano),
			window.RunTime.Format(time.RFC3339Nano), strings.Join(window.Reasons, ","),
		}, "|"))
	}
	sort.Strings(keys)
	return keys
}

// referenceBuildProjection and referenceStreamCompactWindows are buildProjection and streamCompactWindows as they were
// before CHAOS-14167 (ops f968833303d4), kept only as the reference the current fold must equal.
func referenceBuildProjection(
	ctx context.Context,
	tx pgx.Tx,
	config syncConfig,
	now time.Time,
) (projectionPayload, *time.Time, *time.Time, error) {
	scope, err := resolveScope(ctx, tx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	truncatedBefore := now.AddDate(0, 0, -HistoryLookbackDays)
	windows, latestSuccessful, _, err := referenceStreamCompactWindows(ctx, tx, config.OrgID, scope, truncatedBefore, now)
	if err != nil {
		return nil, nil, nil, err
	}
	activePairs, err := loadActivePairs(ctx, tx, config.OrgID, scope)
	if err != nil {
		return nil, nil, nil, err
	}
	backfills, err := loadBackfillRanges(ctx, tx, config, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	activeSchedule, hasSchedule, err := loadSchedule(ctx, tx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	sourceUpdatedAt, err := sourceUpdatedAt(ctx, tx, config.OrgID, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	backfillUpdatedAt, err := backfillUpdatedAt(ctx, tx, config, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	isTruncated, err := hasCoverageBefore(ctx, tx, config.OrgID, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	payload, err := buildPayload(payloadInput{
		Config: config, Scope: scope, Windows: windows, Backfills: backfills,
		ActivePairs: activePairs, Schedule: activeSchedule, HasSchedule: hasSchedule,
		Now: now, LatestSuccessful: latestSuccessful, IsTruncated: isTruncated,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return payload, sourceUpdatedAt, backfillUpdatedAt, nil
}

func referenceStreamCompactWindows(
	ctx context.Context,
	tx pgx.Tx,
	orgID string,
	scope effectiveScope,
	truncatedBefore time.Time,
	now time.Time,
) ([]unitWindow, *time.Time, int, error) {
	if scope.IntegrationID == nil || len(scope.Sources) == 0 || len(scope.DatasetKeys) == 0 {
		return nil, nil, 0, nil
	}
	sourceIDs := make([]uuid.UUID, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		sourceIDs = append(sourceIDs, item.ID)
	}
	rows, err := tx.Query(ctx, `
SELECT unit.source_id, unit.dataset_key, unit.processor_flags,
       unit.since_at, unit.before_at, unit.status,
       COALESCE(run.completed_at, run.started_at, run.created_at), unit.id,
       COALESCE(unit.result->>'reports_complete' = 'false', false),
       (unit.result->'incomplete')::text
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.org_id = $1
  AND unit.integration_id = $2
  AND unit.source_id = ANY($3::uuid[])
  AND unit.dataset_key = ANY($4::text[])
  AND unit.status = ANY($5::text[])
  AND unit.since_at IS NOT NULL
  AND unit.before_at IS NOT NULL
  AND unit.before_at >= $6
  AND run.org_id = $1
ORDER BY COALESCE(run.completed_at, run.started_at, run.created_at), unit.id`,
		orgID, *scope.IntegrationID, sourceIDs, queryDatasetKeys(scope.DatasetKeys), requestedStatuses, truncatedBefore)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("query sync coverage unit windows: %w", err)
	}
	defer rows.Close()
	states := make(map[string]*pairState)
	var latestSuccessful *time.Time
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > maxProjectionRows {
			return nil, nil, rowCount, fmt.Errorf("sync coverage projection row limit exceeded: %d", maxProjectionRows)
		}
		var sourceID uuid.UUID
		var dataset, status string
		var flags json.RawMessage
		var since, before, runTime time.Time
		var unitID uuid.UUID
		var reportsIncomplete bool
		var incomplete *string
		if err := rows.Scan(&sourceID, &dataset, &flags, &since, &before, &status, &runTime, &unitID, &reportsIncomplete, &incomplete); err != nil {
			return nil, nil, rowCount, fmt.Errorf("scan sync coverage unit window: %w", err)
		}
		since = maxTime(since.UTC(), truncatedBefore)
		before = before.UTC()
		if !since.Before(before) {
			continue
		}
		effectiveKeys, err := effectiveDatasetKeys(dataset, flags)
		if err != nil {
			return nil, nil, rowCount, fmt.Errorf("expand sync coverage unit window: %w", err)
		}
		var closedReasons []string
		if status == "success" && reportsIncomplete {
			closedReasons = closedWithReason(incomplete)
		}
		for _, effectiveKey := range effectiveKeys {
			if !containsString(scope.DatasetKeys, effectiveKey) {
				continue
			}
			recordFoldedKeyResolution(dataset, effectiveKey)
			key := sourceID.String() + "\x00" + effectiveKey
			state := states[key]
			if state == nil {
				state = &pairState{}
				states[key] = state
			}
			interval := coverageInterval{Since: since, Before: before}
			state.Requested = append(state.Requested, interval)
			// A tests unit that finished with dropped or skipped reports
			// advanced its watermark but did not cover the window: it stays a
			// gap and is not the last complete sync. It did run, so it
			// supersedes an earlier failure of the window: the window reads as
			// a gap, not as failed.
			//
			// CHAOS-13988 "close with reason": when every lost report is
			// closed with a reason (closedWithReasonCauses) the window is
			// covered like a complete unit's, and is also recorded with its
			// reasons. A window once closed stays covered: a later gap-class
			// unit over it does not reopen it, and only a complete unit
			// (whatever its order) removes the reason.
			if status == "success" && reportsIncomplete && effectiveKey == "tests" {
				if len(closedReasons) == 0 {
					state.Failed = subtractIntervals(state.Failed, []coverageInterval{interval})
					continue
				}
				state.Closed = append(state.Closed, closedRange{Interval: interval, Reasons: closedReasons})
			} else if status == "success" {
				state.Complete = append(state.Complete, interval)
			}
			switch status {
			case "success":
				state.Covered = append(state.Covered, interval)
				state.Failed = subtractIntervals(state.Failed, []coverageInterval{interval})
				value := runTime.UTC()
				if latestSuccessful == nil || value.After(*latestSuccessful) {
					latestSuccessful = &value
				}
			case "failed":
				state.Failed = mergeIntervals(append(state.Failed, interval))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, rowCount, fmt.Errorf("iterate sync coverage unit windows: %w", err)
	}
	windows := make([]unitWindow, 0)
	for key, state := range states {
		parts := strings.SplitN(key, "\x00", 2)
		for _, item := range mergeIntervals(state.Requested) {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "planned", RunTime: time.Time{}})
		}
		for _, item := range mergeIntervals(state.Covered) {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "success", RunTime: time.Time{}})
		}
		for _, item := range state.Failed {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "failed", RunTime: now.Add(time.Microsecond)})
		}
		for _, item := range subtractClosedRanges(state.Closed, state.Complete) {
			windows = append(windows, unitWindow{Since: item.Interval.Since, Before: item.Interval.Before, SourceID: parts[0], DatasetKey: parts[1], Status: statusClosedWithReason, Reasons: item.Reasons})
		}
		if len(windows) > maxCompactWindows {
			return nil, nil, rowCount, fmt.Errorf("sync coverage compact window limit exceeded: %d", maxCompactWindows)
		}
	}
	return windows, latestSuccessful, rowCount, nil
}
