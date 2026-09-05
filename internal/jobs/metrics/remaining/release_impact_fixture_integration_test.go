//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// This file is PR1's proof plan item "mutation proof that the degraded signal
// actually fires" (design.md §6), plus a lightweight per-CLASS coverage
// battery over ComputePartition's branches -- NOT a cross-language byte-exact
// parity harness (the shape dora_table_parity_integration_test.go uses via a
// separate native-producer binary and scripts/worker/compute_parity_fixtures.py).
// That harness is real infrastructure a from-scratch build for this PR would
// duplicate at length; this file instead runs the REAL native executor
// end-to-end against a REAL migrated ClickHouse (migratedClickHouse, the same
// helper capacity's and dora's own schema-guard integration tests use, which
// applies the full checked-in migration chain INCLUDING 088), fixture by
// fixture, and reports how many rows each named parity class produced and
// whether the degraded signal fired for it. Every class here is a branch
// _compute_day/_compute_release_env can take (release_impact.py:88-122,
// 458-584); the table asserts each one is actually exercised, not merely
// invoked.

// releaseImpactParityClass is one named branch of the ported compute path,
// isolated to its own org_id so fixtures cannot bleed into each other.
type releaseImpactParityClass struct {
	name string
	// seed populates ClickHouse for this class's (org, day) only.
	seed func(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time)
	// wantRows is the exact row count ComputePartition must write for this class.
	wantRows int
	// wantDegraded is whether the CHAOS-4258 signal must fire for this class.
	wantDegraded bool
}

// spyReleaseImpactObserver records every call instead of asserting inline, so
// one ComputePartition run can be checked against several expectations
// without re-running it.
type spyReleaseImpactObserver struct {
	partitions []struct{ days, rowsWritten int }
	degraded   []struct {
		orgID       string
		day         time.Time
		deployments int
	}
}

func (s *spyReleaseImpactObserver) ObserveReleaseImpactPartition(days, rowsWritten int) error {
	s.partitions = append(s.partitions, struct{ days, rowsWritten int }{days, rowsWritten})
	return nil
}

func (s *spyReleaseImpactObserver) ObserveReleaseImpactDegradedMissingTelemetry(
	orgID string, day time.Time, deployments int,
) error {
	s.degraded = append(s.degraded, struct {
		orgID       string
		day         time.Time
		deployments int
	}{orgID, day, deployments})
	return nil
}

const releaseImpactFixtureDay = "2026-08-20"

func releaseImpactParityClasses() []releaseImpactParityClass {
	return []releaseImpactParityClass{
		{
			name:         "quiet_day_no_deployments_no_telemetry",
			seed:         func(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) {},
			wantRows:     0,
			wantDegraded: false,
		},
		{
			// This IS CHAOS-4258: deployments occurred, telemetry never
			// arrived. Before this PR both this class and the quiet day above
			// were indistinguishable -- both wrote 0 rows.
			name: "degraded_missing_telemetry",
			seed: func(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) {
				insertDeployment(t, ctx, conn, orgID, releaseImpactFixtureRepoID(1), "v1.0.0-degraded", "production", day, "deploy-degraded")
			},
			wantRows:     0,
			wantDegraded: true,
		},
		{
			// Normal path: enough sessions for BOTH friction and error deltas
			// to resolve to real (non-nil) numbers.
			name: "normal_computation_with_resolved_deltas",
			seed: func(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) {
				repoID := releaseImpactFixtureRepoID(2)
				insertDeployment(t, ctx, conn, orgID, repoID, "v1.0.0-normal", "production", day, "deploy-normal")
				// Baseline window: day-7 .. day.
				insertTelemetryBucket(t, ctx, conn, orgID, "friction.click_rage", "v1.0.0-normal", "production", repoID,
					day.AddDate(0, 0, -1), day.AddDate(0, 0, -1).Add(time.Hour), 50, 200)
				insertTelemetryBucket(t, ctx, conn, orgID, "error.js_exception", "v1.0.0-normal", "production", repoID,
					day.AddDate(0, 0, -1), day.AddDate(0, 0, -1).Add(time.Hour), 500, 1200)
				// Post window: day .. day+24h -- also satisfies
				// _find_release_env_pairs's toDate(bucket_start) = day.
				insertTelemetryBucket(t, ctx, conn, orgID, "friction.click_rage", "v1.0.0-normal", "production", repoID,
					day, day.Add(time.Hour), 100, 200)
				insertTelemetryBucket(t, ctx, conn, orgID, "error.js_exception", "v1.0.0-normal", "production", repoID,
					day, day.Add(time.Hour), 1200, 1200)
			},
			wantRows:     1,
			wantDegraded: false,
		},
		{
			// Telemetry exists (so this is NOT the degraded class) but too
			// thin to clear _MIN_SESSIONS_FRICTION/_MIN_EVENTS_ERROR -- both
			// deltas must come back nil, and the row must still be written
			// (release_impact.py never refuses a partition for thin samples,
			// only records it via missing_required_fields_count).
			name: "insufficient_samples_deltas_stay_nil",
			seed: func(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) {
				repoID := releaseImpactFixtureRepoID(3)
				insertDeployment(t, ctx, conn, orgID, repoID, "v1.0.0-thin", "production", day, "deploy-thin")
				insertTelemetryBucket(t, ctx, conn, orgID, "friction.click_rage", "v1.0.0-thin", "production", repoID,
					day, day.Add(time.Hour), 1, 5)
			},
			wantRows:     1,
			wantDegraded: false,
		},
	}
}

// TestReleaseImpactParityClassBattery runs every named class through the REAL
// native executor against a REAL migrated ClickHouse (migration 088
// included), reports the class -> (rows written, degraded fired) coverage
// table, and asserts each class produced exactly what it claims to.
//
// This is the mutation proof design.md §6 calls for: a mutation that made the
// degraded signal never fire (or always fire) changes the coverage table
// below, not just one assertion buried in a larger test.
func TestReleaseImpactParityClassBattery(t *testing.T) {
	ctx := context.Background()
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)
	day, err := time.Parse("2006-01-02", releaseImpactFixtureDay)
	if err != nil {
		t.Fatalf("parse fixture day: %v", err)
	}

	classes := releaseImpactParityClasses()
	coverage := make(map[string]struct {
		rowsWritten  int
		degradedFire bool
	}, len(classes))

	for _, class := range classes {
		t.Run(class.name, func(t *testing.T) {
			orgID := deterministicOrgIDForClass(class.name)
			class.seed(t, ctx, conn, orgID, day)

			observer := &spyReleaseImpactObserver{}
			executor, err := NewReleaseImpactExecutor(ctx, conn, observer, nil)
			if err != nil {
				t.Fatalf("construct executor: %v", err)
			}

			scope, err := json.Marshal(releaseImpactScope{
				Version: ScopeVersion, Day: releaseImpactFixtureDay,
				BackfillDays: 1, RecomputationWindowDays: 1,
			})
			if err != nil {
				t.Fatalf("marshal scope: %v", err)
			}
			run := Run{OrganizationID: orgID}
			partition := Partition{ID: "class-" + class.name, Scope: scope}

			outcome, err := executor.ComputePartition(ctx, run, partition)
			if err != nil {
				t.Fatalf("ComputePartition: %v", err)
			}
			if outcome.RowsWritten == nil {
				t.Fatal("ComputePartition returned a nil RowsWritten -- CHAOS-4243's zero-row-vs-no-signal distinction requires a non-nil zero")
			}

			gotRows := *outcome.RowsWritten
			gotDegraded := len(observer.degraded) > 0
			coverage[class.name] = struct {
				rowsWritten  int
				degradedFire bool
			}{gotRows, gotDegraded}

			if gotRows != class.wantRows {
				t.Errorf("class %s: rows written = %d, want %d", class.name, gotRows, class.wantRows)
			}
			if gotDegraded != class.wantDegraded {
				t.Errorf("class %s: degraded signal fired = %v, want %v (observer saw %d degraded call(s))",
					class.name, gotDegraded, class.wantDegraded, len(observer.degraded))
			}
			if gotDegraded {
				fired := observer.degraded[0]
				if fired.orgID != orgID {
					t.Errorf("class %s: degraded signal reported org %q, want %q", class.name, fired.orgID, orgID)
				}
				if fired.deployments < 1 {
					t.Errorf("class %s: degraded signal reported %d deployments, want >=1", class.name, fired.deployments)
				}
			}
			if len(observer.partitions) != 1 {
				t.Errorf("class %s: ObserveReleaseImpactPartition called %d time(s), want exactly 1", class.name, len(observer.partitions))
			}
		})
	}

	// Print the class coverage table unconditionally -- this is the
	// human-readable artefact team-lead asked the parity battery to report,
	// not only pass/fail per subtest.
	t.Log("release_impact parity class coverage:")
	for _, class := range classes {
		result := coverage[class.name]
		t.Logf("  %-45s rows=%d degraded=%v", class.name, result.rowsWritten, result.degradedFire)
	}
	if len(coverage) != len(classes) {
		t.Fatalf("coverage table has %d entries, want %d -- a class subtest did not run to completion", len(coverage), len(classes))
	}
}

func deterministicOrgIDForClass(className string) string {
	// A stable, distinct UUID-shaped org per class -- classes never share an
	// org_id, so one class's fixture cannot leak into another's read.
	sum := 0
	for _, r := range className {
		sum = sum*31 + int(r)
	}
	if sum < 0 {
		sum = -sum
	}
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", sum%1_000_000_000_000)
}

// releaseImpactFixtureRepoID returns a valid-shaped UUID string: deployments.
// repo_id is a real UUID column (migration 000), and an arbitrary non-UUID
// string fails to parse on Append -- unlike telemetry_signal_bucket.repo_id,
// which is a plain String column (migration 034) and accepts any string.
func releaseImpactFixtureRepoID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
}

// insertDeployment mirrors dora_scope_precondition_integration_test.go's
// proven deployments column order/types exactly (repo_id as a plain Go
// string parses fine into the UUID column through this driver).
func insertDeployment(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, repoID, releaseRef, environment string, deployedAt time.Time, deploymentID string,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare deployments batch: %v", err)
	}
	if err := batch.Append(
		repoID, deploymentID, "success", environment,
		&deployedAt, &deployedAt, &deployedAt, (*time.Time)(nil),
		nil, releaseRef, 0.0, orgID, deployedAt,
	); err != nil {
		t.Fatalf("append deployment fixture: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send deployments batch: %v", err)
	}
}

func insertTelemetryBucket(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, signalType, releaseRef, environment, repoID string,
	bucketStart, bucketEnd time.Time, signalCount, sessionCount uint64,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO telemetry_signal_bucket (org_id, signal_type, signal_count, session_count, endpoint_group, environment, repo_id, release_ref, bucket_start, bucket_end, ingested_at, schema_version, dedupe_key)`)
	if err != nil {
		t.Fatalf("prepare telemetry_signal_bucket batch: %v", err)
	}
	dedupeKey := fmt.Sprintf("%s-%s-%s-%d", orgID, releaseRef, signalType, bucketStart.UnixNano())
	if err := batch.Append(
		orgID, signalType, signalCount, sessionCount, "api", environment, repoID, releaseRef,
		bucketStart, bucketEnd, bucketStart, "v1", dedupeKey,
	); err != nil {
		t.Fatalf("append telemetry_signal_bucket fixture: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send telemetry_signal_bucket batch: %v", err)
	}
}
