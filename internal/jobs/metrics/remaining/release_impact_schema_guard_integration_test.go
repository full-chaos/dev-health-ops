//go:build integration

package remaining

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestReleaseImpactRefusesAnIncompatibleSchema is the regression test for
// codex r3's finding (CHAOS-4296/#2262): verifyReleaseImpactSchema originally
// checked only that the engine's family name contained "ReplacingMergeTree",
// not the version column or the sorting key. A table converted (by hand, or
// by a partial/interrupted run) to ReplacingMergeTree with the WRONG version
// column, or a sorting key that drops environment/day, would pass that
// engine-only check while silently collapsing DISTINCT
// (org_id, release_ref, environment, day) rows into each other -- wrong
// numbers, reported as a successful run, exactly the failure class
// dora_native_clickhouse.go's classifySortingKey exists to catch for its own
// table.
func TestReleaseImpactRefusesAnIncompatibleSchema(t *testing.T) {
	ctx := context.Background()
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)

	// Baseline: the real schema must BUILD, or the assertions below would
	// pass for a guard that refuses everything.
	if _, err := NewReleaseImpactExecutor(ctx, conn, nil, nil); err != nil {
		t.Fatalf("the real migrated schema must be accepted: %v", err)
	}

	const columns = `
        org_id String DEFAULT 'default',
        day Date,
        release_ref String,
        environment String,
        repo_id String,
        release_user_friction_delta Nullable(Float64),
        release_post_friction_rate Nullable(Float64),
        release_error_rate_delta Nullable(Float64),
        release_post_error_rate Nullable(Float64),
        time_to_first_user_issue_after_release Nullable(Float64),
        release_impact_confidence_score Float32,
        release_impact_coverage_ratio Float32,
        flag_exposure_rate Nullable(Float64),
        flag_activation_rate Nullable(Float64),
        flag_reliability_guardrail Nullable(Float64),
        flag_friction_delta Nullable(Float64),
        flag_rollout_half_life Nullable(Float64),
        flag_churn_rate Nullable(Float64),
        issue_to_release_impact_link_rate Nullable(Float64),
        rollback_or_disable_after_impact_spike UInt32,
        coverage_ratio Float32,
        missing_required_fields_count UInt32,
        instrumentation_change_flag UInt8 DEFAULT 0,
        data_completeness Float32,
        concurrent_deploy_count UInt32,
        computed_at DateTime64(3, 'UTC')
    `

	t.Run("the wrong version column is refused", func(t *testing.T) {
		fresh := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)
		if err := fresh.Exec(ctx, "DROP TABLE release_impact_daily"); err != nil {
			t.Fatalf("stage the wrong-version-column schema: %v", err)
		}
		// Sorting key matches exactly; ONLY the ReplacingMergeTree version
		// column differs (day instead of computed_at) -- so a refusal here
		// is unambiguously about the version column, not the sorting key.
		if err := fresh.Exec(ctx, `
            CREATE TABLE release_impact_daily (`+columns+`
            ) ENGINE = ReplacingMergeTree(day)
            ORDER BY (org_id, release_ref, environment, day)
        `); err != nil {
			t.Fatalf("create the wrong-version-column table: %v", err)
		}
		_, err := NewReleaseImpactExecutor(ctx, fresh, nil, nil)
		if err == nil {
			t.Fatal(
				"a release_impact_daily with the wrong RMT version column was " +
					"accepted; recomputed rows would collapse by the WRONG field")
		}
		if !errors.Is(err, ErrReleaseImpactSchemaIncompatible) {
			t.Fatalf("expected a schema-incompatible refusal, got: %v", err)
		}
		if !strings.Contains(err.Error(), "computed_at") {
			t.Errorf("the refusal must name the expected version column: %v", err)
		}
	})

	t.Run("a shorter sorting key is refused", func(t *testing.T) {
		fresh := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)
		if err := fresh.Exec(ctx, "DROP TABLE release_impact_daily"); err != nil {
			t.Fatalf("stage the wrong-sorting-key schema: %v", err)
		}
		// Version column matches exactly; ONLY the sorting key differs
		// (drops environment and day) -- so a refusal here is unambiguously
		// about the sorting key, not the version column.
		if err := fresh.Exec(ctx, `
            CREATE TABLE release_impact_daily (`+columns+`
            ) ENGINE = ReplacingMergeTree(computed_at)
            ORDER BY (org_id, release_ref)
        `); err != nil {
			t.Fatalf("create the wrong-sorting-key table: %v", err)
		}
		_, err := NewReleaseImpactExecutor(ctx, fresh, nil, nil)
		if err == nil {
			t.Fatal(
				"a release_impact_daily with a shortened sorting key was " +
					"accepted; distinct (environment, day) rows would collapse " +
					"into each other")
		}
		if !errors.Is(err, ErrReleaseImpactSchemaIncompatible) {
			t.Fatalf("expected a schema-incompatible refusal, got: %v", err)
		}
		if !strings.Contains(err.Error(), releaseImpactExpectedSortingKey) {
			t.Errorf("the refusal must name the expected sorting key: %v", err)
		}
	})
}
