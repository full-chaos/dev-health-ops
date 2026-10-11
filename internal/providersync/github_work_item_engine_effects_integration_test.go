//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// This suite authors no DDL. githubDerivedIntegrationConn applies the complete
// production ClickHouse migration chain to a throwaway database before opening
// the Go connection. These tables deliberately remain plain MergeTree tables:
// every adapter's readback dedupes to the newest computed_at generation for
// the logical identity and rejects only a divergent tie at that newest
// timestamp; a superseded older generation never blocks a readback that
// otherwise matches the newest one.
func TestGitHubWorkItemEngineEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	lease := githubDerivedIntegrationLease()

	for _, table := range []string{
		"issue_type_metrics_daily",
	} {
		t.Run("plain_merge_tree_"+table, func(t *testing.T) {
			var ddl string
			if err := conn.QueryRow(ctx, "SHOW CREATE TABLE "+table).Scan(&ddl); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(ddl, "ENGINE = MergeTree") ||
				strings.Contains(ddl, "ReplacingMergeTree") {
				t.Fatalf("%s must remain plain MergeTree; DDL:\n%s", table, ddl)
			}
		})
	}

	t.Run(githubIssueTypeMetricsDestination, func(t *testing.T) {
		sink := GitHubIssueTypeMetricsClickHouseEffects{Conn: conn, Lease: lease}
		repoA := uuid.MustParse("11111111-1111-4111-8111-111111111111")
		repoB := uuid.MustParse("22222222-2222-4222-8222-222222222222")
		base := githubWorkItemEngineEffectIssueRow()
		base.ComputedAt = base.ComputedAt.Add(123456789 * time.Nanosecond)
		rows := []githubIssueTypeMetricsDailyRow{base, base, base}
		rows[0].RepoID, rows[1].RepoID, rows[2].RepoID = &repoA, &repoB, nil
		identity, effect := githubWorkItemEngineIntegrationBatch(
			t, githubDerivedIntegrationOrg, githubIssueTypeMetricsDestination, rows,
		)

		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectAbsent, "before write",
		)
		foreignRows := append([]githubIssueTypeMetricsDailyRow(nil), rows...)
		for index := range foreignRows {
			foreignRows[index].OrgID = "org-other"
		}
		foreignIdentity, foreignEffect := githubWorkItemEngineIntegrationBatch(
			t, "org-other", githubIssueTypeMetricsDestination, foreignRows,
		)
		if err := sink.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, foreignIdentity, foreignEffect, EffectExact, "foreign write",
		)
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectAbsent, "foreign tenant fence",
		)

		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectExact,
			"same physical key across two repos and NULL repo",
		)
		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectExact, "identical retry",
		)
		assertGitHubWorkItemEnginePlainRetry(
			t, ctx, conn,
			`SELECT count(), min(computed_at), max(computed_at)
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND provider = ? AND team_id = ?
	AND issue_type_norm = ? AND repo_id = ? AND computed_at = ?`,
			[]any{
				base.OrgID, mustGitHubWorkItemDerivedDay(t, base.Day), base.Provider,
				base.TeamID, base.IssueTypeNorm, repoA,
			},
			base.ComputedAt,
		)

		older := rows[0]
		older.ComputedAt = older.ComputedAt.Add(-time.Hour)
		older.CreatedCount++
		olderIdentity, olderEffect := githubWorkItemEngineIntegrationBatch(
			t, githubDerivedIntegrationOrg, githubIssueTypeMetricsDestination,
			[]githubIssueTypeMetricsDailyRow{older},
		)
		if err := sink.WriteGitHubWorkItemEffect(ctx, olderIdentity, olderEffect); err != nil {
			t.Fatal(err)
		}
		// Two versions of the same key are now persisted for repoA: the
		// newest generation (base, written above) and this strictly older,
		// divergent one. Readback dedupes to the newest generation, so the
		// stale divergent row does not block recognizing the current state.
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectExact,
			"older divergent generation is superseded by the newest one",
		)

		tied := rows[0]
		tied.CreatedCount++
		tiedIdentity, tiedEffect := githubWorkItemEngineIntegrationBatch(
			t, githubDerivedIntegrationOrg, githubIssueTypeMetricsDestination,
			[]githubIssueTypeMetricsDailyRow{tied},
		)
		if err := sink.WriteGitHubWorkItemEffect(ctx, tiedIdentity, tiedEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemEngineInspection(
			t, ctx, sink, identity, effect, EffectConflict,
			"equal-time divergent generation",
		)
	})

}

func githubWorkItemEngineIntegrationBatch[T any](
	t *testing.T,
	orgID string,
	destination string,
	rows []T,
) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	effect := githubDerivedIntegrationEffect(t, destination, rows)
	identity := githubDerivedIntegrationIdentity(destination, len(rows))
	identity.OrgID = orgID
	return identity, effect
}

func mustGitHubWorkItemDerivedDay(
	t *testing.T, day githubWorkItemDerivedDay,
) time.Time {
	t.Helper()
	parsed, err := day.time()
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func assertGitHubWorkItemEnginePlainRetry(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	query string,
	arguments []any,
	computedAt time.Time,
) {
	t.Helper()
	storedAt := githubWorkItemDerivedSeconds(computedAt)
	arguments = append(arguments, storedAt)
	var count uint64
	var minimum, maximum time.Time
	if err := conn.QueryRow(ctx, query, arguments...).Scan(
		&count, &minimum, &maximum,
	); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("plain MergeTree retry rows=%d want=2 physical duplicates", count)
	}
	if !minimum.Equal(storedAt) || !maximum.Equal(storedAt) ||
		minimum.Nanosecond() != 0 || maximum.Nanosecond() != 0 {
		t.Fatalf(
			"raw DateTime readback min=%s max=%s want seconds=%s",
			minimum, maximum, storedAt,
		)
	}
}

func assertGitHubWorkItemEngineInspection(
	t *testing.T,
	ctx context.Context,
	adapter GitHubWorkItemEffectAdapter,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
	want EffectInspection,
	label string,
) {
	t.Helper()
	got, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
	if err != nil {
		t.Fatalf("%s: inspect: %v", label, err)
	}
	if got != want {
		t.Fatalf("%s: inspection=%s want=%s", label, got, want)
	}
}
