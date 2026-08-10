//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This test authors no DDL. The feature-flag and work-graph tables come from
// the real ClickHouse migration chain applied by chschema, so nullable UUID,
// String, Date, and DateTime64 behavior is exercised against the deployed
// schema rather than a second hand-written copy.
func TestGitLabFeatureFlagsEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("gitlab", "feature-flags")
	// Deliberately include sub-millisecond digits: DateTime64(3) quantizes
	// these on insert, and readback must compare the stored representation.
	now := time.Date(2026, 8, 10, 12, 0, 0, 123456789, time.UTC)

	t.Run("feature flag exact readback and version conflict", func(t *testing.T) {
		created := now.Add(-24 * time.Hour)
		archived := now.Add(-time.Hour)
		row := launchDarklyFlagRow{
			OrgID: claim.OrgID, Provider: "gitlab", FlagKey: "checkout-integration",
			ProjectKey: "group/project", RepoID: "", Environment: "production",
			FlagType: "new_version_flag", CreatedAt: &created, ArchivedAt: &archived,
			LastSynced: now,
		}
		effect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{row})
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectAbsent)
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)

		newer := row
		newer.FlagType = "new_version_flag_v2"
		newer.LastSynced = now.Add(time.Minute)
		newerEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{newer})
		if err := sink.WriteEffect(ctx, claim, newerEffect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectConflict)
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, newerEffect, EffectExact)
	})

	t.Run("same flag identity across environments remains exact after FINAL", func(t *testing.T) {
		created := now.Add(-24 * time.Hour)
		production := launchDarklyFlagRow{
			OrgID: claim.OrgID, Provider: "gitlab", FlagKey: "checkout-environments",
			ProjectKey: "group/project", RepoID: "", Environment: "production",
			FlagType: "new_version_flag", CreatedAt: &created, LastSynced: now,
		}
		stagingCreated := now.Add(-23 * time.Hour)
		staging := production
		staging.Environment = "staging"
		staging.CreatedAt = &stagingCreated
		staging.LastSynced = now.Add(time.Second)
		effect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{production, staging})
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		var physical, final uint64
		if err := conn.QueryRow(ctx, `
SELECT count()
		FROM feature_flag
		WHERE org_id = ? AND provider = ? AND project_key = ? AND flag_key = ?`,
			claim.OrgID, "gitlab", "group/project", "checkout-environments",
		).Scan(&physical); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `
	SELECT count()
		FROM feature_flag FINAL
		WHERE org_id = ? AND provider = ? AND project_key = ? AND flag_key = ?`,
			claim.OrgID, "gitlab", "group/project", "checkout-environments",
		).Scan(&final); err != nil {
			t.Fatal(err)
		}
		t.Logf("feature_flag same-identity rows physical=%d final=%d", physical, final)
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)
	})

	t.Run("event exact readback and divergent conflict", func(t *testing.T) {
		event := launchDarklyEventRow{
			OrgID: claim.OrgID, EventType: "toggle", FlagKey: "checkout-event",
			Environment: "production", RepoID: "", ActorType: "snapshot",
			PrevState: "off", NextState: "on", EventAt: now,
			IngestedAt: now, SourceEventID: "gitlab-event-1", DedupeKey: "gitlab-event-1",
		}
		effect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag_event", EffectReadbackRequired, []launchDarklyEventRow{event})
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectAbsent)
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)

		divergent := event
		divergent.NextState = "off"
		divergentEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag_event", EffectReadbackRequired, []launchDarklyEventRow{divergent})
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, divergentEffect, EffectConflict)
	})

	t.Run("replayed exact event remains exact after MergeTree duplicate", func(t *testing.T) {
		event := launchDarklyEventRow{
			OrgID: claim.OrgID, EventType: "toggle", FlagKey: "checkout-replay",
			Environment: "production", RepoID: "", ActorType: "snapshot",
			PrevState: "off", NextState: "on", EventAt: now,
			IngestedAt: now, SourceEventID: "gitlab-event-replay", DedupeKey: "gitlab-event-replay",
		}
		effect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag_event", EffectReadbackRequired, []launchDarklyEventRow{event})
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		var rows uint64
		if err := conn.QueryRow(ctx, `
SELECT count()
FROM feature_flag_event
WHERE org_id = ? AND dedupe_key = ?`, claim.OrgID, "gitlab-event-replay").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("feature_flag_event replay rows=%d", rows)
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)
	})

	t.Run("work graph edge nullable repo exact readback and version conflict", func(t *testing.T) {
		edge := launchDarklyEdgeRow{
			EdgeID: "gitlab-edge-integration", SourceType: "feature_flag", SourceID: "flag",
			TargetType: "file", TargetID: "group/project:checkout.go", EdgeType: "guards",
			RepoID: "", Provider: "gitlab", Provenance: "native", Confidence: 0.875,
			Evidence: "flag", DiscoveredAt: now.Add(-time.Minute), LastSynced: now,
			EventAt: now.Add(-time.Minute), Day: "2026-08-10", OrgID: claim.OrgID,
		}
		effect := gitlabFeatureFlagsIntegrationEffect(t, "work_graph_edges", EffectReplaySafe, []launchDarklyEdgeRow{edge})
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectAbsent)
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectExact)

		newer := edge
		newer.Evidence = "flag-v2"
		newer.LastSynced = now.Add(2 * time.Minute)
		newerEffect := gitlabFeatureFlagsIntegrationEffect(t, "work_graph_edges", EffectReplaySafe, []launchDarklyEdgeRow{newer})
		if err := sink.WriteEffect(ctx, claim, newerEffect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, effect, EffectConflict)
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, newerEffect, EffectExact)
	})

	t.Run("tenant fence remains independent", func(t *testing.T) {
		foreignClaim := claim
		foreignClaim.OrgID = "org-gitlab-foreign"
		foreign := launchDarklyFlagRow{
			OrgID: foreignClaim.OrgID, Provider: "gitlab", FlagKey: "tenant-integration",
			ProjectKey: "group/project", Environment: "production", FlagType: "flag",
			CreatedAt: &now, LastSynced: now,
		}
		foreignEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{foreign})
		if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
			t.Fatal(err)
		}
		local := foreign
		local.OrgID = claim.OrgID
		localEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{local})
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, localEffect, EffectAbsent)
		if err := sink.WriteEffect(ctx, claim, localEffect); err != nil {
			t.Fatal(err)
		}
		assertGitLabFeatureFlagsInspection(t, ctx, sink, claim, localEffect, EffectExact)
	})

	t.Run("forged row and forbidden link never reach ClickHouse", func(t *testing.T) {
		forged := launchDarklyFlagRow{
			OrgID: "org-forged", Provider: "gitlab", FlagKey: "forged",
			ProjectKey: "group/project", Environment: "production", FlagType: "flag", CreatedAt: &now, LastSynced: now,
		}
		forgedEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag", EffectReplaySafe, []launchDarklyFlagRow{forged})
		if err := sink.WriteEffect(ctx, claim, forgedEffect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
			t.Fatalf("forged write error=%v", err)
		}
		linkEffect := gitlabFeatureFlagsIntegrationEffect(t, "feature_flag_link", EffectReplaySafe, []launchDarklyLinkRow{{OrgID: claim.OrgID, Provider: "gitlab"}})
		if err := sink.WriteEffect(ctx, claim, linkEffect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("forbidden link write error=%v", err)
		}
	})
}

func assertGitLabFeatureFlagsInspection(
	t *testing.T,
	ctx context.Context,
	sink GitLabFeatureFlagsClickHouseEffects,
	claim Claim,
	effect EffectBatch,
	want EffectInspection,
) {
	t.Helper()
	got, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || got != want {
		t.Fatalf("inspection=%s error=%v want=%s", got, err, want)
	}
}

func gitlabFeatureFlagsIntegrationEffect[T any](
	t *testing.T,
	destination string,
	recovery EffectRecoveryPolicy,
	rows []T,
) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(destination, recovery, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
