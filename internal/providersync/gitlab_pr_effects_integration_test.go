//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This suite deliberately authors no ClickHouse DDL. chschema applies the
// production migration chain, including the org_id shadow-table rebuild, so
// the GitLab sink proves against the same ReplacingMergeTree/readback schema
// used outside tests.
func TestGitLabPullRequestEffectsUseMigratedClickHouseReadbackAndTenantLeaseFences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, table := range []string{"git_pull_requests", "git_pull_request_reviews"} {
		var ddl string
		if err := conn.QueryRow(ctx, "SHOW CREATE TABLE "+table).Scan(&ddl); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ddl, "org_id") {
			t.Fatalf("migrated %s schema has no tenant key: %s", table, ddl)
		}
	}

	claim := nativeTestClaim("gitlab", "pr-reviews")
	now := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	row := pullRequestReadbackFixture(now)
	pullRaw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	pullEffect, err := BuildEffectBatch("git_pull_requests", EffectReadbackRequired, []json.RawMessage{pullRaw})
	if err != nil {
		t.Fatal(err)
	}
	review := pullRequestReviewRow{
		OrgID: claim.OrgID, RepoID: row.RepoID, Number: row.Number,
		ReviewID: "gitlab-note-1", Reviewer: "reviewer", State: "APPROVED",
		SubmittedAt: now.Add(-time.Minute), LastSynced: now,
	}
	reviewRaw, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	reviewEffect, err := BuildEffectBatch("git_pull_request_reviews", EffectReadbackRequired, []json.RawMessage{reviewRaw})
	if err != nil {
		t.Fatal(err)
	}
	sink := GitLabPullRequestSocialClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(ctx, claim, pullEffect); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, reviewEffect); err != nil {
		t.Fatal(err)
	}
	// A retry of the same generation is idempotent at the sink boundary; the
	// FINAL readback below must still select the exact row, not an accidental
	// cross-version reconstruction.
	if err := sink.WriteEffect(ctx, claim, pullEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, pullEffect); err != nil || inspection != EffectExact {
		t.Fatalf("pull readback=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, reviewEffect); err != nil || inspection != EffectExact {
		t.Fatalf("review readback=%s error=%v", inspection, err)
	}
	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	foreignRow := row
	foreignRow.OrgID = foreignClaim.OrgID
	foreignRaw, err := json.Marshal(foreignRow)
	if err != nil {
		t.Fatal(err)
	}
	foreignEffect, err := BuildEffectBatch("git_pull_requests", EffectReadbackRequired, []json.RawMessage{foreignRaw})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, pullEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant crossed readback fence: inspection=%s error=%v", inspection, err)
	}

	foreign := claim
	foreign.Provider = "github"
	if err := sink.WriteEffect(ctx, foreign, pullEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign provider write=%v want ErrInvalidConfiguration", err)
	}
	leaseSink := GitLabPullRequestSocialClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		}),
	}
	if err := leaseSink.WriteEffect(ctx, claim, pullEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease-guarded write=%v want lease error", err)
	}
}
