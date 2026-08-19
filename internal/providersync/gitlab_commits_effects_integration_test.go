//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const gitLabCommitsDDL = `CREATE TABLE git_commits (org_id String, repo_id UUID, hash String, message Nullable(String), author_name String, author_email Nullable(String), author_when DateTime64(3, 'UTC'), committer_name String, committer_email Nullable(String), committer_when DateTime64(3, 'UTC'), parents UInt32, last_synced DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, hash)`

func TestGitLabCommitsReadbackSeparatesTenantsAndConvergesAfterRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(ctx)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, gitLabCommitsDDL); err != nil {
		t.Fatal(err)
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabCommitsClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("gitlab", "commits")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	row := gitCommitRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		Hash: "abc", Message: nil, AuthorName: "author", AuthorWhen: now,
		CommitterName: "committer", CommitterWhen: now, LastSynced: now,
	}
	effect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, []gitCommitRow{row})
	if err != nil {
		t.Fatal(err)
	}
	foreignClaim := claim
	foreignClaim.OrgID = "other-org"
	foreign := row
	foreign.OrgID = foreignClaim.OrgID
	foreignMessage := "foreign tenant"
	foreign.Message = &foreignMessage
	foreign.AuthorName = "foreign-author"
	foreign.LastSynced = now.Add(time.Minute)
	foreignEffect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, []gitCommitRow{foreign})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("local before write inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	for name, checked := range map[string]struct {
		claim  Claim
		effect EffectBatch
	}{
		"local":   {claim: claim, effect: effect},
		"foreign": {claim: foreignClaim, effect: foreignEffect},
	} {
		inspection, inspectErr := sink.InspectEffect(ctx, checked.claim, checked.effect)
		if inspectErr != nil || inspection != EffectExact {
			t.Fatalf("%s inspection=%s error=%v", name, inspection, inspectErr)
		}
	}
	emptyMessage := ""
	changed := row
	changed.Message = &emptyMessage
	changed.LastSynced = now.Add(2 * time.Minute)
	changedEffect, err := effectBatchFromValues(
		"git_commits", EffectReadbackRequired, []gitCommitRow{changed},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, changedEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectConflict {
		t.Fatalf("null/empty readback inspection=%s error=%v", inspection, err)
	}
}
