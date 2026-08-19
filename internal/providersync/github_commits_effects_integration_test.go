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

const gitCommitsDDL = `CREATE TABLE git_commits (org_id String, repo_id UUID, hash String, message String, author_name String, author_email Nullable(String), author_when DateTime64(3, 'UTC'), committer_name String, committer_email Nullable(String), committer_when DateTime64(3, 'UTC'), parents UInt32, last_synced DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, hash)`

func TestGitHubCommitsReadbackUsesFinalAndTenantPredicate(t *testing.T) {
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
	if err := conn.Exec(ctx, gitCommitsDDL); err != nil {
		t.Fatal(err)
	}
	sink := GitHubCommitsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	claim := nativeTestClaim("github", "commits")
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	message := "message"
	row := gitCommitRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Hash: "abc", Message: &message, AuthorName: "author", AuthorWhen: now, CommitterName: "committer", CommitterWhen: now, LastSynced: now}
	effect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, []gitCommitRow{row})
	if err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "other-org"
	foreign.LastSynced = now.Add(time.Minute)
	foreignEffect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, []gitCommitRow{foreign})
	if err != nil {
		t.Fatal(err)
	}
	foreignClaim := claim
	foreignClaim.OrgID = foreign.OrgID
	if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign row inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("tenant FINAL inspection=%s error=%v", inspection, err)
	}
}
