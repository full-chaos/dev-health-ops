//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestGitLabFilesReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitLabFilesIntegrationSink(t)
	claim := nativeTestClaim("gitlab", "files")
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	current := gitFileRow{RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LastSynced: now, OrgID: claim.OrgID}
	currentText := "package main\n"
	current.Contents = &currentText
	previous := current
	previous.LastSynced = now.Add(-time.Hour)
	previousText := "package old\n"
	previous.Contents = &previousText
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitLabFileEffect(t, current)); err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, current)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitLabFileEffect(t, current)); err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
	// Replaying the exact effect must converge on the same RMT winner rather
	// than create a conflicting readback generation.
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, current)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitLabFileEffect(t, current)); err != nil || inspection != EffectExact {
		t.Fatalf("replay inspection=%s error=%v", inspection, err)
	}
}

func TestGitLabFilesReadbackScopesSameNaturalKeyByTenant(t *testing.T) {
	ctx, sink := newGitLabFilesIntegrationSink(t)
	claim := nativeTestClaim("gitlab", "files")
	claim.OrgID = "org-a"
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	row := gitFileRow{RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LastSynced: now, OrgID: claim.OrgID}
	textA := "package orga\n"
	row.Contents = &textA
	otherClaim := claim
	otherClaim.OrgID = "org-b"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	textB := "package orgb\n"
	otherRow.Contents = &textB
	if err := sink.WriteEffect(ctx, otherClaim, gitLabFileEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	var count uint64
	var minOrgID, maxOrgID, minContents, maxContents string
	if err := sink.Conn.QueryRow(ctx, `
SELECT count(), min(org_id), max(org_id), min(ifNull(contents, '')), max(ifNull(contents, ''))
FROM git_files FINAL
WHERE org_id = ? AND repo_id = ? AND path = ?`, row.OrgID, row.RepoID, row.Path).
		Scan(&count, &minOrgID, &maxOrgID, &minContents, &maxContents); err != nil {
		t.Fatal(err)
	}
	if count != 1 || minOrgID != claim.OrgID || maxOrgID != claim.OrgID ||
		minContents != textA || maxContents != textA {
		t.Fatalf("tenant readback count=%d orgs=(%q,%q) contents=(%q,%q)", count, minOrgID, maxOrgID, minContents, maxContents)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitLabFileEffect(t, row)); err != nil || inspection != EffectExact {
		t.Fatalf("claim inspection=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, otherClaim, gitLabFileEffect(t, otherRow)); err != nil || inspection != EffectExact {
		t.Fatalf("other tenant inspection=%s error=%v", inspection, err)
	}
}

func TestGitLabFilesWritePreservesExistingContentsForPathsOnlyRewrite(t *testing.T) {
	ctx, sink := newGitLabFilesIntegrationSink(t)
	claim := nativeTestClaim("gitlab", "files")
	base := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	content := "package existing\n"
	withContent := gitFileRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go",
		Contents: &content, LastSynced: base, OrgID: claim.OrgID,
	}
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, withContent)); err != nil {
		t.Fatal(err)
	}
	pathsOnly := withContent
	pathsOnly.Contents = nil
	pathsOnly.LastSynced = base.Add(time.Hour)
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, pathsOnly)); err != nil {
		t.Fatal(err)
	}
	var got *string
	if err := sink.Conn.QueryRow(ctx, `
SELECT contents FROM git_files FINAL
WHERE org_id = ? AND repo_id = ? AND path = ?`, claim.OrgID, withContent.RepoID, withContent.Path).
		Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != content {
		t.Fatalf("paths-only rewrite lost existing content: %#v", got)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitLabFileEffect(t, pathsOnly)); err != nil || inspection != EffectExact {
		t.Fatalf("paths-only readback=%s error=%v", inspection, err)
	}
}

func TestGitLabFilesWriteRechecksLeaseBeforeClickHouseSend(t *testing.T) {
	ctx, sink := newGitLabFilesIntegrationSink(t)
	lease := &gitLabFilesLeaseAfterFirstAssert{}
	sink.Lease = lease
	claim := nativeTestClaim("gitlab", "files")
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	row := gitFileRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go",
		LastSynced: now, OrgID: claim.OrgID,
	}
	text := "package main\n"
	row.Contents = &text
	if err := sink.WriteEffect(ctx, claim, gitLabFileEffect(t, row)); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease-before-send error=%v", err)
	}
	var count uint64
	if err := sink.Conn.QueryRow(ctx, `
SELECT count() FROM git_files FINAL WHERE org_id = ? AND repo_id = ? AND path = ?`,
		row.OrgID, row.RepoID, row.Path).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("lease-lost write landed %d rows", count)
	}
}

type gitLabFilesLeaseAfterFirstAssert struct{ calls int }

func (lease *gitLabFilesLeaseAfterFirstAssert) Assert(context.Context) error {
	lease.calls++
	if lease.calls > 1 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}

func newGitLabFilesIntegrationSink(t *testing.T) (context.Context, GitLabFilesClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitFilesDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitLabFilesClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
}

func gitLabFileEffect(t *testing.T, row gitFileRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, []gitFileRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
