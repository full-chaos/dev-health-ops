//go:build integration

package syncdispatchruntime

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type touchedTestCommit struct {
	org       string
	repo      uuid.UUID
	hash      string
	author    time.Time
	committer time.Time
	synced    time.Time
}

func insertTouchedTestCommits(t *testing.T, ctx context.Context, conn driver.Conn, commits ...touchedTestCommit) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_commits (
		repo_id, hash, author_when, committer_when, parents, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, commit := range commits {
		if err := batch.Append(commit.repo, commit.hash, commit.author, commit.committer, uint32(1), commit.org, commit.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

type touchedTestPullRequest struct {
	org     string
	repo    uuid.UUID
	number  uint32
	created time.Time
	merged  *time.Time
	synced  time.Time
}

func insertTouchedTestPullRequests(t *testing.T, ctx context.Context, conn driver.Conn, pullRequests ...touchedTestPullRequest) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_pull_requests (
		repo_id, number, created_at, merged_at, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, pr := range pullRequests {
		if err := batch.Append(pr.repo, pr.number, pr.created, pr.merged, pr.org, pr.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-9169: a commit or a pull request that a sync WROTE marks its day (and
// repository) in the touched-day record, however old the day is and wherever the
// sync's own window lies; a row written before the lower bound of the read, and
// a row of another organization, mark nothing. The days are the ones the daily
// run reads them under: the committer date of a commit (never its author date),
// the creation and the merge day of a pull request.
func TestTouchedDaysRecordHoldsTheDaysOfTheCommitsAndPullRequestsOfTheRun(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Millisecond)
	merged := touchedTestDay(time.June, 15)
	insertTouchedTestCommits(t, ctx, conn,
		// A late commit: committed on 05-03, written in this run (its day is far outside the run's own window).
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "late", author: touchedTestDay(time.May, 3), committer: touchedTestDay(time.May, 3), synced: inRun},
		// Rebased: the author day is not a day the daily run reads the commit under.
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "rebased", author: touchedTestDay(time.March, 1), committer: touchedTestDay(time.May, 4), synced: inRun},
		// Written before the run, and written in another organization: no key.
		touchedTestCommit{org: touchedTestOrg, repo: touchedTestRepoA, hash: "before", author: touchedTestDay(time.April, 9), committer: touchedTestDay(time.April, 9), synced: beforeRun},
		touchedTestCommit{org: touchedTestOtherOrg, repo: touchedTestRepoB, hash: "other", author: touchedTestDay(time.April, 10), committer: touchedTestDay(time.April, 10), synced: inRun},
	)
	insertTouchedTestPullRequests(t, ctx, conn,
		// Created on 06-02 and merged on 06-15: both days.
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 7, created: touchedTestDay(time.June, 2), merged: &merged, synced: inRun},
		// Open: only its creation day.
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 8, created: touchedTestDay(time.June, 3), synced: inRun},
		touchedTestPullRequest{org: touchedTestOrg, repo: touchedTestRepoB, number: 9, created: touchedTestDay(time.April, 20), synced: beforeRun},
		touchedTestPullRequest{org: touchedTestOtherOrg, repo: touchedTestRepoA, number: 10, created: touchedTestDay(time.April, 21), synced: inRun},
	)
	want := []string{
		touchedTestKey(time.May, 3, touchedTestRepoA),
		touchedTestKey(time.May, 4, touchedTestRepoA),
		touchedTestKey(time.June, 2, touchedTestRepoB),
		touchedTestKey(time.June, 3, touchedTestRepoB),
		touchedTestKey(time.June, 15, touchedTestRepoB),
	}
	sort.Strings(want)

	recorded, err := store.RecordTouchedGit(ctx, touchedTestOrg, touchedTestSince)
	if err != nil {
		t.Fatal(err)
	}
	got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys (recorded %d)\n got %v\nwant %v", recorded, got, want)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization was not recorded, so it has no key; got %v", got)
	}
}
