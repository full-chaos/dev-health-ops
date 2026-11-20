//go:build integration

package datahealth

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestIdentityMappingCountsAResyncedCommitOnce: B5 of CHAOS-14285. A commit synced twice (git_commits is
// ReplacingMergeTree(last_synced), merges stopped) is one observed commit of its author; org B, on the same
// repository id and hash, keeps its own count.
func TestIdentityMappingCountsAResyncedCommitOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES git_commits`); err != nil {
		t.Fatal(err)
	}
	const (
		orgA = "00000000-0000-4000-8000-0000001428a0"
		orgB = "00000000-0000-4000-8000-0000001428b0"
		repo = "00000000-0000-4000-8000-000000142852"
	)
	commit := func(org, hash, synced string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, org_id, last_synced)
VALUES (toUUID('`+repo+`'), '`+hash+`', 'Alice', 'alice@example.com', now64(3) - INTERVAL 3 DAY, now64(3) - INTERVAL 3 DAY, '`+org+`', now64(3) - INTERVAL `+synced+`)`); err != nil {
			t.Fatal(err)
		}
	}
	commit(orgA, "c1", "2 HOUR")
	commit(orgA, "c1", "1 HOUR")
	commit(orgA, "c2", "2 HOUR")
	commit(orgB, "c1", "2 HOUR")

	reader := &Reader{ClickHouse: client}
	check := func(phase string) {
		for _, c := range []struct {
			org  string
			want int
		}{{orgA, 2}, {orgB, 1}} {
			health := reader.IdentityMapping(ctx, c.org, "")
			got := -1
			for _, u := range health.UnmappedIdentities {
				if u.Email != nil && *u.Email == "alice@example.com" && u.ObservedCount != nil {
					got = *u.ObservedCount
				}
			}
			t.Logf("%s: org %s alice observed_count %d", phase, c.org[len(c.org)-4:], got)
			if got != c.want {
				t.Errorf("%s: org %s alice observed_count %d, want %d", phase, c.org[len(c.org)-4:], got, c.want)
			}
		}
	}
	check("separate parts")
	if err := conn.Exec(ctx, `SYSTEM START MERGES git_commits`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `OPTIMIZE TABLE git_commits FINAL`); err != nil {
		t.Fatal(err)
	}
	check("after OPTIMIZE FINAL")
}
