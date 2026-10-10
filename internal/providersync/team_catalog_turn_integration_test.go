//go:build integration

package providersync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// raceStampCollector is a catalog collector that writes ONE new membership the
// way the four provider writers do: it takes the valid_from from the OPEN rows
// through the shared rule (ReuseFirstSeenMembershipValidFrom), then inserts. The
// pause between the read and the insert is what two overlapping runs hit.
type raceStampCollector struct {
	conn driver.Conn
	from func() time.Time
}

func (c raceStampCollector) CollectTeamCatalog(ctx context.Context, ref TeamCatalogReference, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ TeamCatalogSelections, _ time.Time,
) (TeamCatalogResult, error) {
	type row struct {
		team, member string
		from         time.Time
	}
	rows, err := ReuseFirstSeenMembershipValidFrom(ctx, c.conn, ref.OrgID, []row{{"linear:ENG", "linear:alice", c.from()}}, MembershipFirstSeenAccessors[row]{
		Provider:  func(row) string { return "linear" },
		Source:    func(row) string { return "native" },
		TeamID:    func(r row) string { return r.team },
		MemberID:  func(r row) string { return r.member },
		ValidFrom: func(r row) time.Time { return r.from },
		SetFrom:   func(r *row, at time.Time) { r.from = at },
	})
	if err != nil {
		return TeamCatalogResult{}, err
	}
	time.Sleep(700 * time.Millisecond) // the window between the read of the open rows and the insert
	if err := c.conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at)
VALUES (?, 'linear', ?, ?, [], 'native', 1, 100, 10, ?, ?)`, ref.OrgID, rows[0].team, rows[0].member, rows[0].from, time.Now().UTC()); err != nil {
		return TeamCatalogResult{}, err
	}
	return TeamCatalogResult{MembershipsWritten: 1}, nil
}

// CHAOS-9140 on real ClickHouse and Postgres: two runs of one organization and
// provider that overlap leave TWO open rows of a new fact; behind the per-
// (organization, provider) turn they leave ONE, and the turn is the Postgres
// advisory lock: a second holder waits for the first, another key does not.
func TestTeamCatalogTurnKeepsOneOpenRowOfANewFactAcrossOverlappingRuns(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	pgCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(pgCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(pgCtx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	turn := NewPostgresTeamCatalogTurn(pool, 50*time.Millisecond)

	openRows := func(org string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = 'linear' AND team_id = 'linear:ENG' AND member_id = 'linear:alice' AND valid_to IS NULL`, org).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	runTwoOverlapping := func(org string, serializer TeamCatalogSerializer) {
		t.Helper()
		var tick int
		var mu sync.Mutex
		collector := raceStampCollector{conn: conn, from: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			tick++
			return time.Date(2026, 10, 1, tick, 0, 0, 0, time.UTC) // each run's own clock
		}}
		registry := CarryFirstSerializedTeamCatalogCollectors(conn, serializer, map[string]TeamCatalogCollector{"linear": collector})
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := registry["linear"].CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: org}, providerfoundation.Credential{},
					nil, TeamCatalogSelections{Teams: true}, time.Now()); err != nil {
					t.Errorf("run: %v", err)
				}
			}()
		}
		wg.Wait()
	}

	t.Run("without the turn: two open rows (the defect)", func(t *testing.T) {
		org := uuid.NewString()
		runTwoOverlapping(org, nil)
		if got := openRows(org); got != 2 {
			t.Fatalf("open rows = %d, want 2: the control must show the race", got)
		}
	})
	t.Run("with the turn: one open row", func(t *testing.T) {
		org := uuid.NewString()
		runTwoOverlapping(org, turn)
		if got := openRows(org); got != 1 {
			t.Fatalf("open rows = %d, want 1", got)
		}
	})
	t.Run("the turn: a second holder waits, another key does not, a context ends the wait", func(t *testing.T) {
		release, err := turn.Serialize(ctx, "org-a", "linear")
		if err != nil {
			t.Fatal(err)
		}
		other, err := turn.Serialize(ctx, "org-a", "github")
		if err != nil {
			t.Fatalf("another provider of the organization waited: %v", err)
		}
		other()
		shortCtx, stop := context.WithTimeout(ctx, 400*time.Millisecond)
		defer stop()
		if _, err := turn.Serialize(shortCtx, "org-a", "linear"); err == nil {
			t.Fatal("a second holder of the same key got the turn while the first held it")
		}
		got := make(chan struct{})
		go func() {
			next, err := turn.Serialize(ctx, "org-a", "linear")
			if err == nil {
				next()
			}
			close(got)
		}()
		select {
		case <-got:
			t.Fatal("the waiting holder ran before the release")
		case <-time.After(300 * time.Millisecond):
		}
		release()
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("the waiting holder did not get the turn after the release")
		}
	})
	t.Run("a holder whose connection is lost frees the turn", func(t *testing.T) {
		lost, err := turn.Serialize(ctx, "org-lost", "linear")
		if err != nil {
			t.Fatal(err)
		}
		defer lost() // a failed assertion below must not leave a held transaction to block the pool's close
		// end the holder's server session from outside: the transaction, and its lock, go with it
		if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend(a.pid) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.pid <> pg_backend_pid()`); err != nil {
			t.Fatal(err)
		}
		short, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		next, err := turn.Serialize(short, "org-lost", "linear")
		if err != nil {
			t.Fatalf("the turn was not freed by the loss of its holder: %v", err)
		}
		next()
	})

	t.Run("holders never take the last connection: the run's own queries still get one (round 111 P1)", func(t *testing.T) {
		small, err := pgxpool.New(ctx, instance.URI+"&pool_max_conns=4")
		if err != nil {
			t.Fatal(err)
		}
		defer small.Close()
		squad := NewPostgresTeamCatalogTurn(small, 20*time.Millisecond)
		// four sync workers, four domain connections: each run holds its turn and then queries the SAME pool
		var wg sync.WaitGroup
		var failed atomic.Int32
		var holding, peak atomic.Int32
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(provider string) {
				defer wg.Done()
				runCtx, stop := context.WithTimeout(ctx, 20*time.Second)
				defer stop()
				release, err := squad.Serialize(runCtx, "org-squad", provider)
				if err != nil {
					failed.Add(1)
					t.Errorf("%s: no turn: %v", provider, err)
					return
				}
				defer release()
				now := holding.Add(1)
				for {
					seen := peak.Load()
					if now <= seen || peak.CompareAndSwap(seen, now) {
						break
					}
				}
				time.Sleep(300 * time.Millisecond) // the run is under way, then it queries the pool
				var one int
				if err := small.QueryRow(runCtx, `SELECT 1`).Scan(&one); err != nil {
					failed.Add(1)
					t.Errorf("%s: the run's own query starved behind the held turns: %v", provider, err)
				}
				holding.Add(-1)
			}([]string{"linear", "github", "gitlab", "jira"}[i])
		}
		wg.Wait()
		if failed.Load() != 0 {
			t.Fatalf("%d runs failed", failed.Load())
		}
		if peak.Load() > 3 {
			t.Errorf("%d runs held a turn at once on a pool of 4 connections, want at most 3", peak.Load())
		}
	})
}
