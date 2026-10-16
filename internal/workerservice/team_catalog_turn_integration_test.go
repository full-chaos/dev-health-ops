//go:build integration

package workerservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// raceStampCollector writes ONE new membership the way the four provider writers do: it takes
// the valid_from from the OPEN rows through the shared rule (ReuseFirstSeenMembershipValidFrom),
// then inserts. The pause between the read and the insert is what two overlapping runs hit.
type raceStampCollector struct {
	conn driver.Conn
	from func() time.Time
}

func (c raceStampCollector) CollectTeamCatalog(ctx context.Context, ref providersync.TeamCatalogReference, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ providersync.TeamCatalogSelections, _ time.Time,
) (providersync.TeamCatalogResult, error) {
	type row struct {
		team, member string
		from         time.Time
	}
	rows, err := providersync.ReuseFirstSeenMembershipValidFrom(ctx, c.conn, ref.OrgID, []row{{"linear:ENG", "linear:alice", c.from()}}, providersync.MembershipFirstSeenAccessors[row]{
		Provider:  func(row) string { return "linear" },
		Source:    func(row) string { return "native" },
		TeamID:    func(r row) string { return r.team },
		MemberID:  func(r row) string { return r.member },
		ValidFrom: func(r row) time.Time { return r.from },
		SetFrom:   func(r *row, at time.Time) { r.from = at },
	})
	if err != nil {
		return providersync.TeamCatalogResult{}, err
	}
	time.Sleep(700 * time.Millisecond) // the window between the read of the open rows and the insert
	if err := c.conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at)
VALUES (?, 'linear', ?, ?, [], 'native', 1, 100, 10, ?, ?)`, ref.OrgID, rows[0].team, rows[0].member, rows[0].from, time.Now().UTC()); err != nil {
		return providersync.TeamCatalogResult{}, err
	}
	return providersync.TeamCatalogResult{MembershipsWritten: 1}, nil
}

// CHAOS-9140 (D5917) on real ClickHouse and Postgres: two overlapping runs of one organization and
// provider leave TWO open rows of a new fact; behind the lease turn they leave ONE, also on pools
// of ONE connection each (no connection is held while a run lives), and another provider or
// organization does not wait.
func TestCatalogLeaseTurnKeepsOneOpenRowOfANewFactAcrossOverlappingRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	setup, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	pgschema.Apply(ctx, t, setup)
	setup.Close()
	// Two processes' pools, ONE connection each (the smallest valid configuration).
	poolA, err := pgxpool.New(ctx, pg.URI+"&pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(poolA.Close)
	poolB, err := pgxpool.New(ctx, pg.URI+"&pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(poolB.Close)
	turns := []jobruntime.TeamCatalogTurn{jobruntime.NewTeamCatalogTurn(poolA), jobruntime.NewTeamCatalogTurn(poolB)}

	openRows := func(org string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = 'linear' AND team_id = 'linear:ENG' AND member_id = 'linear:alice' AND valid_to IS NULL`, org).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	run := func(org string, serialized bool) {
		t.Helper()
		var tick int
		var mu sync.Mutex
		collector := raceStampCollector{conn: conn, from: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			tick++
			return time.Date(2026, 10, 1, tick, 0, 0, 0, time.UTC) // each run's own clock
		}}
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			var serializer providersync.TeamCatalogSerializer
			if serialized {
				serializer = turns[i] // the two runs come from two different processes
			}
			registry := providersync.CarryFirstSerializedTeamCatalogCollectors(conn, serializer, map[string]providersync.TeamCatalogCollector{"linear": collector})
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := registry["linear"].CollectTeamCatalog(ctx, providersync.TeamCatalogReference{OrgID: org}, providerfoundation.Credential{},
					nil, providersync.TeamCatalogSelections{Teams: true}, time.Now()); err != nil {
					t.Errorf("run: %v", err)
				}
			}()
		}
		wg.Wait()
	}

	t.Run("without the turn: two open rows (the defect)", func(t *testing.T) {
		org := uuid.NewString()
		run(org, false)
		if got := openRows(org); got != 2 {
			t.Fatalf("open rows = %d, want 2: the control must show the race", got)
		}
	})
	t.Run("with the turn, two processes, pools of one connection: one open row", func(t *testing.T) {
		org := uuid.NewString()
		run(org, true)
		if got := openRows(org); got != 1 {
			t.Fatalf("open rows = %d, want 1", got)
		}
	})
	t.Run("the turn: a second holder waits for the release, another provider or organization does not, a context ends the wait", func(t *testing.T) {
		org := uuid.NewString()
		release, err := turns[0].Serialize(ctx, org, "linear")
		if err != nil {
			t.Fatal(err)
		}
		held := true
		defer func() {
			if held {
				release()
			}
		}()
		// the real budget lease carries a loss signal (a lost lease cancels the run: round 117 P1)
		probeOrg := uuid.NewString()
		probeRelease, lost, err := turns[0].SerializeWithLoss(ctx, probeOrg, "linear")
		if err != nil {
			t.Fatal(err)
		}
		if lost == nil {
			t.Error("the real budget lease gave no loss signal")
		}
		probeRelease()
		other, err := turns[1].Serialize(ctx, org, "github")
		if err != nil {
			t.Fatalf("another provider of the organization waited: %v", err)
		}
		other()
		another, err := turns[1].Serialize(ctx, uuid.NewString(), "linear")
		if err != nil {
			t.Fatalf("another organization waited: %v", err)
		}
		another()
		short, stop := context.WithTimeout(ctx, 600*time.Millisecond)
		defer stop()
		if next, err := turns[1].Serialize(short, org, "linear"); err == nil {
			next()
			t.Fatal("a second holder of the same key (another process) got the turn while the first held it")
		}
		got := make(chan struct{})
		go func() {
			next, err := turns[1].Serialize(ctx, org, "linear")
			if err == nil {
				next()
			}
			close(got)
		}()
		select {
		case <-got:
			t.Fatal("the waiting holder ran before the release")
		case <-time.After(400 * time.Millisecond):
		}
		release()
		held = false
		select {
		case <-got:
		case <-time.After(10 * time.Second):
			t.Fatal("the waiting holder did not get the turn after the release")
		}
	})
}
