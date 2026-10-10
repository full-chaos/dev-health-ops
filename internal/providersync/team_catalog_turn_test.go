package providersync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// zeroCountCarryConn answers the carry's count read with 0 (nothing to carry), safe for concurrent runs.
type zeroCountCarryConn struct{}

func (zeroCountCarryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &zeroCountCarryRows{}, nil
}

func (zeroCountCarryConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("no batch")
}

type zeroCountCarryRows struct {
	driver.Rows
	read bool
}

func (rows *zeroCountCarryRows) Next() bool {
	next := !rows.read
	rows.read = true
	return next
}

func (rows *zeroCountCarryRows) Scan(dest ...any) error {
	for _, target := range dest {
		*target.(*uint64) = 0
	}
	return nil
}

func (rows *zeroCountCarryRows) Err() error   { return nil }
func (rows *zeroCountCarryRows) Close() error { return nil }

// turnSerializer is a fake turn: one holder per (org, provider) key at a time.
type turnSerializer struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
	err   error
	calls atomic.Int32
}

func (s *turnSerializer) Serialize(_ context.Context, orgID, provider string) (func(), error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	s.mu.Lock()
	if s.locks == nil {
		s.locks = map[string]*sync.Mutex{}
	}
	lock := s.locks[orgID+"|"+provider]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[orgID+"|"+provider] = lock
	}
	s.mu.Unlock()
	lock.Lock()
	return lock.Unlock, nil
}

type turnProbeCollector struct {
	running, maxRunning *atomic.Int32
	ran                 *atomic.Int32
}

func (c turnProbeCollector) CollectTeamCatalog(context.Context, TeamCatalogReference, providerfoundation.Credential,
	*providerfoundation.HTTPClient, TeamCatalogSelections, time.Time,
) (TeamCatalogResult, error) {
	c.ran.Add(1)
	now := c.running.Add(1)
	for {
		seen := c.maxRunning.Load()
		if now <= seen || c.maxRunning.CompareAndSwap(seen, now) {
			break
		}
	}
	time.Sleep(60 * time.Millisecond)
	c.running.Add(-1)
	return TeamCatalogResult{TeamsWritten: 1}, nil
}

// CHAOS-9140: two runs of one (organization, provider) catalog take turns; two
// providers of one organization, and two organizations, do not wait for each other.
func TestTeamCatalogRunsOfOneOrganizationAndProviderTakeTurns(t *testing.T) {
	run := func(t *testing.T, serializer TeamCatalogSerializer, keys [][2]string) (maxRunning int32) {
		t.Helper()
		var running, max, ran atomic.Int32
		registry := CarryFirstSerializedTeamCatalogCollectors(zeroCountCarryConn{}, serializer, map[string]TeamCatalogCollector{
			"linear": turnProbeCollector{running: &running, maxRunning: &max, ran: &ran},
			"github": turnProbeCollector{running: &running, maxRunning: &max, ran: &ran},
		})
		var wg sync.WaitGroup
		for _, key := range keys {
			wg.Add(1)
			go func(org, provider string) {
				defer wg.Done()
				if _, err := registry[provider].CollectTeamCatalog(context.Background(), TeamCatalogReference{OrgID: org},
					providerfoundation.Credential{}, nil, TeamCatalogSelections{Teams: true}, time.Now()); err != nil {
					t.Errorf("%s/%s: %v", org, provider, err)
				}
			}(key[0], key[1])
		}
		wg.Wait()
		if int(ran.Load()) != len(keys) {
			t.Errorf("collectors ran %d times, want %d (a turn delays a run, it never skips one)", ran.Load(), len(keys))
		}
		return max.Load()
	}
	same := [][2]string{{"org-1", "linear"}, {"org-1", "linear"}, {"org-1", "linear"}}
	if got := run(t, &turnSerializer{}, same); got != 1 {
		t.Errorf("three runs of one organization and provider overlapped: %d at once, want 1", got)
	}
	if got := run(t, nil, same); got < 2 {
		t.Errorf("control: without a serializer the runs did not overlap (%d at once): the probe cannot see an overlap", got)
	}
	if got := run(t, &turnSerializer{}, [][2]string{{"org-1", "linear"}, {"org-1", "github"}, {"org-2", "linear"}}); got < 2 {
		t.Errorf("runs of other keys waited for each other (%d at once, want 2 or more)", got)
	}
}

func TestTeamCatalogTurnFailureStopsTheRunBeforeTheCarryAndTheCollector(t *testing.T) {
	failed := errors.New("no turn")
	conn := &carrySeamConn{}
	ran := false
	registry := CarryFirstSerializedTeamCatalogCollectors(conn, &turnSerializer{err: failed},
		map[string]TeamCatalogCollector{"linear": carrySeamCollector{ran: &ran}})
	_, err := registry["linear"].CollectTeamCatalog(context.Background(), TeamCatalogReference{OrgID: "org-1"},
		providerfoundation.Credential{}, nil, TeamCatalogSelections{Teams: true}, time.Now())
	if !errors.Is(err, failed) || ran || len(conn.queries) != 0 || conn.batches != 0 {
		t.Fatalf("err = %v, collector ran = %v, carry reads = %d, batches = %d; want the turn's error and nothing else", err, ran, len(conn.queries), conn.batches)
	}
}
