package analytics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

type box struct{ n int }

func cloneBox(b *box) *box { c := *b; return &c }

func waitForJoiners() { time.Sleep(150 * time.Millisecond) }

func TestInflightSharesOneRunAndGivesEachCallerItsOwnCopy(t *testing.T) {
	var f inflight[*box]
	var runs atomic.Int32
	release := make(chan struct{})
	run := func(context.Context) (*box, error) {
		runs.Add(1)
		<-release
		return &box{n: 7}, nil
	}
	const callers = 5
	got := make([]*box, callers)
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := f.do(context.Background(), "k", cloneBox, run)
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
			}
			got[i] = v
		}()
	}
	waitForJoiners()
	close(release)
	wg.Wait()
	if runs.Load() != 1 {
		t.Fatalf("runs = %d, want 1", runs.Load())
	}
	got[0].n = 99
	for i := 1; i < callers; i++ {
		if got[i] == got[0] || got[i].n != 7 {
			t.Fatalf("caller %d shares or sees another caller's copy: %+v", i, got[i])
		}
	}
}

func TestInflightKeepsNothingAfterTheRun(t *testing.T) {
	var f inflight[*box]
	var runs atomic.Int32
	run := func(context.Context) (*box, error) { runs.Add(1); return &box{n: int(runs.Load())}, nil }
	for i := 1; i <= 3; i++ {
		v, err := f.do(context.Background(), "k", cloneBox, run)
		if err != nil || v.n != i {
			t.Fatalf("call %d: %+v, %v (a finished run must not be reused)", i, v, err)
		}
	}
}

func TestInflightDifferentKeysNeverShare(t *testing.T) {
	var f inflight[*box]
	var runs atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.do(context.Background(), key, cloneBox, func(context.Context) (*box, error) {
				runs.Add(1)
				<-release
				return &box{}, nil
			})
		}()
	}
	waitForJoiners()
	close(release)
	wg.Wait()
	if runs.Load() != 3 {
		t.Fatalf("runs = %d, want 3", runs.Load())
	}
}

func TestInflightLeaderCancelDoesNotFailTheWaiter(t *testing.T) {
	var f inflight[*box]
	var runs atomic.Int32
	started := make(chan struct{}, 4)
	run := func(ctx context.Context) (*box, error) {
		runs.Add(1)
		started <- struct{}{}
		if runs.Load() == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &box{n: 5}, nil
	}
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := f.do(leaderCtx, "k", cloneBox, run)
		leaderErr <- err
	}()
	<-started
	type answer struct {
		v   *box
		err error
	}
	waiter := make(chan answer, 1)
	go func() {
		v, err := f.do(context.Background(), "k", cloneBox, run)
		waiter <- answer{v, err}
	}()
	waitForJoiners()
	cancel()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v, want its own cancel", err)
	}
	a := <-waiter
	if a.err != nil || a.v == nil || a.v.n != 5 {
		t.Fatalf("waiter = %+v, %v; want its own answer", a.v, a.err)
	}
	if runs.Load() != 2 {
		t.Fatalf("runs = %d, want 2 (the leader's, then the waiter's own once)", runs.Load())
	}
}

func TestInflightWaiterCancelLeavesTheLeaderAlone(t *testing.T) {
	var f inflight[*box]
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var runs atomic.Int32
	run := func(context.Context) (*box, error) {
		runs.Add(1)
		started <- struct{}{}
		<-release
		return &box{n: 3}, nil
	}
	type answer struct {
		v   *box
		err error
	}
	leader := make(chan answer, 1)
	go func() {
		v, err := f.do(context.Background(), "k", cloneBox, run)
		leader <- answer{v, err}
	}()
	<-started
	waiterCtx, cancel := context.WithCancel(context.Background())
	waiter := make(chan answer, 1)
	go func() {
		v, err := f.do(waiterCtx, "k", cloneBox, run)
		waiter <- answer{v, err}
	}()
	waitForJoiners()
	cancel()
	if a := <-waiter; !errors.Is(a.err, context.Canceled) || a.v != nil {
		t.Fatalf("waiter = %+v, %v; want its own cancel", a.v, a.err)
	}
	close(release)
	if a := <-leader; a.err != nil || a.v == nil || a.v.n != 3 {
		t.Fatalf("leader = %+v, %v; want its answer", a.v, a.err)
	}
	if runs.Load() != 1 {
		t.Fatalf("runs = %d, want 1", runs.Load())
	}
}

func TestInflightLeaderFailureIsNotHandedToTheWaiter(t *testing.T) {
	var f inflight[*box]
	var runs atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	boom := errors.New("leader failed")
	run := func(context.Context) (*box, error) {
		started <- struct{}{}
		if runs.Add(1) == 1 {
			<-release
			return nil, boom
		}
		return &box{n: 8}, nil
	}
	leaderErr := make(chan error, 1)
	go func() {
		_, err := f.do(context.Background(), "k", cloneBox, run)
		leaderErr <- err
	}()
	<-started
	waiter := make(chan *box, 1)
	go func() {
		v, _ := f.do(context.Background(), "k", cloneBox, run)
		waiter <- v
	}()
	waitForJoiners()
	close(release)
	if err := <-leaderErr; !errors.Is(err, boom) {
		t.Fatalf("leader err = %v", err)
	}
	if v := <-waiter; v == nil || v.n != 8 {
		t.Fatalf("waiter = %+v, want its own run's answer", v)
	}
}

func TestFlightKeyFieldsNeverRunTogether(t *testing.T) {
	a := (&flightKey{}).strs([]string{"ab", "c"}).String()
	b := (&flightKey{}).strs([]string{"a", "bc"}).String()
	if a == b {
		t.Fatal("different string lists made one key")
	}
	if (&flightKey{}).filter(nil).String() == (&flightKey{}).filter(&model.FilterInput{}).String() {
		t.Fatal("no filter and an empty filter made one key")
	}
}

func TestSankeyCoverageKeySeparatesOrgsWindowsRunsAndFilters(t *testing.T) {
	base := SankeyRequest{
		Path:      []Dimension{DimensionTeam, DimensionRepo},
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"),
		EndDate:   mustGraphQLDate("2026-01-08"),
		MaxNodes:  10, MaxEdges: 20,
	}
	state := InvestmentMembershipScopeState{ScopeMode: scopeModeScoped, RunID: "run-1"}
	filters := func(repos ...string) *model.FilterInput {
		return &model.FilterInput{What: &model.WhatFilterInput{Repos: repos}}
	}
	later := base
	later.EndDate = mustGraphQLDate("2026-01-09")
	otherMeasure := base
	otherMeasure.Measure = MeasureIssueSize
	otherPath := base
	otherPath.Path = []Dimension{DimensionRepo, DimensionTeam}
	nextRun := state
	nextRun.RunID = "run-2"
	otherMode := state
	otherMode.ScopeMode = scopeModeProjectionLag
	keys := map[string]string{
		"base":          sankeyCoverageKey("org-1", 30, base, state, nil),
		"other org":     sankeyCoverageKey("org-2", 30, base, state, nil),
		"other window":  sankeyCoverageKey("org-1", 30, later, state, nil),
		"other measure": sankeyCoverageKey("org-1", 30, otherMeasure, state, nil),
		"other path":    sankeyCoverageKey("org-1", 30, otherPath, state, nil),
		"other run":     sankeyCoverageKey("org-1", 30, base, nextRun, nil),
		"other mode":    sankeyCoverageKey("org-1", 30, base, otherMode, nil),
		"other timeout": sankeyCoverageKey("org-1", 31, base, state, nil),
		"filters a":     sankeyCoverageKey("org-1", 30, base, state, filters("a")),
		"filters b":     sankeyCoverageKey("org-1", 30, base, state, filters("b")),
		"filters a,b":   sankeyCoverageKey("org-1", 30, base, state, filters("a", "b")),
		"filters ab":    sankeyCoverageKey("org-1", 30, base, state, filters("ab")),
		"who a":         sankeyCoverageKey("org-1", 30, base, state, &model.FilterInput{Who: &model.WhoFilterInput{Developers: []string{"a"}}}),
		"scope a":       sankeyCoverageKey("org-1", 30, base, state, &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInput("TEAM"), Ids: []string{"a"}}}),
	}
	seen := map[string]string{}
	for name, key := range keys {
		if other, dup := seen[key]; dup {
			t.Errorf("%q and %q share one key", name, other)
		}
		seen[key] = name
	}
	if sankeyCoverageKey("org-1", 30, base, state, nil) != keys["base"] {
		t.Error("the same question made two keys")
	}
}

type gatedCoverageClient struct {
	queries atomic.Int32
	release chan struct{}
}

func (c *gatedCoverageClient) Query(ctx context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if !strings.Contains(statement, "AS assigned_team") {
		return nil, errors.New("gatedCoverageClient: telemetry statement not served")
	}
	c.queries.Add(1)
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &fakeRowScanner{rows: [][]any{{4.0, 1.0, 2.0, 1.0, nil, nil, nil, nil, nil}}}, nil
}

func TestResolveSankeyCoverageSharesOneStatementPerRunAndKeepsCopiesApart(t *testing.T) {
	old := fetchInvestmentMembershipScopeState
	var runID atomic.Value
	runID.Store("run-1")
	fetchInvestmentMembershipScopeState = func(context.Context, QueryClient, string, int) (InvestmentMembershipScopeState, error) {
		return InvestmentMembershipScopeState{ScopeMode: scopeModeScoped, RunID: runID.Load().(string)}, nil
	}
	t.Cleanup(func() { fetchInvestmentMembershipScopeState = old })
	req := SankeyRequest{Measure: MeasureCount, StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}

	client := &gatedCoverageClient{release: make(chan struct{})}
	got := make([]*model.SankeyCoverage, 3)
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = resolveSankeyCoverage(context.Background(), client, "org-1", req, 30, true, nil)
		}()
	}
	waitForJoiners()
	close(client.release)
	wg.Wait()
	if n := client.queries.Load(); n != 1 {
		t.Fatalf("coverage statements = %d, want 1", n)
	}
	for i, c := range got {
		if c == nil || c.TeamCoverage != 0.25 || c.RepoCoverage != 0.5 {
			t.Fatalf("caller %d coverage = %+v", i, c)
		}
	}
	got[0].TeamCoverage = 0.99
	if got[1] == got[0] || got[1].TeamCoverage != 0.25 || got[2].TeamCoverage != 0.25 {
		t.Fatal("a caller's change reached another caller")
	}

	next := &gatedCoverageClient{release: make(chan struct{})}
	close(next.release)
	runID.Store("run-2")
	resolveSankeyCoverage(context.Background(), next, "org-1", req, 30, true, nil)
	resolveSankeyCoverage(context.Background(), next, "org-2", req, 30, true, nil)
	if n := next.queries.Load(); n != 2 {
		t.Fatalf("two organisations ran %d coverage statements, want 2", n)
	}
}
