//go:build integration

package daily

// Cells for the findings of the second Codex round on the snapshot marker (real ClickHouse; merges stopped so a count of stored
// rows is the rows written). Overlap cells interleave a second run between the first run's live-key read and its writes by
// a hook on the connection; the snapshot cells run the production family order (testops_risk reads the suites into the share,
// testops_test writes) with controlled clocks.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// hookConn runs hook once, between the writer's live-key scan and its write: before a carried-values read (a query that holds
// argMax and no HAVING; the live-key read has a HAVING) or, when the writer reads the carried values with the scan, before its
// INSERT batch.
type hookConn struct {
	driver.Conn
	marker string
	hook   func()
	fired  bool
}

func (c *hookConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if !c.fired && strings.Contains(query, c.marker) && !strings.Contains(query, "HAVING") {
		c.fired = true
		c.hook()
	}
	return c.Conn.Query(ctx, query, args...)
}

func (c *hookConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !c.fired && strings.Contains(query, "INSERT INTO testops_test_metrics_daily") {
		c.fired = true
		c.hook()
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

func overlapSeed(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID, version time.Time, team, service string) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate,
  failure_rate, flake_rate, total_suites, flake_cases, distinct_cases, team_id, service_id, org_id, computed_at)
VALUES (?, ?, 4, 2, 2, 0.5, 0.5, 0.5, 1, 1, 2, ?, ?, ?, ?)`, repo, snapshotDay, team, service, snapshotOrg, version.UTC()); err != nil {
		t.Fatal(err)
	}
}

// markerRows is every marker of the key: its version, team and service.
func markerRows(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(computed_at) || '|' || ifNull(team_id, '') || '|' || ifNull(service_id, '')
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? AND retracted = 1 ORDER BY computed_at`, snapshotOrg, repo, snapshotDay)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	t.Logf("OVERLAP MARKERS repo=%s %v", repo, out)
	return out
}

// KNOWN LIMIT (ticket 14016, filed separately): two runs read the same live row (neither sees the other's marker), then both
// write: the key holds two markers, of two snapshots. Every reader answers as it does with one marker
// (TestTwoMarkersOnAKeyAnswerEveryReaderAsOneMarkerDoes), so the duplicate costs a row and nothing else.
func TestNamedLimitTwoRunsThatSawTheSameLiveRowWriteTwoMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	repo := uuid.MustParse("13928000-0000-4000-8000-0000000000f1")
	overlapSeed(t, ctx, conn, repo, snapshotAt(5), "team-1", "svc-1")
	hooked := &hookConn{Conn: conn, marker: "argMax(", hook: func() { snapshotPartitionRetire(t, ctx, conn, repo, snapshotAt(25)) }}
	scope := newStaleKeyScope([]string{repo.String()})
	if _, err := supersedeStaleTeamKeys(ctx, hooked, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay, scope, nil, snapshotAt(20)); err != nil {
		t.Fatal(err)
	}
	if !hooked.fired {
		t.Fatal("the interleave hook never fired: the second run did not run between the scan and the write, nothing was tested")
	}
	retiredForEveryReader(t, ctx, conn, repo)
	got := markerRows(t, ctx, conn, repo)
	t.Logf("KNOWN LIMIT (ticket 14016): %d markers for one retired key: %v", len(got), got)
	if len(got) != 2 {
		t.Fatalf("%d markers, the limit is 2 (one per snapshot): %v", len(got), got)
	}
}

// KNOWN LIMIT (ticket 14016): the partition writer and the whole-organization finalizer read the same live row: two markers.
func TestNamedLimitThePartitionWriterAndTheFinalizerThatSawTheSameLiveRowWriteTwoMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	repo := uuid.MustParse("13928000-0000-4000-8000-0000000000f2")
	overlapSeed(t, ctx, conn, repo, snapshotAt(5), "team-1", "svc-1")
	hooked := &hookConn{Conn: conn, marker: "argMax(", hook: func() { snapshotOrgRetire(t, ctx, conn, snapshotAt(25), noRepositories) }}
	scope := newStaleKeyScope([]string{repo.String()})
	if _, err := supersedeStaleTeamKeys(ctx, hooked, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay, scope, nil, snapshotAt(20)); err != nil {
		t.Fatal(err)
	}
	if !hooked.fired {
		t.Fatal("the interleave hook never fired: the second run did not run between the scan and the write, nothing was tested")
	}
	retiredForEveryReader(t, ctx, conn, repo)
	got := markerRows(t, ctx, conn, repo)
	t.Logf("KNOWN LIMIT (ticket 14016): %d markers for one retired key: %v", len(got), got)
	if len(got) != 2 {
		t.Fatalf("%d markers, the limit is 2 (one per snapshot): %v", len(got), got)
	}
}

// KNOWN LIMIT (ticket 14016): a live row of the same second arrives after the scan; the marker and the row then hold one version
// and which of the two a reader takes is a ClickHouse tie that these cells do not prove. The cell logs the winner it saw.
func TestNamedLimitAMarkerCanTieALiveRowThatArrivedAfterTheScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	for name, retire := range map[string]func(driver.Conn, uuid.UUID){
		"partition": func(c driver.Conn, repo uuid.UUID) {
			scope := newStaleKeyScope([]string{repo.String()})
			if _, err := supersedeStaleTeamKeys(ctx, c, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay, scope, nil, snapshotAt(10)); err != nil {
				t.Fatal(err)
			}
		},
		"finalizer": func(c driver.Conn, repo uuid.UUID) {
			if _, err := retractStaleTeamKeysOutsideRun(ctx, c, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay,
				newStaleKeyScope(), noRepositories, snapshotAt(10)); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			conn := snapshotConn(t, ctx)
			repo := uuid.New()
			overlapSeed(t, ctx, conn, repo, snapshotAt(5), "team-1", "svc-1")
			arrived := false
			hooked := &hookConn{Conn: conn, marker: "argMax(", hook: func() {
				arrived = true
				overlapSeed(t, ctx, conn, repo, snapshotAt(10), "team-1", "svc-1")
			}}
			if name == "finalizer" {
				// the finalizer has no carried read before its write path; the row arrives inside the membership read.
				hooked = &hookConn{Conn: conn, marker: "never matches", hook: func() {}}
				if _, err := retractStaleTeamKeysOutsideRun(ctx, hooked, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay,
					newStaleKeyScope(), func(context.Context) (staleKeyScope, error) {
						arrived = true
						overlapSeed(t, ctx, conn, repo, snapshotAt(10), "team-1", "svc-1")
						return newStaleKeyScope(), nil
					}, snapshotAt(10)); err != nil {
					t.Fatal(err)
				}
			} else {
				retire(hooked, repo)
				arrived = arrived && hooked.fired
			}
			if !arrived {
				t.Fatal("the late row never arrived between the scan and the write: nothing was tested")
			}
			var atNewest uint64
			if err := conn.QueryRow(ctx, `SELECT count() FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?
AND computed_at = (SELECT max(computed_at) FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?)`,
				snapshotOrg, repo, snapshotDay, snapshotOrg, repo, snapshotDay).Scan(&atNewest); err != nil {
				t.Fatal(err)
			}
			t.Logf("OVERLAP rows at the newest version = %d", atNewest)
			var winner uint64
			if err := conn.QueryRow(ctx, `SELECT toUInt64(argMax(retracted, computed_at)) FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`,
				snapshotOrg, repo, snapshotDay).Scan(&winner); err != nil {
				t.Fatal(err)
			}
			// The two production reader shapes, pinned as they answer today: the live-key read (GROUP BY key HAVING argMax(retracted,
			// computed_at) = 0, teamkeytables LiveHaving) and the registry newest-row read (ORDER BY computed_at DESC LIMIT 1 BY).
			var liveByHaving, newestRetracted uint64
			if err := conn.QueryRow(ctx, `SELECT toUInt64(count()) FROM (SELECT repo_id FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? GROUP BY repo_id HAVING `+teamkeytables.TestopsTestMetricsDaily.LiveHaving("")+`)`,
				snapshotOrg, repo, snapshotDay).Scan(&liveByHaving); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(ctx, `SELECT toUInt64(retracted) FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day`,
				snapshotOrg, repo, snapshotDay).Scan(&newestRetracted); err != nil {
				t.Fatal(err)
			}
			// FINAL is an extra line: no production reader of this table uses it.
			var finalWinner, finalRows uint64
			if err := conn.QueryRow(ctx, `SELECT toUInt64(max(retracted)), toUInt64(count()) FROM testops_test_metrics_daily FINAL
WHERE org_id = ? AND repo_id = ? AND day = ?`, snapshotOrg, repo, snapshotDay).Scan(&finalWinner, &finalRows); err != nil {
				t.Fatal(err)
			}
			t.Logf("KNOWN LIMIT (ticket 14016): tie behaviour today: argMax winner retracted=%d; HAVING live-key read live=%d; LIMIT 1 BY newest row retracted=%d; FINAL (extra, not a production shape) retracted=%d of %d row(s); winner not proven for every reader",
				winner, liveByHaving, newestRetracted, finalWinner, finalRows)
			if winner != 1 || liveByHaving != 0 || newestRetracted != 1 {
				t.Fatalf("the tie reads argMax retracted=%d, HAVING live=%d, LIMIT 1 BY retracted=%d; today's pinned answers are %d, %d, %d",
					winner, liveByHaving, newestRetracted, 1, 0, 1)
			}
			if atNewest != 2 {
				t.Fatalf("%d rows share the newest version of the key, the limit is the live row and the marker (2)", atNewest)
			}
		})
	}
}

// The values a marker carries come from the row it retires, not from a row that arrived between the scan and the write.
func TestAMarkerCarriesTheValuesOfTheRowItRetires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	repo := uuid.MustParse("13928000-0000-4000-8000-0000000000f4")
	overlapSeed(t, ctx, conn, repo, snapshotAt(5), "team-1", "svc-1")
	hooked := &hookConn{Conn: conn, marker: "argMax(", hook: func() { overlapSeed(t, ctx, conn, repo, snapshotAt(20), "team-2", "svc-2") }}
	scope := newStaleKeyScope([]string{repo.String()})
	if _, err := supersedeStaleTeamKeys(ctx, hooked, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay, scope, nil, snapshotAt(10)); err != nil {
		t.Fatal(err)
	}
	for _, line := range markerRows(t, ctx, conn, repo) {
		if !strings.HasSuffix(line, "|team-1|svc-1") {
			t.Fatalf("the marker %q carries the values of a row it does not retire, want team-1 / svc-1", line)
		}
	}
}

// The snapshot of a run is the instant its suite read happened. testops_risk reads the suites into the share, testops_test
// writes later with its own clock: the marker and the row carry the read instant, never the later clock.
func TestTheSnapshotOfARunIsTheInstantItsCachedSuiteReadHappened(t *testing.T) {
	f3Day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	f3Next := f3Day.Add(24 * time.Hour)
	e := f3Start(t)
	e.exec("SYSTEM STOP MERGES testops_test_metrics_daily")
	attempt1, attempt2 := f3Attempts(t)
	run2 := attemptsRunState{attempt: 1, started: "2026-07-22T15:01:00Z", updated: "2026-07-22T15:30:00Z", artifacts: []attemptsArtifact{
		{9503, "go-test-y", "2026-07-22T15:20:00Z", false,
			f3Junit(t, "pkg/y", "2026-07-22T15:02:00Z", f3Case("TestYA", "passed")+f3Case("TestYB", "passed")+f3Case("TestYC", "failed"))},
	}}
	at := func(day time.Time, h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	clock := func(second int) func() time.Time {
		return func() time.Time { return time.Date(2026, 8, 1, 0, 0, second, 0, time.UTC) }
	}
	partition := func(org string) (Run, Partition) {
		repo := e.stack.repoID(e.t, org)
		run := Run{ID: "run-" + uuid.NewString(), OrganizationID: org, TargetDay: f3Day}
		return run, Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	}
	// read is testops_risk of a pass: it reads the suites into the share of ctx at the clock of the run.
	read := func(ctx context.Context, org string, second int) {
		t.Helper()
		risk, err := NewTestopsRiskExecutor(e.stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		risk.nowUTC = clock(second)
		run, part := partition(org)
		if _, err := risk.ComputeFamily(ctx, run, part); err != nil {
			t.Fatal(err)
		}
	}
	// write is testops_test of the same pass, at the later clock of its own.
	write := func(ctx context.Context, org string, second int) {
		t.Helper()
		tests, err := NewTestopsTestExecutor(e.stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		tests.nowUTC = clock(second)
		run, part := partition(org)
		if _, err := tests.ComputeFamily(ctx, run, part); err != nil {
			t.Fatal(err)
		}
	}
	pass := func(ctx context.Context, org string, readAt, writeAt int) {
		read(ctx, org, readAt)
		write(ctx, org, writeAt)
	}
	want := func(t *testing.T, label string, got retiredStored, retracted uint8, newest time.Time) {
		t.Helper()
		if got.retracted != retracted || !got.newest.Equal(newest) {
			t.Errorf("%s: D stores %+v, want newest %s retracted %d", label, got, newest, retracted)
		}
	}
	newestAt := func(second int) time.Time { return clock(second)() }

	t.Run("an_empty_cached_read_stamps_the_read_not_the_later_write", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "snap-1-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		pass(withTestopsTestMetricShare(e.ctx), org, 5, 5)
		repo := e.stack.repoID(t, org)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		pass(withTestopsTestMetricShare(e.ctx), org, 10, 30)
		want(t, "older run, read at :10, write at :30", e.stored(org, repo, f3Day), 1, newestAt(10))
	})

	t.Run("a_newer_live_record_is_not_retired_by_an_older_cached_read", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "snap-2-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		pass(withTestopsTestMetricShare(e.ctx), org, 5, 5)
		repo := e.stack.repoID(t, org)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		older := withTestopsTestMetricShare(e.ctx)
		read(older, org, 10)
		e.stack.pass(t, org, f3Run2, at(f3Day, 15), run2)
		pass(withTestopsTestMetricShare(e.ctx), org, 20, 20)
		write(older, org, 30)
		want(t, "newer run's live row at :20 kept against the older read of :10", e.stored(org, repo, f3Day), 0, newestAt(20))
	})

	t.Run("a_newer_marker_is_not_resurrected_by_an_older_cached_read", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "snap-3-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		pass(withTestopsTestMetricShare(e.ctx), org, 5, 5)
		repo := e.stack.repoID(t, org)
		older := withTestopsTestMetricShare(e.ctx)
		read(older, org, 10)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		pass(withTestopsTestMetricShare(e.ctx), org, 20, 20)
		write(older, org, 30)
		want(t, "newer run's marker at :20 kept against the older live read of :10", e.stored(org, repo, f3Day), 1, newestAt(20))
	})
}

// retiredForEveryReader says whether the key reads as retired by the three reader shapes: the newest row of the key (ORDER BY
// computed_at DESC LIMIT 1 BY, as the registry readers take it), argMax by computed_at, and FINAL (an extra line: no production
// reader of this table uses FINAL).
func retiredForEveryReader(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID) {
	t.Helper()
	var newestRetracted, viaArgMax, viaFinal uint64
	if err := conn.QueryRow(ctx, `SELECT toUInt64(max(retracted)) FROM (SELECT * FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day)`,
		snapshotOrg, repo, snapshotDay).Scan(&newestRetracted); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT toUInt64(argMax(retracted, computed_at)) FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, snapshotOrg, repo, snapshotDay).Scan(&viaArgMax); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT toUInt64(max(retracted)) FROM testops_test_metrics_daily FINAL
WHERE org_id = ? AND repo_id = ? AND day = ?`, snapshotOrg, repo, snapshotDay).Scan(&viaFinal); err != nil {
		t.Fatal(err)
	}
	t.Logf("OVERLAP READERS newest-row retracted=%d argMax retracted=%d FINAL retracted=%d", newestRetracted, viaArgMax, viaFinal)
	if newestRetracted != 1 || viaArgMax != 1 || viaFinal != 1 {
		t.Fatalf("the retired row does not stay retired for every reader: newest-row retracted=%d argMax retracted=%d FINAL retracted=%d",
			newestRetracted, viaArgMax, viaFinal)
	}
}

// Do the readers answer the same with two markers on a key as with one? The readers take the newest row of each key
// (ORDER BY computed_at DESC LIMIT 1 BY, the shape of analytics dedupFromMeasureSource and the report/window sources) and then
// leave a marker key out, or take argMax by computed_at. A second marker of an older version sits under the newer one.
func TestTwoMarkersOnAKeyAnswerEveryReaderAsOneMarkerDoes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	one := uuid.MustParse("13928000-0000-4000-8000-0000000000f5")
	two := uuid.MustParse("13928000-0000-4000-8000-0000000000f6")
	marker := func(repo uuid.UUID, second int) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, team_id, service_id, computed_at, retracted)
VALUES (?, ?, ?, 'team-1', 'svc-1', ?, 1)`, snapshotOrg, repo, snapshotDay, snapshotAt(second).UTC()); err != nil {
			t.Fatal(err)
		}
	}
	for _, repo := range []uuid.UUID{one, two} {
		overlapSeed(t, ctx, conn, repo, snapshotAt(5), "team-1", "svc-1")
	}
	marker(one, 25)
	marker(two, 20)
	marker(two, 25)
	answer := func(repo uuid.UUID) string {
		t.Helper()
		var newestRows, liveRows uint64
		var newestRetracted uint64
		var total uint64
		if err := conn.QueryRow(ctx, `SELECT toUInt64(count()), toUInt64(countIf(retracted = 0)), toUInt64(max(retracted)), toUInt64(sum(total_cases)) FROM (
  SELECT * FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day)`,
			snapshotOrg, repo, snapshotDay).Scan(&newestRows, &liveRows, &newestRetracted, &total); err != nil {
			t.Fatal(err)
		}
		var viaArgMax uint64
		var viaTotal uint64
		if err := conn.QueryRow(ctx, `SELECT toUInt64(argMax(retracted, computed_at)), toUInt64(argMax(total_cases, computed_at)) FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, snapshotOrg, repo, snapshotDay).Scan(&viaArgMax, &viaTotal); err != nil {
			t.Fatal(err)
		}
		return strings.Join([]string{
			"newest-row rows=" + u64s(newestRows) + " live=" + u64s(liveRows) + " retracted=" + u64s(newestRetracted) + " total=" + u64s(total),
			"argMax retracted=" + u64s(viaArgMax) + " total=" + u64s(viaTotal),
		}, "; ")
	}
	a, b := answer(one), answer(two)
	t.Logf("READERS one marker: %s", a)
	t.Logf("READERS two markers: %s", b)
	if a != b {
		t.Fatalf("a key with two markers answers differently from one marker:\n one: %s\n two: %s", a, b)
	}
}

func u64s(v uint64) string { return strconv.FormatUint(v, 10) }
