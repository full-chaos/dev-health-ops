package providersync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9704: the prod dry run of compact-ownership-intervals spun at 1 CPU for
// over 200 s on one organization's team_repo_ownership, while the rehearsal at
// the same row count took 5 s. The rows below are SYNTHETIC, built from the
// counts and shapes GWC read on prod (no prod data): team_repo_ownership
// inferred 44,367 rows (open 32, closed 44,312, tombstones 23) over 42 keys,
// median 1 row, max 4,432 (about 10 keys near 4,400), 2 rows of other
// sources; team_project_ownership native 67,462 rows (open 36, closed 67,426)
// over 94 keys, max 6,169, p99 about 3,500, 7 rows of other sources. The
// rehearsal fixture had chains of 11 rows; the planner was quadratic in the
// rows of one key.

const (
	prodShapeInferredClosed = 44312
	prodShapeInferredOpen   = 32
	prodShapeInferredTombs  = 23
	prodShapeNativeClosed   = 67426
	prodShapeNativeOpen     = 36
)

// prodShapedRepoRows builds one organization's team_repo_ownership rows. scale
// shrinks the ten deep keys (1 = prod size); the shallow keys, the open rows
// and the tombstones stay.
func prodShapedRepoRows(seed int64, scale float64) []ownershipIntervalRow {
	rng := rand.New(rand.NewSource(seed))
	epoch := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	type key struct {
		repo string
		id   uuid.UUID
		team string
		rank uint8
	}
	// Ten deep keys: five repositories with two co-owning teams each, whose
	// share drifts. 32 shallow keys: one team each, an open row, ten of them
	// with three closed rows before it.
	var keys []key
	for repo := 0; repo < 37; repo++ {
		name := fmt.Sprintf("org2/repo-%02d", repo)
		id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(name))
		teams := 1
		if repo < 5 {
			teams = 2
		}
		for team := 0; team < teams; team++ {
			keys = append(keys, key{repo: name, id: id, team: fmt.Sprintf("gh:team-%02d", (repo+team*7)%19), rank: uint8(team + 1)})
		}
	}
	quotas := make([]int, len(keys))
	deep := prodShapeInferredClosed - 10*3
	// The deepest key holds 4,432 rows with its three tombstones.
	quotas[0] = 4429
	for index := 1; index < 10; index++ {
		quotas[index] = (deep - 4429) / 9
	}
	for index := 1; index <= (deep-4429)%9; index++ {
		quotas[index]++
	}
	for index := 0; index < 10; index++ {
		quotas[index] = int(math.Round(float64(quotas[index]) * scale))
		quotas[10+index] = 3
	}
	shares := []float64{1, 0.5, 0.75, 0.25, 0.6, 0.4}
	var rows []ownershipIntervalRow
	var deepRows [][]int
	for index, k := range keys {
		id := k.id
		base := ownershipIntervalRow{
			Provider: "github", Natural: k.repo, TeamID: k.team, Source: "inferred", RepoID: &id, MatchType: "exact",
			IsPrimary: k.rank, Specificity: 100, Priority: 10,
		}
		at := epoch.Add(time.Duration(rng.Intn(24*60)) * time.Minute)
		share := shares[rng.Intn(len(shares))]
		var mine []int
		for n := 0; n < quotas[index]; n++ {
			if rng.Float64() < 0.3 {
				share = shares[rng.Intn(len(shares))]
			}
			row := base
			s := share
			row.Share = &s
			row.ValidFrom = at
			end := at.Add(time.Duration(30+rng.Intn(150)) * time.Minute)
			row.ValidTo = &end
			row.UpdatedAt = end.Add(time.Duration(rng.Intn(900)) * time.Millisecond)
			mine = append(mine, len(rows))
			rows = append(rows, row)
			switch draw := rng.Float64(); {
			case draw < 0.003:
				at = end.Add(time.Hour)
			case draw < 0.0035:
				at = end.Add(-10 * time.Minute)
			default:
				at = end
			}
		}
		if index >= 10 {
			row := base
			s := share
			row.Share = &s
			row.ValidFrom = at
			row.UpdatedAt = at.Add(time.Duration(rng.Intn(900)) * time.Millisecond)
			rows = append(rows, row)
		} else {
			deepRows = append(deepRows, mine)
		}
	}
	// Two rows of another source inside deep keys, newer than the inferred rows
	// before them and older than the ones after.
	for index := 0; index < 2; index++ {
		mine := deepRows[index]
		row := rows[mine[len(mine)/2]]
		row.Source = "provider_access"
		row.Share = nil
		row.IsPrimary = 2
		end := row.ValidFrom.Add(200 * time.Hour)
		row.ValidTo = &end
		row.UpdatedAt = row.ValidFrom.Add(100 * time.Hour)
		rows = append(rows, row)
	}
	// Supersede tombstones in the deep keys: anchors before the first row, ones
	// inside the history, and some holding the key's newest stamp.
	for n := 0; n < prodShapeInferredTombs; n++ {
		mine := deepRows[n%10]
		var row ownershipIntervalRow
		if n < 10 && n%4 == 0 {
			row = rows[mine[0]]
			row.ValidFrom = row.ValidFrom.Add(-time.Hour)
		} else {
			row = rows[mine[rng.Intn(len(mine))]]
			row.ValidFrom = row.ValidFrom.Add(17*time.Minute + time.Duration(n)*time.Millisecond)
		}
		from := row.ValidFrom
		row.ValidTo = &from
		row.UpdatedAt = from.Add(time.Millisecond)
		if n%11 == 5 {
			row.UpdatedAt = epoch.Add(5000 * time.Hour)
		}
		rows = append(rows, row)
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	return rows
}

// prodShapedProjectRows builds one organization's team_project_ownership rows:
// 94 keys, one of 6,169 rows, the next ones near 3,500, a long tail; open 36,
// no tombstone, 7 rows of another source; the priority changes rarely.
func prodShapedProjectRows(seed int64) []ownershipIntervalRow {
	rng := rand.New(rand.NewSource(seed))
	epoch := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	const keys = 94
	quotas := make([]int, keys)
	quotas[0] = 6168 // 6,169 rows with its open row
	rest := prodShapeNativeClosed - quotas[0]
	weights := make([]float64, keys)
	total := 0.0
	for index := 1; index < keys; index++ {
		weights[index] = 1 / math.Pow(float64(index), 0.6)
		total += weights[index]
	}
	assigned := quotas[0]
	for index := 1; index < keys; index++ {
		quotas[index] = min(3515, int(float64(rest)*weights[index]/total))
		assigned += quotas[index]
	}
	for index := keys - 1; assigned < prodShapeNativeClosed; index-- {
		if index == 0 {
			index = keys - 1
		}
		if quotas[index] < 3515 {
			quotas[index]++
			assigned++
		}
	}
	var rows []ownershipIntervalRow
	for index := 0; index < keys; index++ {
		projectKey := fmt.Sprintf("P%02d", index)
		base := ownershipIntervalRow{
			Provider: "linear", Natural: fmt.Sprintf("proj-%02d", index), TeamID: fmt.Sprintf("linear:team-%02d", index%23),
			Source: "native", ProjectKey: &projectKey, IsPrimary: 1, Specificity: 100, Priority: 10,
		}
		at := epoch.Add(time.Duration(rng.Intn(24*60)) * time.Minute)
		for n := 0; n < quotas[index]; n++ {
			if rng.Float64() < 0.01 {
				base.Priority = int32(10 + 10*rng.Intn(3))
			}
			row := base
			row.ValidFrom = at
			end := at.Add(time.Duration(30+rng.Intn(150)) * time.Minute)
			row.ValidTo = &end
			row.UpdatedAt = end
			row.LastSynced = end
			rows = append(rows, row)
			at = end
		}
		if index < prodShapeNativeOpen {
			row := base
			row.ValidFrom, row.UpdatedAt, row.LastSynced = at, at, at
			rows = append(rows, row)
		}
		if index < 7 {
			row := base
			row.Source = "jira_legacy"
			row.ValidFrom = epoch.Add(-48 * time.Hour)
			end := epoch.Add(-24 * time.Hour)
			row.ValidTo = &end
			row.UpdatedAt, row.LastSynced = end, end
			rows = append(rows, row)
		}
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	return rows
}

// keyDepths is the number of rows per key of one source, largest first.
func keyDepths(rows []ownershipIntervalRow, source string) []int {
	byKey := map[string]int{}
	for _, row := range rows {
		if row.Source == source {
			byKey[row.keyOf()]++
		}
	}
	depths := make([]int, 0, len(byKey))
	for _, n := range byKey {
		depths = append(depths, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(depths)))
	return depths
}

func TestProdShapedRowsHaveTheProdCounts(t *testing.T) {
	repo := prodShapedRepoRows(9704, 1)
	total, open, closed, tombstones, reversed := countClasses(repo, "inferred")
	if total != 44367 || open != 32 || closed != 44312 || tombstones != 23 || reversed != 0 || len(repo)-total != 2 {
		t.Fatalf("repo inferred total %d open %d closed %d tombstones %d close-before-open %d other %d; want 44367/32/44312/23/0/2",
			total, open, closed, tombstones, reversed, len(repo)-total)
	}
	depths := keyDepths(repo, "inferred")
	if len(depths) != 42 || depths[0] != 4432 || depths[9] < 4400 || depths[len(depths)/2] != 1 {
		t.Fatalf("repo key depths: keys %d, deepest %v, median %d", len(depths), depths[:10], depths[len(depths)/2])
	}
	project := prodShapedProjectRows(9704)
	total, open, closed, tombstones, reversed = countClasses(project, "native")
	if total != 67462 || open != 36 || closed != 67426 || tombstones != 0 || reversed != 0 || len(project)-total != 7 {
		t.Fatalf("project native total %d open %d closed %d tombstones %d close-before-open %d other %d; want 67462/36/67426/0/0/7",
			total, open, closed, tombstones, reversed, len(project)-total)
	}
	depths = keyDepths(project, "native")
	if len(depths) != 94 || depths[0] != 6169 || depths[1] > 3516 {
		t.Fatalf("project key depths: keys %d, deepest %v", len(depths), depths[:3])
	}
}

// compactionBound is the time a plan plus differential may take at prod scale.
// The new code takes about a second; the 9556 code did not finish in minutes.
func compactionBound() time.Duration {
	if raw := os.Getenv("OWNERSHIP_COMPACTION_BOUND_SECONDS"); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 60 * time.Second
}

// TestCompactionPlansTheProdShapedTablesInBoundedTime fails on the 9556
// planner: it calls only functions that exist there, so the same file run on
// f850e8f9 times out at the bound.
func TestCompactionPlansTheProdShapedTablesInBoundedTime(t *testing.T) {
	for _, shape := range []struct {
		name string
		spec ownershipCompactionSpec
		rows []ownershipIntervalRow
	}{
		{"team_repo_ownership", repoSpec(), prodShapedRepoRows(9704, 1)},
		{"team_project_ownership", projectSpec(), prodShapedProjectRows(9704)},
	} {
		type result struct {
			plan ownershipCompactionPlan
			diff OwnershipCompactionDifferential
		}
		done := make(chan result, 1)
		started := time.Now()
		go func() {
			plan := planOwnershipCompaction(shape.spec, shape.rows)
			done <- result{plan: plan, diff: ownershipDifferential(shape.rows, plan.final(shape.rows))}
		}()
		bound := compactionBound()
		select {
		case got := <-done:
			elapsed := time.Since(started)
			t.Logf("%s prod shape: rows %d, runs found %d merged %d kept multi-source %d, rows merged %d, tombstones dropped %d, differential groups %d instants %d mismatches %d, elapsed %s",
				shape.name, len(shape.rows), got.plan.org.RunsFound, got.plan.org.RunsMerged, got.plan.org.RunsKeptMultiSource, got.plan.org.RowsMerged,
				got.plan.org.TombstonesDropped, got.diff.GroupsChecked, got.diff.InstantsChecked, got.diff.Mismatches, elapsed)
			if got.diff.Mismatches != 0 || got.plan.org.RunsMerged == 0 {
				t.Fatalf("%s: plan %+v differential %+v", shape.name, got.plan.org, got.diff)
			}
		case <-time.After(bound):
			t.Fatalf("%s: plan + differential of the prod-shaped organization (%d rows) did not finish within %s", shape.name, len(shape.rows), bound)
		}
	}
}

func TestCompactionPlanStopsAtItsDeadline(t *testing.T) {
	rows := prodShapedRepoRows(9704, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := planOwnershipCompactionWithin(newCompactionMeter(ctx, nil, 0), repoSpec(), rows)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("plan under a cancelled context: %v", err)
	}
	if _, err := ownershipDifferentialWithin(newCompactionMeter(ctx, nil, 0), rows, rows[1:]); !errors.Is(err, context.Canceled) {
		t.Fatalf("differential under a cancelled context: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("a cancelled plan took %s", elapsed)
	}
}

func TestCompactionMeterReportsCountsEveryNRows(t *testing.T) {
	rows := prodShapedRepoRows(9704, 0.1)
	var lines []OwnershipCompactionProgress
	meter := newCompactionMeter(context.Background(), func(line OwnershipCompactionProgress) { lines = append(lines, line) }, 1000)
	plan, err := planOwnershipCompactionWithin(meter, repoSpec(), rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ownershipDifferentialWithin(meter, rows, plan.final(rows)); err != nil {
		t.Fatal(err)
	}
	last := map[string]OwnershipCompactionProgress{}
	for _, line := range lines {
		previous, seen := last[line.Phase]
		if seen && line.Done < previous.Done {
			t.Fatalf("progress went back: %+v after %+v", line, previous)
		}
		if line.Done > line.Total {
			t.Fatalf("done above total: %+v", line)
		}
		last[line.Phase] = line
	}
	for _, phase := range []string{"plan", "runs", "differential"} {
		line, ok := last[phase]
		if !ok || line.Done != line.Total || line.Total == 0 {
			t.Fatalf("phase %s: last line %+v", phase, line)
		}
	}
	if len(lines) < 10 {
		t.Fatalf("only %d progress lines for %d rows at every 1000", len(lines), len(rows))
	}
}

// randomOwnershipRows is a small, edge-dense group set: ties in updated_at and
// valid_from, 1 ms intervals, overlaps, gaps, close-before-open rows,
// tombstones, open rows and several sources on one key.
func randomOwnershipRows(rng *rand.Rand) []ownershipIntervalRow {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := func(n int) time.Time { return epoch.Add(time.Duration(n) * time.Millisecond) }
	sources := []string{"inferred", "inferred", "inferred", "provider_access", "manual"}
	shares := []float64{1, 0.5}
	var rows []ownershipIntervalRow
	seen := map[string]bool{}
	for repo := 0; repo < 1+rng.Intn(3); repo++ {
		for team := 0; team < 1+rng.Intn(3); team++ {
			for _, source := range sources[:1+rng.Intn(len(sources))] {
				at := rng.Intn(5)
				for n := 0; n < rng.Intn(14); n++ {
					row := ownershipIntervalRow{
						Provider: "github", Natural: "r" + strconv.Itoa(repo), TeamID: "t" + strconv.Itoa(team), Source: source,
						MatchType: "exact", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: ms(at),
					}
					if rng.Intn(4) > 0 {
						s := shares[rng.Intn(len(shares))]
						if rng.Intn(3) > 0 {
							s = 1
						}
						row.Share = &s
					}
					length := 1 + rng.Intn(4)
					switch draw := rng.Intn(20); {
					case draw == 0:
						length = 0 // tombstone
					case draw == 1:
						length = -1 - rng.Intn(2) // close before open
					}
					end := ms(at + length)
					row.ValidTo = &end
					if rng.Intn(12) == 0 {
						row.ValidTo = nil
					}
					row.UpdatedAt = ms(at + rng.Intn(8))
					if !seen[row.sortKey()] {
						seen[row.sortKey()] = true
						rows = append(rows, row)
					}
					switch draw := rng.Intn(10); {
					case draw == 0:
						at += length + 1 + rng.Intn(2)
					case draw == 1 && length > 1:
						at += length - 1
					default:
						at += max(length, 1)
					}
				}
			}
		}
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	return rows
}

// plantedChange moves one boundary, one share or one row of a final state: a
// post set the differential must judge the same way in both planners.
func plantedChange(rng *rand.Rand, rows []ownershipIntervalRow) []ownershipIntervalRow {
	out := append([]ownershipIntervalRow(nil), rows...)
	if len(out) == 0 {
		return out
	}
	index := rng.Intn(len(out))
	switch rng.Intn(4) {
	case 0:
		out = append(out[:index], out[index+1:]...)
	case 1:
		if out[index].ValidTo != nil {
			end := out[index].ValidTo.Add(time.Duration(1+rng.Intn(3)) * time.Millisecond)
			out[index].ValidTo = &end
		}
	case 2:
		s := 0.33
		out[index].Share = &s
	default:
		out[index].UpdatedAt = out[index].UpdatedAt.Add(time.Duration(rng.Intn(5)-2) * time.Millisecond)
	}
	return out
}

func samePlan(t *testing.T, label string, got, want ownershipCompactionPlan) {
	t.Helper()
	if got.org != want.org {
		t.Fatalf("%s: counts\n got %+v\nwant %+v", label, got.org, want.org)
	}
	if len(got.runs) != len(want.runs) || len(got.drops) != len(want.drops) {
		t.Fatalf("%s: runs %d/%d drops %d/%d", label, len(got.runs), len(want.runs), len(got.drops), len(want.drops))
	}
	for index := range want.runs {
		g, w := got.runs[index], want.runs[index]
		if rowSignature(g.merged) != rowSignature(w.merged) || len(g.rows) != len(w.rows) {
			t.Fatalf("%s: run %d differs", label, index)
		}
		for row := range w.rows {
			if rowSignature(g.rows[row]) != rowSignature(w.rows[row]) {
				t.Fatalf("%s: run %d row %d differs", label, index, row)
			}
		}
	}
	for index := range want.drops {
		if rowSignature(got.drops[index]) != rowSignature(want.drops[index]) {
			t.Fatalf("%s: drop %d differs", label, index)
		}
	}
}

func compareWithLegacy(t *testing.T, label string, spec ownershipCompactionSpec, rows []ownershipIntervalRow, rng *rand.Rand) {
	t.Helper()
	got := planOwnershipCompaction(spec, rows)
	want := legacyPlanOwnershipCompaction(spec, rows)
	samePlan(t, label, got, want)
	final := want.final(rows)
	sets := [][]ownershipIntervalRow{final, rows}
	for n := 0; n < 4; n++ {
		sets = append(sets, plantedChange(rng, final))
	}
	for index, post := range sets {
		if g, w := ownershipDifferential(rows, post), legacyOwnershipDifferential(rows, post); g != w {
			t.Fatalf("%s: differential %d\n got %+v\nwant %+v", label, index, g, w)
		}
	}
}

// TestCompactionPlannerEqualsThe9556Planner proves the new planner and
// differential give the SAME plan and the SAME counts as the 9556 code (kept
// in ownership_interval_compaction_legacy_test.go) wherever the old code
// finishes: edge-dense random sets, the shapes of the unit tests, and the
// prod-shaped organization at reduced scale.
func TestCompactionPlannerEqualsThe9556Planner(t *testing.T) {
	rng := rand.New(rand.NewSource(97040))
	mismatchesSeen, multiSourceSeen := 0, 0
	for seed := 0; seed < 500; seed++ {
		rows := randomOwnershipRows(rng)
		compareWithLegacy(t, "random seed "+strconv.Itoa(seed), repoSpec(), rows, rng)
		plan := legacyPlanOwnershipCompaction(repoSpec(), rows)
		multiSourceSeen += plan.org.RunsKeptMultiSource
		mismatchesSeen += legacyOwnershipDifferential(rows, plantedChange(rng, plan.final(rows))).Mismatches
	}
	if multiSourceSeen == 0 || mismatchesSeen == 0 {
		t.Fatalf("the random sets never hit a multi-source run (%d) or a mismatch (%d)", multiSourceSeen, mismatchesSeen)
	}
	unitShapes := map[string][]ownershipIntervalRow{
		"winner flips": {
			repoRow("a", "acme/api", 0, 10, 1), repoRow("a", "acme/api", 10, 20, 100),
			withSource(withShare(repoRow("a", "acme/api", 5, 15, 50), 0.25), "manual", 50),
		},
		"tombstones": {
			repoRow("a", "acme/api", 5, 5, 5), repoRow("a", "acme/api", 20, -1, 20),
			repoRow("b", "acme/api", 0, -1, 1), repoRow("b", "acme/api", 7, 7, 1),
		},
		"share change, gap, overlap": {
			repoRow("a", "acme/api", 0, 10, 10), repoRow("a", "acme/api", 10, 20, 20),
			withShare(repoRow("a", "acme/api", 20, 30, 30), 0.5), withShare(repoRow("a", "acme/api", 30, 40, 40), 0.5),
			repoRow("a", "acme/api", 41, 50, 50), repoRow("b", "acme/api", 0, 10, 10), repoRow("b", "acme/api", 9, 20, 20),
		},
	}
	for name, rows := range unitShapes {
		compareWithLegacy(t, name, repoSpec(), rows, rng)
	}
	for seed := int64(1); seed <= 3; seed++ {
		compareWithLegacy(t, "prod shape 1.5% seed "+strconv.FormatInt(seed, 10), repoSpec(), prodShapedRepoRows(seed, 0.015), rng)
	}
}

// TestCompactionLegacyPlannerAtProdShape is the timing evidence of the 9556
// code on the prod shape; it runs only on request (it does not finish).
func TestCompactionLegacyPlannerAtProdShape(t *testing.T) {
	if os.Getenv("OWNERSHIP_COMPACTION_LEGACY_TIMING") == "" {
		t.Skip("timing evidence only: set OWNERSHIP_COMPACTION_LEGACY_TIMING=1")
	}
	scale := 1.0
	if raw := os.Getenv("OWNERSHIP_COMPACTION_LEGACY_SCALE"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatal(err)
		}
		scale = parsed
	}
	rows := prodShapedRepoRows(9704, scale)
	done := make(chan time.Duration, 2)
	go func() {
		started := time.Now()
		plan := legacyPlanOwnershipCompaction(repoSpec(), rows)
		planned := time.Since(started)
		t.Logf("legacy plan: %s", planned)
		legacyOwnershipDifferential(rows, plan.final(rows))
		done <- time.Since(started)
	}()
	bound := compactionBound()
	select {
	case elapsed := <-done:
		t.Logf("legacy plan + differential at scale %.2f (%d rows): %s", scale, len(rows), elapsed)
	case <-time.After(bound):
		t.Fatalf("legacy plan + differential at scale %.2f (%d rows) did not finish within %s", scale, len(rows), bound)
	}
}
