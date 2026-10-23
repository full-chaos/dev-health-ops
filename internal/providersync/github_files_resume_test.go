package providersync

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
)

// segmentedFilesFake is a repository whose root listing is truncated (so the
// files route walks it) and whose walk ceiling is lowered to maxDirs, so the
// walk is read in several slices.
func segmentedFilesFake(t *testing.T, tree *walkTreeFake, maxDirs int) *filesChunkFake {
	t.Helper()
	tree.truncateAbove = 20
	fake := newFilesChunkFake(t, 0, 0, 0)
	fake.walk, fake.head = tree, "root"
	previous := gitHubFilesWalkLimits
	gitHubFilesWalkLimits = func() gitHubTreeWalkLimits {
		limits := rootListedWalkLimits()
		limits.maxDirs = maxDirs
		return limits
	}
	t.Cleanup(func() { gitHubFilesWalkLimits = previous })
	return fake
}

func rowPaths(rows []gitFileRow) []string {
	paths := make([]string, 0, len(rows))
	for _, row := range rows {
		paths = append(paths, row.Path)
	}
	sort.Strings(paths)
	return paths
}

func decodeEmissionCursor(t *testing.T, raw string) githubFilesChunkCursor {
	t.Helper()
	cursor, err := decodeGitHubFilesChunkCursor(raw)
	if err != nil {
		t.Fatalf("cursor %q: %v", raw, err)
	}
	return cursor
}

func TestGitHubFilesUnitReadsAWalkInSlicesAndWritesEveryPathOnceAcrossAttempts(t *testing.T) {
	for _, stopAfter := range []int{0, 1, 2} {
		tree := randomSliceTree(7, 120)
		want := tree.blobPaths("root")
		fake := segmentedFilesFake(t, tree, 12)
		var rows []gitFileRow
		resume, before, attempts := "", "", 0
		var final ChunkRouteEmission
		for {
			attempts++
			if attempts > 400 {
				t.Fatal("the unit does not complete")
			}
			run, err := runFilesChunks(t, fake, resume, stopAfter)
			for _, emission := range run.emissions {
				if emission.CursorBefore != before && !(before == "" && emission.CursorBefore == "") {
					t.Fatalf("cursor chain broken: before=%q want %q", emission.CursorBefore, before)
				}
				if len(emission.CursorAfter) > maxChunkCursorBytes {
					t.Fatalf("cursor of %d bytes", len(emission.CursorAfter))
				}
				before = emission.CursorAfter
				if emission.Final {
					final = emission
				}
			}
			rows = append(rows, run.rows...)
			if err == nil {
				break
			}
			if !errors.As(err, new(ChunkContinuationError)) {
				t.Fatalf("stopAfter=%d attempt %d: %v", stopAfter, attempts, err)
			}
			resume = run.emissions[len(run.emissions)-1].CursorAfter
		}
		got := rowPaths(rows)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("stopAfter=%d: %d rows, want the %d blobs of the tree once each", stopAfter, len(got), len(want))
		}
		cursor := decodeEmissionCursor(t, final.CursorAfter)
		if cursor.Segments < 2 || !cursor.Walked || cursor.PathsTotal != len(want) {
			t.Fatalf("final cursor=%+v, want several slices and %d paths", cursor, len(want))
		}
		if final.Batch.Result["paths_total"] != len(want) || final.Batch.Result["files_synced"] != len(want) {
			t.Fatalf("result=%v", final.Batch.Result)
		}
		stamp := rows[0].LastSynced
		for _, row := range rows {
			if !row.LastSynced.Equal(stamp) {
				t.Fatalf("%s written at %s, not the unit's one instant %s", row.Path, row.LastSynced, stamp)
			}
		}
	}
}

func TestGitHubFilesUnitCursorFixesTheSliceSoALaterAttemptReadsTheSameEndUnderOtherCeilings(t *testing.T) {
	tree := randomSliceTree(11, 120)
	want := tree.blobPaths("root")
	fake := segmentedFilesFake(t, tree, 12)
	first, err := runFilesChunks(t, fake, "", 1)
	if !errors.As(err, new(ChunkContinuationError)) || len(first.emissions) != 1 {
		t.Fatalf("err=%v emissions=%d", err, len(first.emissions))
	}
	resume := first.emissions[0].CursorAfter
	cursor := decodeEmissionCursor(t, resume)
	if cursor.Segments == 0 && cursor.WalkNext == "" {
		t.Fatalf("a chunk of an unfinished slice must carry the slice's end: %+v", cursor)
	}
	gitHubFilesWalkLimits = func() gitHubTreeWalkLimits { return rootListedWalkLimits() }
	var rows []gitFileRow
	rows = append(rows, first.rows...)
	for attempts := 0; ; attempts++ {
		if attempts > 400 {
			t.Fatal("the unit does not complete")
		}
		run, err := runFilesChunks(t, fake, resume, 1)
		rows = append(rows, run.rows...)
		if err == nil {
			break
		}
		if !errors.As(err, new(ChunkContinuationError)) {
			t.Fatal(err)
		}
		resume = run.emissions[len(run.emissions)-1].CursorAfter
	}
	if strings.Join(rowPaths(rows), "\n") != strings.Join(want, "\n") {
		t.Fatalf("%d rows, want the %d blobs once each: a looser ceiling must not move the fixed slice end", len(rows), len(want))
	}
}

func TestGitHubFilesUnitStopsWithTheCeilingWhenNoSliceCanPassItsStart(t *testing.T) {
	fake := segmentedFilesFake(t, walkWide(60), 1)
	run, err := runFilesChunks(t, fake, "", 0)
	var limit *GitHubTreeWalkLimitError
	if !errors.As(err, &limit) || limit.Limit != GitHubTreeWalkLimitDirs || !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("err=%v, want the dirs ceiling, as before slicing", err)
	}
	if len(run.rows) != 0 {
		t.Fatalf("%d rows written before the stop", len(run.rows))
	}
}

func TestGitHubFilesUnitStopsAtTheSliceCapWithTheCeilingError(t *testing.T) {
	fake := segmentedFilesFake(t, walkWide(400), 4)
	_, err := runFilesChunks(t, fake, "", 0)
	var limit *GitHubTreeWalkLimitError
	if !errors.As(err, &limit) || limit.Limit != GitHubTreeWalkLimitDirs {
		t.Fatalf("err=%v, want the dirs ceiling", err)
	}
}

func TestGitHubFilesCursorNewFieldsAreCheckedAndOldCursorsStillDecode(t *testing.T) {
	if _, err := decodeGitHubFilesChunkCursor(`{"v":1,"tree":"t","next":3,"rows":3,"contents":0,"oversize":0,"unavailable":0,"paths_total":9,"eligible_total":2,"requests":4,"chunks":1}`); err != nil {
		t.Fatalf("a cursor without slice fields: %v", err)
	}
	long := strings.Repeat("a", gitHubWalkFrontierMaxBytes+1)
	for name, raw := range map[string]string{
		"next without known":    `{"v":1,"tree":"t","walk_after":"a/","walk_next":"b/"}`,
		"next not after after":  `{"v":1,"tree":"t","walk_after":"b/","walk_next":"a/","walk_known":true}`,
		"slice cap reached":     `{"v":1,"tree":"t","segments":20}`,
		"negative paths done":   `{"v":1,"tree":"t","paths_done":-1}`,
		"frontier above bound":  `{"v":1,"tree":"t","walk_after":"` + long + `"}`,
		"unknown field":         `{"v":1,"tree":"t","walk_other":"a"}`,
		"negative eligibledone": `{"v":1,"tree":"t","eligible_done":-1}`,
	} {
		if _, err := decodeGitHubFilesChunkCursor(raw); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s: err=%v, want ErrInvalidConfiguration", name, err)
		}
	}
	cursor := githubFilesChunkCursor{
		Version: 1, TreeRef: "t", WalkAfter: strings.Repeat("<", 160), WalkNext: strings.Repeat("&", 160), WalkKnown: true,
		Segments: 19, PathsDone: 1 << 20, EligibleDone: 1 << 20, Rows: 1 << 20, PathsTotal: 1 << 20,
	}
	if _, err := cursor.encode(); err != nil {
		t.Fatalf("two escape-heavy frontiers must fit the cursor: %v", err)
	}
	var back githubFilesChunkCursor
	encoded, _ := cursor.encode()
	if err := json.Unmarshal([]byte(encoded), &back); err != nil || back.WalkNext != cursor.WalkNext {
		t.Fatalf("round trip: %v", err)
	}
}

func TestGitHubFilesUnitRestartsTheSlicedWalkWhenThePinnedTreeIsGone(t *testing.T) {
	oldTree := randomSliceTree(21, 120)
	fake := segmentedFilesFake(t, oldTree, 12)
	first, err := runFilesChunks(t, fake, "", 3)
	if !errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("err=%v", err)
	}
	resume := first.emissions[len(first.emissions)-1].CursorAfter
	if cursor := decodeEmissionCursor(t, resume); cursor.Segments == 0 && cursor.WalkAfter == "" {
		t.Fatalf("premise: the cursor must be past the first slice: %+v", cursor)
	}
	newTree := randomSliceTreeNamed(22, 120, "u", "root2")
	for sha, entries := range newTree.trees {
		fake.walk.trees[sha] = entries
	}
	delete(fake.walk.trees, "root")
	fake.head = "root2"
	var rows []gitFileRow
	var final ChunkRouteEmission
	for attempts := 0; ; attempts++ {
		if attempts > 400 {
			t.Fatal("the unit does not complete")
		}
		run, err := runFilesChunks(t, fake, resume, 2)
		rows = append(rows, run.rows...)
		if err == nil {
			final = run.emissions[len(run.emissions)-1]
			break
		}
		if !errors.As(err, new(ChunkContinuationError)) {
			t.Fatal(err)
		}
		resume = run.emissions[len(run.emissions)-1].CursorAfter
	}
	want := newTree.blobPaths("root2")
	if strings.Join(rowPaths(rows), "\n") != strings.Join(want, "\n") {
		t.Fatalf("%d rows, want the %d blobs of the new head once each", len(rows), len(want))
	}
	if final.Batch.Result["pin_lost"] != 1 || final.Batch.Result["paths_total"] != len(want) {
		t.Fatalf("result=%v", final.Batch.Result)
	}
}

func TestGitHubFilesUnitSliceCapAllowsExactlyTwentySlices(t *testing.T) {
	for _, tc := range []struct {
		name     string
		children int
		wantRows int
		wantErr  bool
	}{
		{"twenty slices complete", 20, 20, false},
		{"twenty-one slices stop with the ceiling", 21, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := segmentedFilesFake(t, walkWide(tc.children+1), 2)
			var rows []gitFileRow
			resume := ""
			var err error
			for attempts := 0; attempts < 100; attempts++ {
				var run filesChunkRun
				run, err = runFilesChunks(t, fake, resume, 1)
				rows = append(rows, run.rows...)
				if err == nil || !errors.As(err, new(ChunkContinuationError)) {
					break
				}
				resume = run.emissions[len(run.emissions)-1].CursorAfter
			}
			var limit *GitHubTreeWalkLimitError
			if tc.wantErr != errors.As(err, &limit) || (tc.wantErr && limit.Limit != GitHubTreeWalkLimitDirs) {
				t.Fatalf("err=%v", err)
			}
			if !tc.wantErr && len(rows) != tc.wantRows {
				t.Fatalf("rows=%d, want %d", len(rows), tc.wantRows)
			}
		})
	}
}
