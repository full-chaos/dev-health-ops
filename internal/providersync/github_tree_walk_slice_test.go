package providersync

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

var sliceNamePool = []string{"a.b", "a-b", "a0", "a_b", "a", "b.go", "B.go", "x", ".hidden", "z", "a+", "m", "m.go", "mm"}

// randomSliceTree builds a tree of about `dirs` directories whose entry names
// collide in every way a directory sorts differently from a file ("a" against
// "a.b", "a-b", "a0"); each listing is shuffled, so only the walk's own order
// can put it right.
func randomSliceTree(seed int64, dirs int) *walkTreeFake {
	return randomSliceTreeNamed(seed, dirs, "t", "root")
}

func randomSliceTreeNamed(seed int64, dirs int, shaPrefix, rootName string) *walkTreeFake {
	rng := rand.New(rand.NewSource(seed))
	fake := &walkTreeFake{trees: map[string][]walkEntry{}}
	counter := 0
	var build func(depth int) string
	build = func(depth int) string {
		counter++
		sha := fmt.Sprintf("%s%d", shaPrefix, counter)
		var entries []walkEntry
		used := map[string]bool{}
		for _, name := range sliceNamePool {
			if rng.Intn(3) == 0 {
				continue
			}
			used[name] = true
			if depth < 4 && counter < dirs && rng.Intn(3) == 0 {
				entries = append(entries, walkEntry{path: name, kind: "tree", sha: build(depth + 1)})
			} else {
				entries = append(entries, walkEntry{path: name, kind: "blob"})
			}
		}
		rng.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
		fake.trees[sha] = entries
		return sha
	}
	root := build(0)
	fake.trees[rootName] = fake.trees[root]
	return fake
}

func sliceLimits(maxDirs int, recursive bool) gitHubTreeWalkLimits {
	limits := rootListedWalkLimits()
	limits.recursiveSubtrees = recursive
	limits.maxDirs = maxDirs
	return limits
}

func sliceWalk(t *testing.T, fake *walkTreeFake, limits gitHubTreeWalkLimits) (gitHubWalkSlice, error) {
	t.Helper()
	return gitHubWalkTreeSlice(context.Background(), fake.client(t), "/repos/acme/api", "root", limits)
}

func singlePass(t *testing.T, fake *walkTreeFake) []string {
	t.Helper()
	paths, _, err := fake.walk(t, serialWalkLimits())
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	return paths
}

func TestGitHubWalkSlicesJoinToTheSinglePassWalkWithNoGapAndNoRepeat(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		for _, recursive := range []bool{false, true} {
			for _, maxDirs := range []int{8, 15, 40} {
				fake := randomSliceTree(seed, 120)
				want := singlePass(t, fake)
				limits := sliceLimits(maxDirs, recursive)
				var joined []string
				after, slices := "", 0
				for {
					limits.after = after
					before := fake.gets
					result, err := sliceWalk(t, fake, limits)
					if err != nil {
						t.Fatalf("seed=%d recursive=%v max=%d after=%q: %v", seed, recursive, maxDirs, after, err)
					}
					if result.Stats.dirs > maxDirs {
						t.Fatalf("slice read %d dirs, ceiling %d", result.Stats.dirs, maxDirs)
					}
					if fake.gets-before > 2*maxDirs+2 {
						t.Fatalf("slice made %d requests for a ceiling of %d dirs", fake.gets-before, maxDirs)
					}
					if len(result.Sizes) != len(result.Paths) {
						t.Fatalf("sizes=%d paths=%d", len(result.Sizes), len(result.Paths))
					}
					for _, path := range result.Paths {
						if path < after || (result.Next != "" && path >= result.Next) {
							t.Fatalf("path %q outside [%q, %q)", path, after, result.Next)
						}
					}
					joined = append(joined, result.Paths...)
					slices++
					if result.Next == "" {
						break
					}
					if result.Next <= after || slices > 500 {
						t.Fatalf("frontier %q does not advance past %q", result.Next, after)
					}
					after = result.Next
				}
				sort.Strings(joined)
				if strings.Join(joined, "\n") != strings.Join(want, "\n") {
					t.Fatalf("seed=%d recursive=%v max=%d: %d slices join to %d paths, single pass %d", seed, recursive, maxDirs, slices, len(joined), len(want))
				}
				if maxDirs == 8 && slices < 2 {
					t.Fatalf("seed=%d: a ceiling of 8 must cut the tree, got %d slice", seed, slices)
				}
			}
		}
	}
}

func TestGitHubWalkSliceWithAnUpperBoundReadsExactlyTheSameSliceUnderLooseCeilings(t *testing.T) {
	fake := randomSliceTree(3, 120)
	tight := sliceLimits(10, true)
	first, err := sliceWalk(t, fake, tight)
	if err != nil || first.Next == "" || len(first.Paths) == 0 {
		t.Fatalf("premise: next=%q paths=%d err=%v", first.Next, len(first.Paths), err)
	}
	loose := sliceLimits(5_000, true)
	loose.until = first.Next
	again, err := sliceWalk(t, fake, loose)
	if err != nil || again.Stopped != nil {
		t.Fatalf("err=%v stopped=%v", err, again.Stopped)
	}
	a, b := append([]string(nil), first.Paths...), append([]string(nil), again.Paths...)
	sort.Strings(a)
	sort.Strings(b)
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatalf("bounded slice has %d paths, first slice %d", len(b), len(a))
	}
}

func TestGitHubWalkSliceThatCannotPassItsLowerBoundIsTheCeilingError(t *testing.T) {
	fake := walkWide(50)
	limits := sliceLimits(1, false)
	first, err := sliceWalk(t, fake, limits)
	if err != nil || first.Next != "p0/" || len(first.Paths) != 0 {
		t.Fatalf("a ceiling of one directory stops at the first child: next=%q paths=%d err=%v", first.Next, len(first.Paths), err)
	}
	limits.after = first.Next
	if _, err := sliceWalk(t, fake, limits); walkLimitOf(t, err).Limit != GitHubTreeWalkLimitDirs {
		t.Fatalf("a walk that stops where it started is an error: %v", err)
	}
	limits = sliceLimits(3, false)
	limits.after = first.Next
	second, err := sliceWalk(t, fake, limits)
	if err != nil || second.Next <= first.Next || len(second.Paths) != 2 {
		t.Fatalf("a wider ceiling passes it: next=%q paths=%d err=%v", second.Next, len(second.Paths), err)
	}
}

func TestGitHubWalkSliceFrontierIsCutBackToAnAncestorWhenTooLongForTheCursor(t *testing.T) {
	segment := strings.Repeat("d", 340)
	fake := &walkTreeFake{trees: map[string][]walkEntry{
		"root": {{path: "a.go", kind: "blob"}, {path: segment, kind: "tree", sha: "s1"}, {path: "z.go", kind: "blob"}},
		"s1":   {{path: segment, kind: "tree", sha: "s2"}},
		"s2":   {{path: segment, kind: "tree", sha: "s3"}},
		"s3":   {{path: segment, kind: "tree", sha: "s4"}, {path: "p", kind: "tree", sha: "s5"}},
		"s4":   {{path: "f.go", kind: "blob"}},
		"s5":   {{path: "g.go", kind: "blob"}},
	}}
	cut, err := sliceWalk(t, fake, sliceLimits(5, false))
	if err != nil {
		t.Fatal(err)
	}
	if want := segment + "/" + segment + "/"; cut.Next != want {
		t.Fatalf("frontier is %d bytes, want the ancestor of 2 directories", len(cut.Next))
	}
	if gitHubWalkFrontierEncodedLen(cut.Next) > gitHubWalkFrontierMaxBytes || len(cut.Paths) != 1 || cut.Paths[0] != "a.go" {
		t.Fatalf("paths=%v", cut.Paths)
	}
	limits := sliceLimits(50, false)
	limits.after = cut.Next
	rest, err := sliceWalk(t, fake, limits)
	if err != nil || rest.Next != "" {
		t.Fatalf("err=%v next=%q", err, rest.Next)
	}
	joined := append(append([]string(nil), cut.Paths...), rest.Paths...)
	sort.Strings(joined)
	if len(joined) != 4 {
		t.Fatalf("joined=%v, want a.go, f.go, g.go and z.go once", joined)
	}
}

func TestGitHubWalkSliceListsTheFrontierAncestorsAndReadsLaterDirectoriesInOneRequest(t *testing.T) {
	fake := randomSliceTree(5, 120)
	cut, err := sliceWalk(t, fake, sliceLimits(12, true))
	if err != nil || cut.Next == "" {
		t.Fatalf("next=%q err=%v", cut.Next, err)
	}
	limits := sliceLimits(5_000, true)
	limits.after = cut.Next
	before := fake.gets
	rest, err := sliceWalk(t, fake, limits)
	if err != nil || rest.Next != "" {
		t.Fatalf("err=%v next=%q", err, rest.Next)
	}
	depth := strings.Count(cut.Next, "/") + 1
	if rest.Stats.dirs > fake.countDirs()-cut.Stats.dirs+depth+1 {
		t.Fatalf("rest read %d dirs of %d total, first slice %d", rest.Stats.dirs, fake.countDirs(), cut.Stats.dirs)
	}
	if fake.gets-before > rest.Stats.dirs {
		t.Fatalf("%d requests for %d dirs: subtrees after the frontier must be read recursively", fake.gets-before, rest.Stats.dirs)
	}
}

func (fake *walkTreeFake) countDirs() int {
	count := 0
	var visit func(sha string)
	visit = func(sha string) {
		count++
		for _, entry := range fake.trees[sha] {
			if entry.kind == "tree" {
				visit(entry.sha)
			}
		}
	}
	visit("root")
	return count
}

// blobPaths lists every blob path below sha, sorted.
func (fake *walkTreeFake) blobPaths(sha string) []string {
	var out []string
	var visit func(sha, prefix string)
	visit = func(sha, prefix string) {
		for _, entry := range fake.trees[sha] {
			path := joinGitHubWalkPath(prefix, entry.path)
			if entry.kind == "tree" {
				visit(entry.sha, path)
			} else {
				out = append(out, path)
			}
		}
	}
	visit(sha, "")
	sort.Strings(out)
	return out
}
