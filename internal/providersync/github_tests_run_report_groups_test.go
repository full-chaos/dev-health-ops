package providersync

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// CHAOS-13991 round 2: a copy or aggregate is dropped only when nothing it holds is lost, judged for the whole run at once. Its
// suites are compared as whole instances (a suite row with the cases under it), the copy and union relationships of the run form
// overlap groups that are dropped together or kept together, and an aggregate's parts are an actual disjoint partition of its case
// identities, smaller aggregates resolved first.

const (
	groupLCOV      = "TN:\nSF:foo.go\nDA:1,1\nLF:1\nLH:1\nend_of_record\n"
	groupCobertura = `<coverage lines-valid="1" lines-covered="1"><packages><package name="example"><classes><class filename="foo.go">` +
		`<lines><line number="1" hits="1"/></lines></class></classes></package></packages></coverage>`
)

// groupRows is the number of report rows one artifact commits alone.
func groupRows(t *testing.T, artifact rowsArtifact) int {
	t.Helper()
	walk, _, _ := rowsWalk(t, 1, artifact)
	return committedReportRows(walk)
}

// assertGroupDecision: the markers are exactly want; every artifact not marked aggregate_of_parts is committed whole; each cause
// in want is observed once; the unit advances only when nothing is marked aggregate_mismatch.
func assertGroupDecision(t *testing.T, artifacts []rowsArtifact, want map[string]string) {
	t.Helper()
	walk, _, _ := rowsWalk(t, 1, artifacts...)
	if got := markerCauses(walk); !maps.Equal(got, want) {
		t.Fatalf("markers %v, want %v", got, want)
	}
	wantRows := 0
	for _, artifact := range artifacts {
		if want[artifact.id] != githubTestsAggregateOfPartsCause {
			wantRows += groupRows(t, artifact)
		}
	}
	if got := committedReportRows(walk); got != wantRows {
		t.Fatalf("committed %d rows, want %d (every artifact not dropped)", got, wantRows)
	}
	causes := map[string]int{}
	for _, cause := range want {
		causes[cause] = 1
	}
	for _, cause := range []string{githubTestsAggregateOfPartsCause, githubTestsAggregateMismatchCause} {
		if observed(walk, cause) != causes[cause] {
			t.Fatalf("observations %+v, want %s x%d", walk.cursor.Incomplete, cause, causes[cause])
		}
	}
	if advanced := walk.final.Watermark != nil; advanced != (causes[githubTestsAggregateMismatchCause] == 0) {
		t.Fatalf("watermark advanced=%t with markers %v: only a run without a mismatch advances", advanced, want)
	}
}

func groupSuite(service string, names ...string) string {
	var body strings.Builder
	fmt.Fprintf(&body, `<testsuite name="tests" time="1.0" file="services/%s/test.py">`, service)
	for _, name := range names {
		fmt.Fprintf(&body, `<testcase classname="example.Tests" name="%s" time="0.1"/>`, name)
	}
	body.WriteString(`</testsuite>`)
	return body.String()
}

func groupArchive(id string, size int64, layout string, suites ...string) rowsArtifact {
	artifact := rowsArtifact{id: id, size: size}
	switch layout {
	case "siblings":
		artifact.body = "<testsuites>" + strings.Join(suites, "") + "</testsuites>"
	case "nested":
		artifact.body = `<testsuites><testsuite name="test-run">` + strings.Join(suites, "") + "</testsuite></testsuites>"
	case "members":
		artifact.body = suites[0]
		if len(suites) > 1 {
			artifact.extra = map[string]string{"junit-2.xml": suites[1]}
		}
	}
	return artifact
}

// P1-a: cases swapped between two same-named suites are a different report, whether the suites differ in other stored fields or
// not; the same membership in reversed suite order is the same report.
func TestGitHubTestsRunReportsCompareSuiteMembership(t *testing.T) {
	for _, branch := range []string{"copy", "union"} {
		for _, layout := range []string{"siblings", "nested", "members"} {
			for _, variant := range []string{"distinct-metadata", "same-metadata", "unchanged-membership"} {
				t.Run(branch+"/"+layout+"/"+variant, func(t *testing.T) {
					leftService, rightService := "frontend", "backend"
					if variant == "same-metadata" {
						rightService = leftService
					}
					left, right := groupSuite(leftService, "a1", "a2"), groupSuite(rightService, "b1", "b2")
					newLeft, newRight := groupSuite(leftService, "a1", "b2"), groupSuite(rightService, "b1", "a2")
					want := map[string]string{"3": githubTestsAggregateMismatchCause}
					if variant == "unchanged-membership" {
						newLeft, newRight = right, left
						want["3"] = githubTestsAggregateOfPartsCause
					}
					var artifacts []rowsArtifact
					if branch == "copy" {
						artifacts = append(artifacts, groupArchive("1", 10, layout, left, right))
					} else {
						artifacts = append(artifacts, groupArchive("1", 10, layout, left), groupArchive("2", 20, layout, right))
					}
					assertGroupDecision(t, append(artifacts, groupArchive("3", 30, layout, newLeft, newRight)), want)
				})
			}
		}
	}
}

// P1-b: three timed copies; one of them differs. The whole copy group is kept and both would-be drops are reported; coverage on
// the kept representative (id 1) loses nothing, so its copies still go.
func TestGitHubTestsRunReportsKeepTheWholeCopyGroupOnAMismatch(t *testing.T) {
	plain := strings.Replace(groupSuite("frontend", "a1", "a2"), "/>", `><failure message="boom"/></testcase>`, 1)
	mismatch := map[string]string{"2": githubTestsAggregateMismatchCause, "3": githubTestsAggregateMismatchCause}
	dropped := map[string]string{"2": githubTestsAggregateOfPartsCause, "3": githubTestsAggregateOfPartsCause}
	for _, variant := range []string{"different-case", "different-suite", "lcov", "cobertura", "unchanged"} {
		for index := range 3 {
			if variant == "unchanged" && index > 0 {
				continue
			}
			t.Run(fmt.Sprintf("%s/conflict-id-%d", variant, index+1), func(t *testing.T) {
				artifacts := []rowsArtifact{{id: "1", size: 10, body: plain}, {id: "2", size: 20, body: plain}, {id: "3", size: 30, body: plain}}
				want := mismatch
				switch variant {
				case "different-case":
					artifacts[index].body = strings.ReplaceAll(plain, "boom", "other message")
				case "different-suite":
					artifacts[index].body = strings.ReplaceAll(plain, "frontend", "backend")
				case "lcov":
					artifacts[index].extra = map[string]string{"coverage.info": groupLCOV}
				case "cobertura":
					artifacts[index].extra = map[string]string{"coverage.xml": groupCobertura}
				}
				if variant == "unchanged" || (index == 0 && (variant == "lcov" || variant == "cobertura")) {
					want = dropped
				}
				assertGroupDecision(t, artifacts, want)
			})
		}
	}
}

// P1-b: two timed copies of an aggregate of two shards. A difference anywhere in the group keeps every artifact of it.
func TestGitHubTestsRunReportsKeepTheWholeUnionGroupOnAMismatch(t *testing.T) {
	first := []fxSuite{{name: "left", cases: []fxCase{{name: "a", status: "failed"}}}}
	second := []fxSuite{{name: "right", cases: []fxCase{{name: "b"}}}}
	base := fxJUnit(append(slices.Clone(first), second...))
	for _, variant := range []string{"different-case", "different-suite", "lcov-on-earlier-aggregate", "lcov-on-later-aggregate",
		"cobertura-on-earlier-aggregate", "cobertura-on-later-aggregate", "unchanged"} {
		t.Run(variant, func(t *testing.T) {
			shardLeft := fxJUnit(first)
			var earlierExtra, laterExtra map[string]string
			extra := map[string]string{"coverage.info": groupLCOV}
			if strings.HasPrefix(variant, "cobertura") {
				extra = map[string]string{"coverage.xml": groupCobertura}
			}
			switch {
			case variant == "different-case":
				shardLeft = replaceOnce(t, shardLeft, "boom", "shard message")
			case variant == "different-suite":
				shardLeft = replaceOnce(t, shardLeft, `name="left" time=`, `name="left" framework="jest" time=`)
			case strings.Contains(variant, "earlier"):
				earlierExtra = extra
			case strings.Contains(variant, "later"):
				laterExtra = extra
			}
			want := map[string]string{"3": githubTestsAggregateMismatchCause, "4": githubTestsAggregateMismatchCause}
			if variant == "unchanged" {
				want = map[string]string{"3": githubTestsAggregateOfPartsCause, "4": githubTestsAggregateOfPartsCause}
			}
			assertGroupDecision(t, []rowsArtifact{{id: "1", size: 10, body: shardLeft}, {id: "2", size: 20, body: fxJUnit(second)},
				{id: "3", size: 30, body: base, extra: earlierExtra}, {id: "4", size: 40, body: base, extra: laterExtra}}, want)
		})
	}
}

var groupSpecs = []fxSuite{
	{name: "alpha", cases: []fxCase{{name: "a", status: "failed"}}},
	{name: "beta", cases: []fxCase{{name: "b"}}},
	{name: "gamma", cases: []fxCase{{name: "c"}}},
	{name: "delta", cases: []fxCase{{name: "d"}}},
}

func groupMembers(members [][]int, untimed bool) []rowsArtifact {
	var artifacts []rowsArtifact
	for index, member := range members {
		var suites []fxSuite
		for _, which := range member {
			suite := groupSpecs[which]
			if untimed {
				suite.cases = []fxCase{{name: suite.cases[0].name, status: suite.cases[0].status, dur: "-"}}
			}
			suites = append(suites, suite)
		}
		artifacts = append(artifacts, rowsArtifact{id: fmt.Sprint(index + 1), size: int64(index+1) * 100, body: fxJUnit(suites)})
	}
	return artifacts
}

// P1-c: a total is an aggregate of the leaves even when intermediate aggregates, crossing partitions or an identical total sit
// beside it; the intermediates are resolved first.
func TestGitHubTestsRunReportsDropNestedAggregates(t *testing.T) {
	drop := githubTestsAggregateOfPartsCause
	for _, cell := range []struct {
		name    string
		members [][]int
		want    map[string]string
	}{
		{"flat", [][]int{{0}, {1}, {2}, {0, 1, 2}}, map[string]string{"4": drop}},
		{"nested", [][]int{{0}, {1}, {2}, {0, 1}, {0, 1, 2}}, map[string]string{"4": drop, "5": drop}},
		{"two-intermediates", [][]int{{0}, {1}, {2}, {0, 1}, {1, 2}, {0, 1, 2}}, map[string]string{"4": drop, "5": drop, "6": drop}},
		{"crossing-partitions", [][]int{{0, 1}, {2, 3}, {0, 2}, {1, 3}, {0, 1, 2, 3}}, map[string]string{"5": drop}},
		{"extra-identical-aggregate-timed", [][]int{{0}, {1}, {2}, {0, 1, 2}, {0, 1, 2}}, map[string]string{"4": drop, "5": drop}},
		{"extra-identical-aggregate-untimed", [][]int{{0}, {1}, {2}, {0, 1, 2}, {0, 1, 2}}, map[string]string{"4": drop, "5": drop}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			assertGroupDecision(t, groupMembers(cell.members, strings.HasSuffix(cell.name, "untimed")), cell.want)
		})
	}
}

// P1-c: the same layouts with a total that holds a distinct case, suite field or coverage snapshot: its group is kept and reported.
func TestGitHubTestsRunReportsFindTheMismatchBehindOtherAggregates(t *testing.T) {
	mismatch := githubTestsAggregateMismatchCause
	for _, layout := range []struct {
		name    string
		members [][]int
		total   []int
		want    map[string]string
	}{
		// The intermediate 4 is a lossless aggregate of 1 and 2, but it shares their group with the total, so it is kept too.
		{"nested", [][]int{{0}, {1}, {2}, {0, 1}}, []int{0, 1, 2}, map[string]string{"4": mismatch, "5": mismatch}},
		{"crossing-partitions", [][]int{{0, 1}, {2, 3}, {0, 2}, {1, 3}}, []int{0, 1, 2, 3}, map[string]string{"5": mismatch}},
		// 4 holds the identities of 1+2+3 with other durations: an aggregate whose stored durations differ, reported as well.
		{"equal-identity-peer", [][]int{{0}, {1}, {2}, {0, 1, 2}}, []int{0, 1, 2}, map[string]string{"4": mismatch, "5": mismatch}},
	} {
		for _, kind := range []string{"case", "suite", "lcov", "cobertura"} {
			t.Run(layout.name+"/"+kind, func(t *testing.T) {
				artifacts := groupMembers(append(slices.Clone(layout.members), layout.total), false)
				if layout.name == "equal-identity-peer" {
					artifacts[3].body = strings.ReplaceAll(artifacts[3].body, `time="0.1"`, `time="0.2"`)
				}
				total := &artifacts[len(artifacts)-1]
				switch kind {
				case "case":
					total.body = replaceOnce(t, total.body, "boom", "aggregate message")
				case "suite":
					total.body = replaceOnce(t, total.body, `name="alpha" time=`, `name="alpha" framework="jest" time=`)
				case "lcov":
					total.extra = map[string]string{"coverage.info": groupLCOV}
				case "cobertura":
					total.extra = map[string]string{"coverage.xml": groupCobertura}
				}
				assertGroupDecision(t, artifacts, layout.want)
			})
		}
	}
}

// Round 3, admission: a drop is valid only when its parts are admitted. Three shards, a subtotal and a total; an unrelated report
// fills the run bound and the third shard (its archive also holds a test log) sorts after it, so the bound cuts it. The total holds
// case c, so it is kept: no row is lost to a drop made before the cut.
func TestGitHubTestsRunReportsKeepAnAggregateWhosePartTheBoundCuts(t *testing.T) {
	artifacts := groupMembers([][]int{{0}, {1}, {2}, {0, 1}, {0, 1, 2}}, false)
	log := make([]byte, 600_000)
	random := rand.NewPCG(63, 79)
	for index := range log {
		log[index] = byte(' ' + random.Uint64()%95)
	}
	artifacts[2].extra = map[string]string{"test.log": string(log)}
	artifacts = append(artifacts, rowsArtifact{id: "6", body: githubTestsNamedMultiSuiteJUnit("unrelated-job-", (githubTestsMaxReportRowsPerRun-4)/2)})
	for index := range artifacts {
		members := map[string]string{"junit.xml": artifacts[index].body}
		for name, body := range artifacts[index].extra {
			members[name] = body
		}
		artifacts[index].size = int64(len(githubTestsZip(t, members)))
	}
	if artifacts[4].size >= artifacts[5].size || artifacts[5].size >= artifacts[2].size {
		t.Fatalf("fixture order: total %d, unrelated %d, shard c %d must ascend", artifacts[4].size, artifacts[5].size, artifacts[2].size)
	}
	walk, _, _ := rowsWalk(t, 1, artifacts...)
	rows := rowsWalkCommitted(t, artifacts...)
	if !slices.ContainsFunc(rows.cases, func(row testCaseResultRow) bool { return row.CaseName == "c" }) {
		t.Fatalf("markers %v: case c is not committed", markerCauses(walk))
	}
	markers := markerCauses(walk)
	if markers["5"] == githubTestsAggregateOfPartsCause {
		t.Fatalf("markers %v: the total was dropped although the bound cut its part 3", markers)
	}
	if markers["4"] != githubTestsAggregateOfPartsCause {
		t.Fatalf("markers %v: the subtotal's parts 1 and 2 are admitted, so it is still dropped", markers)
	}
}

// Round 3, overlap groups: 7 is the exact union of 1+2 and of 6+5; 6 is an aggregate of 3+4 that differs from them. 7 shares case
// identities with 6, so it is in 6's group and is kept with it, whichever partition proved it.
func TestGitHubTestsRunReportsKeepACrossingPartitionInItsConflictGroup(t *testing.T) {
	for _, variant := range []string{"case", "suite", "lcov", "cobertura", "no-conflict"} {
		t.Run(variant, func(t *testing.T) {
			artifacts := groupMembers([][]int{{0, 2}, {1, 3}, {0}, {1}, {2, 3}, {0, 1}, {0, 1, 2, 3}}, false)
			switch variant {
			case "case":
				for _, index := range []int{0, 5, 6} {
					artifacts[index].body = replaceOnce(t, artifacts[index].body, "boom", "different failure")
				}
			case "suite":
				for _, index := range []int{0, 5, 6} {
					artifacts[index].body = replaceOnce(t, artifacts[index].body, `name="alpha" time=`, `name="alpha" framework="jest" time=`)
				}
			case "lcov":
				artifacts[5].extra = map[string]string{"coverage.info": groupLCOV}
			case "cobertura":
				artifacts[5].extra = map[string]string{"coverage.xml": groupCobertura}
			}
			want := map[string]string{"6": githubTestsAggregateMismatchCause, "7": githubTestsAggregateMismatchCause}
			if variant == "no-conflict" {
				want = map[string]string{"6": githubTestsAggregateOfPartsCause, "7": githubTestsAggregateOfPartsCause}
			}
			assertGroupDecision(t, artifacts, want)
		})
	}
}

// Round 3, search order: two matrix legs upload the same shards with other timings and the total equals one leg. Whichever leg has
// the lower artifact ids, the parts are searched among artifacts whose suites are in the total, so the total is dropped.
func TestGitHubTestsRunReportsDropAnExactMatrixTotalInEitherUploadOrder(t *testing.T) {
	for _, cell := range []struct {
		name          string
		shards        int
		otherLegFirst bool
	}{
		{"four-shards-other-leg-first", 4, true},
		{"five-shards-exact-leg-first", 5, false},
		{"five-shards-other-leg-first", 5, true},
		{"eight-shards-other-leg-first", 8, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var exact, other []fxSuite
			for index := range cell.shards {
				name := fmt.Sprintf("shard-%d", index)
				exact = append(exact, fxSuite{name: name, cases: []fxCase{{name: name, dur: "0.125"}}})
				other = append(other, fxSuite{name: name, cases: []fxCase{{name: name, dur: "0.25"}}})
			}
			var artifacts []rowsArtifact
			add := func(suites []fxSuite) {
				body := fxJUnit(suites)
				artifacts = append(artifacts, rowsArtifact{id: fmt.Sprint(len(artifacts) + 1), size: int64(len(artifacts)+1) * 10, body: body})
			}
			for index := range exact {
				if cell.otherLegFirst {
					add(other[index : index+1])
					add(exact[index : index+1])
				} else {
					add(exact[index : index+1])
					add(other[index : index+1])
				}
			}
			add(exact)
			assertGroupDecision(t, artifacts, map[string]string{artifacts[len(artifacts)-1].id: githubTestsAggregateOfPartsCause})
		})
	}
}
