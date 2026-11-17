package providersync

import (
	"fmt"
	"maps"
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
