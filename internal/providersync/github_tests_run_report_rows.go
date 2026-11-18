package providersync

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
)

// githubTestsRowsJSONBytes is the JSON size of the rows as the effect batch counts them: BuildEffectBatch compacts the
// json.Marshal output of each row, which has no insignificant whitespace, so this is exactly its payload size and never below it.
func githubTestsRowsJSONBytes[T any](rows []T) (int, error) {
	total := 0
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			return 0, err
		}
		total += len(encoded)
	}
	return total, nil
}

// githubTestsArtifactDeclaredSize is the size the artifacts listing declares; a missing or unusable size sorts last.
func githubTestsArtifactDeclaredSize(artifact githubTestsArtifactPayload) int64 {
	size, err := strconv.ParseInt(string(artifact.SizeInBytes), 10, 64)
	if err != nil || size < 0 {
		return math.MaxInt64
	}
	return size
}

func githubTestsArtifactIDLess(left, right json.Number) bool {
	l, lerr := strconv.ParseInt(string(left), 10, 64)
	r, rerr := strconv.ParseInt(string(right), 10, 64)
	if lerr == nil && rerr == nil {
		return l < r
	}
	return string(left) < string(right)
}

// githubTestsSortArtifactsForAdmission orders a run's artifacts by ascending declared size, ties by artifact id, so the
// admitted set under the run's row bound does not depend on the order the listing came back in.
func githubTestsSortArtifactsForAdmission(items []githubTestsArtifactPayload) {
	sort.SliceStable(items, func(i, j int) bool {
		left, right := githubTestsArtifactDeclaredSize(items[i]), githubTestsArtifactDeclaredSize(items[j])
		if left != right {
			return left < right
		}
		return githubTestsArtifactIDLess(items[i].ID, items[j].ID)
	})
}

// githubTestsReportBudget is what one run has admitted so far: rows (suites, cases and coverage together) and the JSON bytes per
// destination. An artifact is admitted whole or not at all.
type githubTestsReportBudget struct {
	rows                                 int
	suiteBytes, caseBytes, coverageBytes int
}

func (budget githubTestsReportBudget) fits(rows, suiteBytes, caseBytes, coverageBytes int) bool {
	return budget.rows+rows <= githubTestsMaxReportRowsPerRun &&
		budget.suiteBytes+suiteBytes <= githubTestsMaxReportPayloadBytesPerDestination &&
		budget.caseBytes+caseBytes <= githubTestsMaxReportPayloadBytesPerDestination &&
		budget.coverageBytes+coverageBytes <= githubTestsMaxReportPayloadBytesPerDestination
}

func (budget *githubTestsReportBudget) admit(rows, suiteBytes, caseBytes, coverageBytes int) {
	budget.rows += rows
	budget.suiteBytes += suiteBytes
	budget.caseBytes += caseBytes
	budget.coverageBytes += coverageBytes
}

// githubTestsMaxHeldReportRowsPerRun bounds the rows a run holds parsed while it decides which artifacts to keep: twice the
// admitted bound, so the duplicate test below can see a whole aggregate next to its parts (acr: about 72,900 rows parsed, 36,500
// kept). An artifact that would take the held rows past it is skipped whole under per_run_cap, before it is held.
const githubTestsMaxHeldReportRowsPerRun = 2 * githubTestsMaxReportRowsPerRun

// githubTestsParsedReport is one artifact's parsed rows, held until the run's artifacts are decided together.
type githubTestsParsedReport struct {
	ID       json.Number
	Name     string
	Suites   []testSuiteResultRow
	Cases    []testCaseResultRow
	Coverage []coverageSnapshotRow
	// Reports is the number of members parsed whole as a report, zero-row reports included.
	Reports int
}

func (report githubTestsParsedReport) rows() int {
	return len(report.Suites) + len(report.Cases) + len(report.Coverage)
}

// githubTestsReportDrop is an artifact left out of the run, with the marker cause that says why.
type githubTestsReportDrop struct {
	Index int
	Cause string
}

// githubTestsRunReportPlan is the decision for one run: which artifacts are admitted (in admission order), which are dropped as
// duplicates or over the bound, and which aggregates are kept although their outcomes disagree with their parts.
type githubTestsRunReportPlan struct {
	Admitted  []int
	Dropped   []githubTestsReportDrop
	OverBound []int
	Mismatch  []githubTestsReportMismatch
}

// githubTestsReportMismatch is an aggregate kept with its parts because their cases match by identity but not by outcome.
type githubTestsReportMismatch struct {
	Index int
	Parts []int
}

type githubTestsCaseCounts map[string]int

func githubTestsCountsEqual(left, right githubTestsCaseCounts) bool {
	if len(left) != len(right) {
		return false
	}
	for key, count := range left {
		if right[key] != count {
			return false
		}
	}
	return true
}

func githubTestsCaseIdentity(row testCaseResultRow) string {
	class := ""
	if row.ClassName != nil {
		class = *row.ClassName
	}
	return class + "\x00" + row.CaseName
}

// githubTestsPlanRunReports decides a run's report artifacts, in this order: (1) an artifact that is a copy of one with a lower id is
// dropped, the higher id going; a copy is equal on every case's class, name, outcome and duration and on every suite's name,
// start and duration, and carries durations at all (without them two reports cannot be told from two real executions of the same
// tests, such as matrix legs, so both stay); (2) an artifact whose cases equal the
// disjoint union of two or more other artifacts is an aggregate of its parts and is dropped, the parts staying; an aggregate whose
// cases match its parts but whose outcomes differ is kept with all of them and reported as a mismatch; (3) the survivors are
// admitted whole, in the given order (ascending declared size), under the run bound, the rest skipped whole. The decision reads
// only the parsed rows, so it does not depend on names, the provider, or the order the listing came back in.
// parsed must already be in admission order.
func githubTestsPlanRunReports(parsed []githubTestsParsedReport) (githubTestsRunReportPlan, error) {
	plan := githubTestsRunReportPlan{}
	dropped := make([]bool, len(parsed))
	identities := make([]githubTestsCaseCounts, len(parsed))
	outcomes := make([]githubTestsCaseCounts, len(parsed))
	copies := make([]githubTestsCaseCounts, len(parsed))
	timed := make([]bool, len(parsed))
	var candidates []int
	for index, report := range parsed {
		if len(report.Cases) == 0 {
			continue
		}
		candidates = append(candidates, index)
		identities[index], outcomes[index], copies[index] = githubTestsCaseCounts{}, githubTestsCaseCounts{}, githubTestsCaseCounts{}
		for _, row := range report.Cases {
			identity := githubTestsCaseIdentity(row)
			identities[index][identity]++
			outcomes[index][identity+"\x00"+row.Status]++
			duration := "none"
			if row.DurationSeconds != nil {
				duration = strconv.FormatFloat(*row.DurationSeconds, 'g', -1, 64)
				if *row.DurationSeconds > 0 {
					timed[index] = true
				}
			}
			copies[index][identity+"\x00"+row.Status+"\x00"+duration]++
		}
		for _, suite := range report.Suites {
			started, duration := "none", "none"
			if suite.StartedAt != nil {
				started = strconv.FormatInt(suite.StartedAt.UnixNano(), 10)
			}
			if suite.DurationSeconds != nil {
				duration = strconv.FormatFloat(*suite.DurationSeconds, 'g', -1, 64)
			}
			copies[index]["suite\x00"+suite.SuiteName+"\x00"+started+"\x00"+duration]++
		}
	}
	byID := append([]int(nil), candidates...)
	sort.SliceStable(byID, func(i, j int) bool { return githubTestsArtifactIDLess(parsed[byID[i]].ID, parsed[byID[j]].ID) })
	var kept []int
	for _, index := range byID {
		duplicate := false
		for _, other := range kept {
			if timed[index] && timed[other] && githubTestsCountsEqual(copies[index], copies[other]) {
				duplicate = true
				break
			}
		}
		if duplicate {
			dropped[index] = true
			plan.Dropped = append(plan.Dropped, githubTestsReportDrop{Index: index, Cause: githubTestsAggregateOfPartsCause})
			continue
		}
		kept = append(kept, index)
	}
	bySize := append([]int(nil), kept...)
	sort.SliceStable(bySize, func(i, j int) bool {
		left, right := len(parsed[bySize[i]].Cases), len(parsed[bySize[j]].Cases)
		if left != right {
			return left > right
		}
		return githubTestsArtifactIDLess(parsed[bySize[j]].ID, parsed[bySize[i]].ID)
	})
	for _, index := range bySize {
		var parts []int
		for _, other := range kept {
			if other == index || dropped[other] {
				continue
			}
			subset := len(identities[other]) > 0
			for key, count := range identities[other] {
				if identities[index][key] < count {
					subset = false
					break
				}
			}
			if subset {
				parts = append(parts, other)
			}
		}
		if len(parts) < 2 {
			continue
		}
		union, disjoint := githubTestsCaseCounts{}, true
		unionOutcomes := githubTestsCaseCounts{}
		for _, part := range parts {
			for key, count := range identities[part] {
				if union[key] > 0 {
					disjoint = false
				}
				union[key] += count
			}
			for key, count := range outcomes[part] {
				unionOutcomes[key] += count
			}
		}
		if !disjoint || !githubTestsCountsEqual(union, identities[index]) {
			continue
		}
		if githubTestsCountsEqual(unionOutcomes, outcomes[index]) {
			dropped[index] = true
			plan.Dropped = append(plan.Dropped, githubTestsReportDrop{Index: index, Cause: githubTestsAggregateOfPartsCause})
		} else {
			plan.Mismatch = append(plan.Mismatch, githubTestsReportMismatch{Index: index, Parts: append([]int(nil), parts...)})
		}
	}
	var budget githubTestsReportBudget
	for index, report := range parsed {
		if dropped[index] {
			continue
		}
		suiteBytes, suiteErr := githubTestsRowsJSONBytes(report.Suites)
		caseBytes, caseErr := githubTestsRowsJSONBytes(report.Cases)
		coverageBytes, coverageErr := githubTestsRowsJSONBytes(report.Coverage)
		if err := errors.Join(suiteErr, caseErr, coverageErr); err != nil {
			return githubTestsRunReportPlan{}, err
		}
		if !budget.fits(report.rows(), suiteBytes, caseBytes, coverageBytes) {
			plan.OverBound = append(plan.OverBound, index)
			continue
		}
		budget.admit(report.rows(), suiteBytes, caseBytes, coverageBytes)
		plan.Admitted = append(plan.Admitted, index)
	}
	sort.Slice(plan.Dropped, func(i, j int) bool { return plan.Dropped[i].Index < plan.Dropped[j].Index })
	sort.Slice(plan.Mismatch, func(i, j int) bool { return plan.Mismatch[i].Index < plan.Mismatch[j].Index })
	return plan, nil
}
