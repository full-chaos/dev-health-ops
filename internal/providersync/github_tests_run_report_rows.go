package providersync

import (
	"crypto/sha256"
	"encoding/hex"
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
	// PartitionWork is the work the search for aggregates' parts spent (githubTestsPartitionBound.Work counts it).
	PartitionWork int
}

// githubTestsReportMismatch is a copy or aggregate kept with its parts (CHAOS-13991): its overlap group (githubTestsPlanRunReports)
// holds a copy or aggregate whose cases match their copy or parts by identity but that holds an outcome, another stored field or
// coverage rows they do not. Parts are the artifacts it was compared with.
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

// githubTestsPartitionBound bounds the search for an aggregate's parts in one run (CHAOS-13991). Work counts key checks: one per
// identity key sorted, indexed, tested against the covered set, or marked and unmarked, and one per key read when the pool of
// possible parts is built. Partitions caps the partitions of one aggregate compared by content. A search that runs out of work before
// it proves a partition leaves the artifact kept, neither dropped nor marked; one that proved partitions, none lossless, marks
// aggregate_mismatch.
type githubTestsPartitionBound struct {
	Work       int
	Partitions int
}

// githubTestsDefaultPartitionBound: 8,388,608 key checks per run (a map lookup each, well under a second; acr's run of about
// 73,000 held rows spends about 400,000) and 16 partitions per aggregate.
var githubTestsDefaultPartitionBound = githubTestsPartitionBound{Work: 1 << 23, Partitions: 16}

// githubTestsPlanRunReports decides a run's report artifacts. (1) Copies: an artifact equal to one with a lower id on every case's
// class, name, outcome and duration and on every suite's name, start and duration, both carrying durations at all (without them two
// reports cannot be told from two real executions of the same tests, such as matrix legs), is a copy of the lowest such id, its
// representative. (2) Aggregates: among representatives, smallest first, an artifact whose case identities are a disjoint union of
// two or more smaller artifacts' that are not themselves aggregates is an aggregate of those parts; the partitions are searched
// in a fixed order under githubTestsPartitionBound, and the first that holds every stored row of the aggregate (or else the first
// found) is its parts. (3) Overlap groups: copies with their representative and aggregates with their parts join into groups. A copy
// or aggregate is lossless when it holds no coverage rows and its suites (each with its cases) equal its representative's or its
// parts' together (githubTestsReportLossless). In a group where every copy and aggregate is lossless they are dropped as
// aggregate_of_parts; in any other group every artifact is kept and each copy and aggregate is reported as a mismatch. (4) The
// survivors are admitted whole, in the given order (ascending declared size), under the run bound, the rest skipped whole. The
// decision reads only the parsed rows, so it does not depend on names, the provider, or the order the listing came back in.
// parsed must already be in admission order.
func githubTestsPlanRunReports(parsed []githubTestsParsedReport) (githubTestsRunReportPlan, error) {
	return githubTestsPlanRunReportsWithin(parsed, githubTestsDefaultPartitionBound)
}

func githubTestsPlanRunReportsWithin(parsed []githubTestsParsedReport, bound githubTestsPartitionBound) (githubTestsRunReportPlan, error) {
	plan := githubTestsRunReportPlan{}
	identities := make([]githubTestsCaseCounts, len(parsed))
	copies := make([]githubTestsCaseCounts, len(parsed))
	timed := make([]bool, len(parsed))
	var candidates []int
	for index, report := range parsed {
		if len(report.Cases) == 0 {
			continue
		}
		candidates = append(candidates, index)
		identities[index], copies[index] = githubTestsCaseCounts{}, githubTestsCaseCounts{}
		for _, row := range report.Cases {
			identity := githubTestsCaseIdentity(row)
			identities[index][identity]++
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
	contents := make([]githubTestsCaseCounts, len(parsed))
	content := func(index int) (githubTestsCaseCounts, error) {
		if contents[index] == nil {
			counts, err := githubTestsReportContent(parsed[index])
			if err != nil {
				return nil, err
			}
			contents[index] = counts
		}
		return contents[index], nil
	}
	type relation struct {
		index    int
		parts    []int
		lossless bool
	}
	var relations []relation
	byID := append([]int(nil), candidates...)
	sort.SliceStable(byID, func(i, j int) bool { return githubTestsArtifactIDLess(parsed[byID[i]].ID, parsed[byID[j]].ID) })
	var representatives []int
	for _, index := range byID {
		representative := -1
		for _, other := range representatives {
			if timed[index] && timed[other] && githubTestsCountsEqual(copies[index], copies[other]) {
				representative = other
				break
			}
		}
		if representative < 0 {
			representatives = append(representatives, index)
			continue
		}
		lossless, err := githubTestsReportLossless(parsed[index], content, index, []int{representative})
		if err != nil {
			return githubTestsRunReportPlan{}, err
		}
		relations = append(relations, relation{index: index, parts: []int{representative}, lossless: lossless})
	}
	bySize := append([]int(nil), representatives...)
	sort.SliceStable(bySize, func(i, j int) bool { return len(parsed[bySize[i]].Cases) < len(parsed[bySize[j]].Cases) })
	aggregate := make([]bool, len(parsed))
	keys := make([][]string, len(parsed))
	sortedKeys := func(index int) []string {
		if keys[index] == nil {
			keys[index] = make([]string, 0, len(identities[index]))
			for key := range identities[index] {
				keys[index] = append(keys[index], key)
			}
			sort.Strings(keys[index])
			plan.PartitionWork += len(keys[index])
		}
		return keys[index]
	}
	for _, index := range bySize {
		if plan.PartitionWork > bound.Work {
			break
		}
		var pool []int
		for _, other := range representatives {
			if other == index || aggregate[other] || len(parsed[other].Cases) >= len(parsed[index].Cases) {
				continue
			}
			part := true
			for _, key := range sortedKeys(other) {
				plan.PartitionWork++
				if identities[other][key] != identities[index][key] {
					part = false
					break
				}
			}
			if part {
				pool = append(pool, other)
			}
		}
		if len(pool) < 2 {
			continue
		}
		var first, lossless []int
		err := githubTestsFindPartitions(sortedKeys(index), pool, sortedKeys, &plan.PartitionWork, bound, func(parts []int) (bool, error) {
			if first == nil {
				first = append([]int(nil), parts...)
			}
			ok, err := githubTestsReportLossless(parsed[index], content, index, parts)
			if err != nil || !ok {
				return err != nil, err
			}
			lossless = append([]int(nil), parts...)
			return true, nil
		})
		if err != nil {
			return githubTestsRunReportPlan{}, err
		}
		switch {
		case lossless != nil:
			aggregate[index] = true
			relations = append(relations, relation{index: index, parts: lossless, lossless: true})
		case first != nil:
			aggregate[index] = true
			relations = append(relations, relation{index: index, parts: first, lossless: false})
		}
	}
	group := make([]int, len(parsed))
	for index := range group {
		group[index] = index
	}
	var root func(int) int
	root = func(index int) int {
		for group[index] != index {
			group[index] = group[group[index]]
			index = group[index]
		}
		return index
	}
	for _, link := range relations {
		for _, part := range link.parts {
			group[root(part)] = root(link.index)
		}
	}
	conflict := make([]bool, len(parsed))
	for _, link := range relations {
		if !link.lossless {
			conflict[root(link.index)] = true
		}
	}
	dropped := make([]bool, len(parsed))
	for _, link := range relations {
		if conflict[root(link.index)] {
			plan.Mismatch = append(plan.Mismatch, githubTestsReportMismatch{Index: link.index, Parts: link.parts})
			continue
		}
		dropped[link.index] = true
		plan.Dropped = append(plan.Dropped, githubTestsReportDrop{Index: link.index, Cause: githubTestsAggregateOfPartsCause})
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

// githubTestsFindPartitions calls visit with each set of pool artifacts whose case identities are disjoint and together are exactly
// target (each pool artifact's identities already match target's counts), until visit returns true, bound.Partitions sets were
// visited, or *work passes bound.Work. The order is fixed: target's keys ascending, and for the first key not yet covered the pool
// artifacts that hold it in pool order.
func githubTestsFindPartitions(
	target []string, pool []int, keysOf func(int) []string, work *int, bound githubTestsPartitionBound, visit func([]int) (bool, error),
) error {
	holders := make(map[string][]int, len(target))
	*work += len(target)
	for _, item := range pool {
		for _, key := range keysOf(item) {
			holders[key] = append(holders[key], item)
		}
		*work += len(keysOf(item))
	}
	covered := make(map[string]bool, len(target))
	var chosen []int
	visited := 0
	var search func(position int) (bool, error)
	search = func(position int) (bool, error) {
		for position < len(target) && covered[target[position]] {
			position++
			*work++
		}
		if *work > bound.Work {
			return true, nil
		}
		if position == len(target) {
			visited++
			stop, err := visit(chosen)
			return stop || err != nil || visited >= bound.Partitions, err
		}
		for _, item := range holders[target[position]] {
			free := true
			for _, key := range keysOf(item) {
				*work++
				if covered[key] {
					free = false
					break
				}
			}
			if *work > bound.Work {
				return true, nil
			}
			if !free {
				continue
			}
			for _, key := range keysOf(item) {
				covered[key] = true
			}
			*work += len(keysOf(item))
			chosen = append(chosen, item)
			stop, err := search(position + 1)
			if stop || err != nil {
				return true, err
			}
			chosen = chosen[:len(chosen)-1]
			for _, key := range keysOf(item) {
				delete(covered, key)
			}
			*work += len(keysOf(item))
		}
		return false, nil
	}
	_, err := search(0)
	return err
}

// githubTestsReportLossless says whether dropping report (at index) loses nothing that the reports at others keep (CHAOS-13991): it
// holds no coverage rows, and its suites, each with its cases (githubTestsReportContent), equal the others' together.
func githubTestsReportLossless(
	report githubTestsParsedReport, content func(int) (githubTestsCaseCounts, error), index int, others []int,
) (bool, error) {
	if len(report.Coverage) > 0 {
		return false, nil
	}
	own, err := content(index)
	if err != nil {
		return false, err
	}
	union := githubTestsCaseCounts{}
	for _, other := range others {
		counts, err := content(other)
		if err != nil {
			return false, err
		}
		for key, count := range counts {
			union[key] += count
		}
	}
	return githubTestsCountsEqual(own, union), nil
}

// githubTestsReportContent is the multiset of a report's suite instances: each is the digest of the suite row's stored content and
// the multiset of the stored content of the case rows under that suite, so a case moved between two same-named suites changes it.
// Stored content is the row as the writer appends it (github_tests_effects_clickhouse.go) without the fields that are per artifact
// by construction: a suite's suite_id (it hashes the report slot, the artifact name), report_slot (the artifact name) and
// legacy_suite_ids (they hash the artifact id); a case's suite_id and case_id (they hash the suite_id). Case rows under no suite row
// of the report keep their suite_id, so they never equal another artifact's. Coverage rows are not listed: a report with any is
// never dropped.
func githubTestsReportContent(report githubTestsParsedReport) (githubTestsCaseCounts, error) {
	members := make(map[string][]string, len(report.Suites))
	for _, row := range report.Cases {
		suiteID := row.SuiteID
		row.SuiteID, row.CaseID = "", ""
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		members[suiteID] = append(members[suiteID], string(encoded))
	}
	counts := githubTestsCaseCounts{}
	instance := func(head string, cases []string) {
		sort.Strings(cases)
		digest := sha256.New()
		for _, part := range append([]string{head}, cases...) {
			digest.Write([]byte(strconv.Itoa(len(part)) + ":" + part))
		}
		counts[hex.EncodeToString(digest.Sum(nil))]++
	}
	for _, suite := range report.Suites {
		cases := members[suite.SuiteID]
		delete(members, suite.SuiteID)
		suite.SuiteID, suite.ReportSlot, suite.LegacySuiteIDs = "", "", nil
		encoded, err := json.Marshal(suite)
		if err != nil {
			return nil, err
		}
		instance("suite\x00"+string(encoded), cases)
	}
	for suiteID, cases := range members {
		instance("no suite\x00"+suiteID, cases)
	}
	return counts, nil
}
