package providersync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
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
// effect contract's rows, so the duplicate test below can see a whole aggregate next to its parts (acr: about 72,900 rows parsed,
// 36,500 kept). An artifact that would take the held rows past it is skipped whole under per_run_cap, before it is held. It is
// not derived from the admitted bound: that bound's retraction reserve (CHAOS-14259) must not refuse a report before planning.
const githubTestsMaxHeldReportRowsPerRun = 2 * maxEffectRows

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
	// PartitionWork is the work the search for aggregates' parts spent (githubTestsPartitionBound.Work counts it).
	PartitionWork int
	// BoundExhausted are the artifacts kept because the partition search ran out of work with no proven partition;
	// UndoneByAdmission are the aggregates kept because a drop was undone when a part was not admitted by the run budget.
	BoundExhausted    []int
	UndoneByAdmission []int
}

// githubTestsReportMismatch is a copy or aggregate kept with its parts (CHAOS-13991): its touch group (githubTestsPlanRunReports)
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
// identity key sorted, indexed, tested against the covered set, or marked and unmarked, one per key read when the pool of possible
// parts is built, and one per suite instance read when a possible part is tested against the aggregate's content. A search that
// runs out of work before it proves a partition leaves the artifact kept, neither dropped nor marked.
type githubTestsPartitionBound struct {
	Work int
}

// githubTestsDefaultPartitionBound: 8,388,608 key checks per run, a map lookup each, well under a second; acr's run of about
// 73,000 held rows spends about 290,000.
var githubTestsDefaultPartitionBound = githubTestsPartitionBound{Work: 1 << 23}

// githubTestsPlanRunReports decides a run's report artifacts. (1) Copies: artifacts equal on every case's class, name, outcome and
// duration and on every suite's name, start and duration, all carrying durations at all (without them two reports cannot be told
// from two real executions of the same tests, such as matrix legs), are one copy family; its representative is chosen by content
// (githubTestsCopyRepresentative), the others are its copies. (2) Aggregates: among representatives, smallest first, an artifact whose case identities are a disjoint union of
// two or more smaller artifacts' that are not themselves aggregates is an aggregate of those parts. Its parts are searched first
// among the artifacts whose suites (each with its cases) are all in the aggregate, so a partition that holds every stored row is
// found whatever the upload order; only when none exists is a partition by case identity alone its parts. (3) Touch groups: a
// copy and its representative are one group, and so are two artifacts that hold one equal suite instance (githubTestsReportContent).
// A partition by case identity alone joins nothing: when matrix legs share identities, it can match either leg (CHAOS-14045). A copy
// or aggregate is lossless when it holds no coverage rows and its suites equal its representative's or its parts' together
// (githubTestsReportLossless). In a group where every copy and aggregate is lossless they are dropped as aggregate_of_parts; in any
// other group every artifact is kept and each copy and aggregate is reported as a mismatch. (4) The survivors are admitted whole, in the given order (ascending declared size), under the run bound,
// the rest skipped whole; a drop whose representative or parts are not all admitted (or dropped with theirs admitted) is undone and
// the admission rerun, so a drop never loses a row the bound cut. The decision reads only the parsed rows, so it does not depend on
// names, the provider, or the order the listing came back in. parsed must already be in admission order.
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
	var families [][]int
	for _, index := range byID {
		placed := false
		for family, members := range families {
			if first := members[0]; timed[index] && timed[first] && githubTestsCountsEqual(copies[index], copies[first]) {
				families[family], placed = append(members, index), true
				break
			}
		}
		if !placed {
			families = append(families, []int{index})
		}
	}
	var representatives []int
	for _, members := range families {
		representative := members[0]
		if len(members) > 1 {
			var err error
			if representative, err = githubTestsCopyRepresentative(parsed, content, members); err != nil {
				return githubTestsRunReportPlan{}, err
			}
		}
		representatives = append(representatives, representative)
		for _, index := range members {
			if index == representative {
				continue
			}
			lossless, err := githubTestsReportLossless(parsed[index], content, index, []int{representative})
			if err != nil {
				return githubTestsRunReportPlan{}, err
			}
			relations = append(relations, relation{index: index, parts: []int{representative}, lossless: lossless})
		}
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
		var identityPool, contentPool []int
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
			if !part {
				continue
			}
			identityPool = append(identityPool, other)
			if len(parsed[index].Coverage) > 0 {
				continue
			}
			within, err := githubTestsContentWithin(content, other, index, &plan.PartitionWork)
			if err != nil {
				return githubTestsRunReportPlan{}, err
			}
			if within {
				contentPool = append(contentPool, other)
			}
		}
		if len(identityPool) < 2 {
			continue
		}
		var parts []int
		lossless, exhausted := false, false
		if len(contentPool) >= 2 {
			spent, err := githubTestsFindPartitions(sortedKeys(index), contentPool, sortedKeys, &plan.PartitionWork, bound, func(found []int) (bool, error) {
				ok, err := githubTestsReportLossless(parsed[index], content, index, found)
				if err != nil || !ok {
					return err != nil, err
				}
				parts, lossless = append([]int(nil), found...), true
				return true, nil
			})
			if err != nil {
				return githubTestsRunReportPlan{}, err
			}
			exhausted = exhausted || spent
		}
		if !lossless {
			spent, err := githubTestsFindPartitions(sortedKeys(index), identityPool, sortedKeys, &plan.PartitionWork, bound, func(found []int) (bool, error) {
				parts = append([]int(nil), found...)
				return true, nil
			})
			if err != nil {
				return githubTestsRunReportPlan{}, err
			}
			exhausted = exhausted || spent
		}
		if parts == nil && exhausted {
			plan.BoundExhausted = append(plan.BoundExhausted, index)
		}
		if parts != nil {
			aggregate[index] = true
			relations = append(relations, relation{index: index, parts: parts, lossless: lossless})
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
	conflict := make([]bool, len(parsed))
	if slices.ContainsFunc(relations, func(link relation) bool { return !link.lossless }) {
		owner := map[string]int{}
		for _, index := range candidates {
			counts, err := content(index)
			if err != nil {
				return githubTestsRunReportPlan{}, err
			}
			for key := range counts {
				if first, seen := owner[key]; seen {
					group[root(index)] = root(first)
					continue
				}
				owner[key] = index
			}
		}
		for _, link := range relations {
			if !aggregate[link.index] {
				group[root(link.index)] = root(link.parts[0])
			}
		}
		for _, link := range relations {
			if !link.lossless {
				conflict[root(link.index)] = true
			}
		}
	}
	dropped := make([]bool, len(parsed))
	var drops []relation
	for _, link := range relations {
		if conflict[root(link.index)] {
			plan.Mismatch = append(plan.Mismatch, githubTestsReportMismatch{Index: link.index, Parts: link.parts})
			continue
		}
		dropped[link.index] = true
		drops = append(drops, link)
	}
	type sized struct{ suiteBytes, caseBytes, coverageBytes int }
	sizes := make([]sized, len(parsed))
	for index, report := range parsed {
		suiteBytes, suiteErr := githubTestsRowsJSONBytes(report.Suites)
		caseBytes, caseErr := githubTestsRowsJSONBytes(report.Cases)
		coverageBytes, coverageErr := githubTestsRowsJSONBytes(report.Coverage)
		if err := errors.Join(suiteErr, caseErr, coverageErr); err != nil {
			return githubTestsRunReportPlan{}, err
		}
		sizes[index] = sized{suiteBytes, caseBytes, coverageBytes}
	}
	partsOf := make(map[int][]int, len(drops))
	for _, link := range drops {
		partsOf[link.index] = link.parts
	}
	for {
		plan.Admitted, plan.OverBound = nil, nil
		admitted := make([]bool, len(parsed))
		var budget githubTestsReportBudget
		for index, report := range parsed {
			if dropped[index] {
				continue
			}
			size := sizes[index]
			if !budget.fits(report.rows(), size.suiteBytes, size.caseBytes, size.coverageBytes) {
				plan.OverBound = append(plan.OverBound, index)
				continue
			}
			budget.admit(report.rows(), size.suiteBytes, size.caseBytes, size.coverageBytes)
			plan.Admitted = append(plan.Admitted, index)
			admitted[index] = true
		}
		// A dropped artifact's rows are held when each of its parts is admitted, or dropped with its own parts held.
		var held func(int, int) bool
		held = func(index, depth int) bool {
			if admitted[index] {
				return true
			}
			if !dropped[index] || depth > len(parsed) {
				return false
			}
			for _, part := range partsOf[index] {
				if !held(part, depth+1) {
					return false
				}
			}
			return true
		}
		undone := false
		for _, link := range drops {
			if dropped[link.index] && !held(link.index, 0) {
				dropped[link.index], undone = false, true
				if !slices.Contains(plan.UndoneByAdmission, link.index) {
					plan.UndoneByAdmission = append(plan.UndoneByAdmission, link.index)
				}
			}
		}
		if !undone {
			break
		}
	}
	for _, link := range drops {
		if dropped[link.index] {
			plan.Dropped = append(plan.Dropped, githubTestsReportDrop{Index: link.index, Cause: githubTestsAggregateOfPartsCause})
		}
	}
	sort.Slice(plan.Dropped, func(i, j int) bool { return plan.Dropped[i].Index < plan.Dropped[j].Index })
	sort.Ints(plan.UndoneByAdmission)
	sort.Slice(plan.Mismatch, func(i, j int) bool { return plan.Mismatch[i].Index < plan.Mismatch[j].Index })
	return plan, nil
}

// githubTestsRetractionRows retire the report slot of each dropped report (CHAOS-14259). An earlier sync of the run may have stored
// the report as its own slot (a build before CHAOS-14045, or a listing without its parts), and nothing else overwrites it: per
// parsed suite of the report, the row at its parsed key (slot, attempt, legacy ids, day, team and service kept) with every measure
// 0, no duration and Retracted set. The readers retire a slot whose newest row of its newest attempt is retracted
// (testopspooled). No case or coverage row: a lossless drop holds no coverage, and its cases are its parts'.
func githubTestsRetractionRows(parsed []githubTestsParsedReport, dropped []githubTestsReportDrop) []testSuiteResultRow {
	var rows []testSuiteResultRow
	for _, drop := range dropped {
		for _, suite := range parsed[drop.Index].Suites {
			suite.TotalCount, suite.PassedCount, suite.FailedCount, suite.SkippedCount = 0, 0, 0, 0
			suite.ErrorCount, suite.QuarantinedCount, suite.RetriedCount = 0, 0, 0
			suite.DurationSeconds = nil
			suite.Retracted = true
			rows = append(rows, suite)
		}
	}
	return rows
}

// githubTestsRetractionRoom is what a run's suite batch leaves for retraction rows under the effect contract: rows up to
// maxEffectRows, and the payload headroom over the admitted suites' bound (githubTestsMaxReportPayloadBytesPerDestination). A
// zero field keeps the contract; cells lower it.
type githubTestsRetractionRoom struct{ rows, bytes int }

func (room githubTestsRetractionRoom) fits(admittedSuites int, rows []testSuiteResultRow) bool {
	if room.rows == 0 {
		room.rows = maxEffectRows
	}
	if room.bytes == 0 {
		room.bytes = maxEffectPayloadBytes - githubTestsMaxReportPayloadBytesPerDestination
	}
	if admittedSuites+len(rows) > room.rows {
		return false
	}
	size := 0
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			return false
		}
		size += len(encoded)
	}
	return size <= room.bytes
}

// githubTestsRetirement is a run's parsed reports and the planner's drops of them.
type githubTestsRetirement struct {
	parsed  []githubTestsParsedReport
	dropped []githubTestsReportDrop
}

// githubTestsMinimalRetractionRows retire each dropped report's slot with one row: the slot's newest row decides, so one row
// does what a row per suite does for the readers (only an old reader after a rollback still counts the slot's other stale
// suites). The row keeps the slot, attempt, legacy ids and day of the report's first suite, holds no name, framework,
// environment, team or service, and is keyed githubTestsRetiredSlotKey: the key sorts after every parsed suite key, so the
// row joins the end of the run's sorted suite rows and moves no live row to another chunk offset.
func githubTestsMinimalRetractionRows(retirement githubTestsRetirement) []testSuiteResultRow {
	rows := make([]testSuiteResultRow, 0, len(retirement.dropped))
	for _, drop := range retirement.dropped {
		suites := githubTestsRetractionRows(retirement.parsed, []githubTestsReportDrop{drop})
		if len(suites) == 0 {
			continue
		}
		row := suites[0]
		row.SuiteID = githubTestsRetiredSlotKey(row.ReportSlot, row.RunAttempt)
		row.SuiteName, row.Framework, row.Environment, row.TeamID, row.ServiceID = "", nil, nil, nil, nil
		rows = append(rows, row)
	}
	return rows
}

// githubTestsRetiredSlotKeyPrefix starts the key of a slot's minimal retraction row: "~" sorts after the hex digits of every
// parsed suite key, and the chunk split gives such a row a chunk of its own (effectOwnChunkRows).
const githubTestsRetiredSlotKeyPrefix = "~retired-"

func githubTestsRetiredSlotKey(slot string, attempt int64) string {
	return githubTestsRetiredSlotKeyPrefix + hashTestIdentifier("github-report-retired-slot", slot, strconv.FormatInt(attempt, 10))
}

// githubTestsRetractionSplitPolicy is the tests route's chunk policy less the stamps a stored chunk adds (two cursors of at most
// 4 KiB and the chunk metadata): the split the route checks must hold under the executor's stamped split.
func githubTestsRetractionSplitPolicy() ChunkPolicy {
	policy := TestOpsRouteChunkPolicy()
	policy.MaxPreparedBytes -= 16 << 10
	return policy
}

func githubTestsEffectsSplit(effects []EffectBatch) bool {
	_, err := splitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, ShadowComparison{}, githubTestsRetractionSplitPolicy())
	return err == nil
}

// githubTestsRunReportEffects builds a run's effects with the rows that retire its dropped reports' slots. One row per suite
// (githubTestsRetractionRows) when the suite batch has room for them and the batch still splits into prepared chunks: a full
// row sits at its suite's key, so it moves live rows to other chunk offsets, where two large rows of two destinations can meet.
// Else one minimal row per slot, limited "one_row_per_slot": the split gives it a chunk of its own (effectOwnChunkRows), so it
// adds no row to a chunk of the run's own rows. (When the run's own rows do not split, chunk preparation refuses the run
// whatever it holds.)
func githubTestsRunReportEffects(room githubTestsRetractionRoom, retirement githubTestsRetirement, suites []testSuiteResultRow,
	cases []testCaseResultRow, coverage []coverageSnapshotRow, completeness []testOpsRunReportCompletenessRow,
) (effects []EffectBatch, limited string, err error) {
	build := func(retractions []testSuiteResultRow) ([]EffectBatch, error) {
		return testOpsEffects(nil, nil, nil, append(slices.Clip(suites), retractions...), cases, coverage, completeness...)
	}
	full := githubTestsRetractionRows(retirement.parsed, retirement.dropped)
	if len(full) == 0 {
		effects, err = build(nil)
		return effects, "", err
	}
	if room.fits(len(suites), full) {
		if effects, err := build(full); err == nil && githubTestsEffectsSplit(effects) {
			return effects, "", nil
		}
	}
	effects, err = build(githubTestsMinimalRetractionRows(retirement))
	if err != nil {
		return nil, "", err
	}
	return effects, "one_row_per_slot", nil
}

// githubTestsContentWithin says whether every suite instance of the report at part (githubTestsReportContent) is also in the report
// at whole, as many times, counting one work unit per instance read.
// githubTestsCoverageContent is the allow-list of coverageSnapshotRow fields that are report content, the coverage part of a copy
// family's representative key. Every other field names the run, the artifact, the format, the attribution or the sync time and
// must never steer the pick. TestGitHubTestsCoverageRowFieldsAreClassified classifies every field of coverageSnapshotRow and
// fails when one is added, removed or renamed: classify the new field there, and add it here only if it is report content.
type githubTestsCoverageContent struct {
	LinesTotal        *int64   `json:"lines_total"`
	LinesCovered      *int64   `json:"lines_covered"`
	LineCoveragePct   *float64 `json:"line_coverage_pct"`
	BranchesTotal     *int64   `json:"branches_total"`
	BranchesCovered   *int64   `json:"branches_covered"`
	BranchCoveragePct *float64 `json:"branch_coverage_pct"`
	FunctionsTotal    *int64   `json:"functions_total"`
	FunctionsCovered  *int64   `json:"functions_covered"`
	ServiceID         *string  `json:"service_id"`
}

func githubTestsCoverageContentOf(row coverageSnapshotRow) githubTestsCoverageContent {
	return githubTestsCoverageContent{
		LinesTotal: row.LinesTotal, LinesCovered: row.LinesCovered, LineCoveragePct: row.LineCoveragePct,
		BranchesTotal: row.BranchesTotal, BranchesCovered: row.BranchesCovered, BranchCoveragePct: row.BranchCoveragePct,
		FunctionsTotal: row.FunctionsTotal, FunctionsCovered: row.FunctionsCovered, ServiceID: row.ServiceID,
	}
}

// githubTestsCopyRepresentative picks a copy family's representative by content, not by id (CHAOS-14045): a member that holds
// coverage rows first (a copy is dropped only without them, so a family with one coverage-bearing member then drops the others in
// any upload order), then the lowest content digest, then the lowest id (members are in id order). The digest holds the suite
// instances and the content fields of the coverage rows (githubTestsCoverageContent), each part sorted: so the upload id decides
// only between members of equal content.
func githubTestsCopyRepresentative(
	parsed []githubTestsParsedReport, content func(int) (githubTestsCaseCounts, error), members []int,
) (int, error) {
	digest := func(index int) (string, error) {
		counts, err := content(index)
		if err != nil {
			return "", err
		}
		keys := make([]string, 0, len(counts))
		for key, count := range counts {
			keys = append(keys, key+":"+strconv.Itoa(count))
		}
		sort.Strings(keys)
		coverage := make([]string, 0, len(parsed[index].Coverage))
		for _, row := range parsed[index].Coverage {
			encoded, err := json.Marshal(githubTestsCoverageContentOf(row))
			if err != nil {
				return "", err
			}
			coverage = append(coverage, string(encoded))
		}
		sort.Strings(coverage)
		return strings.Join(keys, ",") + "\x00coverage\x00" + strings.Join(coverage, "\x00"), nil
	}
	best := members[0]
	bestDigest, err := digest(best)
	if err != nil {
		return 0, err
	}
	for _, index := range members[1:] {
		indexDigest, err := digest(index)
		if err != nil {
			return 0, err
		}
		covered, bestCovered := len(parsed[index].Coverage) > 0, len(parsed[best].Coverage) > 0
		if (covered && !bestCovered) || (covered == bestCovered && indexDigest < bestDigest) {
			best, bestDigest = index, indexDigest
		}
	}
	return best, nil
}

func githubTestsContentWithin(content func(int) (githubTestsCaseCounts, error), part, whole int, work *int) (bool, error) {
	partContent, err := content(part)
	if err != nil {
		return false, err
	}
	wholeContent, err := content(whole)
	if err != nil {
		return false, err
	}
	for key, count := range partContent {
		*work++
		if wholeContent[key] < count {
			return false, nil
		}
	}
	return true, nil
}

// githubTestsFindPartitions calls visit with each set of pool artifacts whose case identities are disjoint and together are exactly
// target (each pool artifact's identities already match target's counts), until visit returns true or *work passes bound.Work. The order is fixed: target's keys ascending, and for the first key not yet covered the pool
// artifacts that hold it in pool order.
func githubTestsFindPartitions(
	target []string, pool []int, keysOf func(int) []string, work *int, bound githubTestsPartitionBound, visit func([]int) (bool, error),
) (bool, error) {
	exhausted := false
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
	var search func(position int) (bool, error)
	search = func(position int) (bool, error) {
		for position < len(target) && covered[target[position]] {
			position++
			*work++
		}
		if *work > bound.Work {
			exhausted = true
			return true, nil
		}
		if position == len(target) {
			stop, err := visit(chosen)
			return stop || err != nil, err
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
				exhausted = true
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
	return exhausted, err
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
