package providersync

import (
	"encoding/json"
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
