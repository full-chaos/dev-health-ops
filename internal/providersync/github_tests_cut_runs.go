package providersync

import "strings"

// A peeled artifacts listing ends the next range at the oldest created second it saw, inclusive, so the runs of that
// second are listed again (githubTestsPeel). The cursor keeps the runs of that second already read whole: CutSecond is
// the second (unix seconds, 0 = none) and CutRuns its comma list of run keys, or githubTestsCutRunsOverflow when the
// list did not fit (then no run of that second is skipped). The list costs at most 25 + 96 + 1 bytes of the cursor.
const (
	githubTestsMaxCutRunsBytes = 96
	githubTestsCutRunsOverflow = "-"
	// githubTestsMaxCutSecond is 9999-12-31T23:59:59Z: it bounds the encoded width of CutSecond.
	githubTestsMaxCutSecond    = 253402300799
	githubTestsCutRunsConflict = "github_tests_cut_runs"
)

// githubTestsCutRunKey is the created second and the (id, attempt) key of one listed run: "<id>" for attempt 1,
// "<id>.<attempt>" otherwise. The key is "" when the run cannot be keyed exactly; such a run is never recorded.
func githubTestsCutRunKey(run gitHubWorkflowRunPayload) (int64, string) {
	created := parseGitHubWorkflowTime(run.CreatedAt)
	if created == nil {
		return 0, ""
	}
	second := created.UTC().Unix()
	if second < 1 || second > githubTestsMaxCutSecond {
		return 0, ""
	}
	id := run.ID.String()
	if !githubTestsCutRunDigits(id, 20) {
		return 0, ""
	}
	attempt := stringValue(run.RunAttempt)
	if !githubTestsCutRunDigits(attempt, 6) {
		return 0, ""
	}
	if attempt == "1" {
		return second, id
	}
	return second, id + "." + attempt
}

// githubTestsCutRunDigits is a positive decimal of at most width digits with no leading zero.
func githubTestsCutRunDigits(value string, width int) bool {
	if value == "" || len(value) > width || value[0] == '0' {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func githubTestsCutRunsHave(runs, key string) bool {
	for _, listed := range strings.Split(runs, ",") {
		if listed == key {
			return true
		}
	}
	return false
}

// githubTestsSkipCutRun reports a run this phase already read whole at the cut second: its key is in the list of that
// exact second. An overflowed list skips nothing.
func githubTestsSkipCutRun(cursor githubTestsChunkCursor, second int64, key string) bool {
	return key != "" && second == cursor.CutSecond && cursor.CutRuns != githubTestsCutRunsOverflow &&
		githubTestsCutRunsHave(cursor.CutRuns, key)
}

// githubTestsRecordCutRun adds a run read whole (complete verdict) to the list. An older second starts a new list: the
// runs of a newer second cannot be in a later range, which ends at or before the oldest second seen.
func githubTestsRecordCutRun(cursor githubTestsChunkCursor, second int64, key string) githubTestsChunkCursor {
	switch {
	case key == "":
	case cursor.CutSecond == 0 || second < cursor.CutSecond:
		cursor.CutSecond, cursor.CutRuns = second, key
	case second > cursor.CutSecond || cursor.CutRuns == githubTestsCutRunsOverflow || githubTestsCutRunsHave(cursor.CutRuns, key):
	case len(cursor.CutRuns)+1+len(key) > githubTestsMaxCutRunsBytes:
		cursor.CutRuns = githubTestsCutRunsOverflow
	default:
		cursor.CutRuns += "," + key
	}
	return cursor
}

func githubTestsClearCutRuns(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	cursor.CutSecond, cursor.CutRuns = 0, ""
	return cursor
}

// githubTestsCutRunsValid is the shape githubTestsRecordCutRun writes: both fields or neither, a second in range, and
// the overflow mark or distinct well-formed keys within the byte cap.
func githubTestsCutRunsValid(second int64, runs string) bool {
	if second == 0 && runs == "" {
		return true
	}
	if second < 1 || second > githubTestsMaxCutSecond || runs == "" || len(runs) > githubTestsMaxCutRunsBytes {
		return false
	}
	if runs == githubTestsCutRunsOverflow {
		return true
	}
	seen := map[string]struct{}{}
	for _, key := range strings.Split(runs, ",") {
		id, attempt, hasAttempt := strings.Cut(key, ".")
		if !githubTestsCutRunDigits(id, 20) || hasAttempt && (!githubTestsCutRunDigits(attempt, 6) || attempt == "1") {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}
