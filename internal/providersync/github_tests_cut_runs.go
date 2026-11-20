package providersync

import (
	"strings"
	"time"
)

// A peeled artifacts listing ends the next range at the oldest created second it saw, inclusive, so the runs of that
// second are listed again (githubTestsPeel). Two cursor lists of run keys, with fixed roles, keep those runs from being
// read twice:
//   - ActiveCutKeys records the runs of the active range read whole at the second RangeOldest. It is never a skip source.
//     It is cleared whenever RangeOldest moves to an older second, and it becomes githubTestsCutKeysOverflow when it does
//     not fit.
//   - CarriedCutKeys is the active list frozen at the peel; it belongs to RangeHi (the previous range's cut second). It is
//     the only skip source and nothing in the revisit writes it. An overflowed active list carries nothing, so the whole
//     revisit reads as main.
//
// Each list costs at most 8 + 52 bytes of the cursor.
const (
	githubTestsMaxCutKeysBytes = 52
	githubTestsCutKeysOverflow = "-"
	githubTestsCutRunsConflict = "github_tests_cut_runs"
)

// githubTestsCutRunKey is the created second and the (id, attempt) key of one listed run: "<id>" for attempt 1,
// "<id>.<attempt>" otherwise. The key is "" when the run cannot be keyed exactly; such a run is never recorded or skipped.
func githubTestsCutRunKey(run gitHubWorkflowRunPayload) (int64, string) {
	created := parseGitHubWorkflowTime(run.CreatedAt)
	if created == nil {
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
		return created.UTC().Unix(), id
	}
	return created.UTC().Unix(), id + "." + attempt
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

// githubTestsCutSecond is the unix second of a range bound ("" or unparsable = none).
func githubTestsCutSecond(bound string) (int64, bool) {
	if bound == "" {
		return 0, false
	}
	parsed, err := time.Parse(time.RFC3339, bound)
	if err != nil {
		return 0, false
	}
	return parsed.UTC().Unix(), true
}

func githubTestsCutKeysHave(keys, key string) bool {
	for _, listed := range strings.Split(keys, ",") {
		if listed == key {
			return true
		}
	}
	return false
}

// githubTestsSkipCutRun reports a run the previous range read whole at its cut second: its key is in the carried list and
// it was created in that second (the end of this range).
func githubTestsSkipCutRun(cursor githubTestsChunkCursor, second int64, key string) bool {
	cut, ok := githubTestsCutSecond(cursor.RangeHi)
	return ok && key != "" && cursor.CarriedCutKeys != "" && second == cut && githubTestsCutKeysHave(cursor.CarriedCutKeys, key)
}

// githubTestsRecordCutRun adds a run read whole (one complete verdict) to the active list when it was created in the
// second RangeOldest: only those runs can be listed again by the next range.
func githubTestsRecordCutRun(cursor githubTestsChunkCursor, second int64, key string) githubTestsChunkCursor {
	oldest, ok := githubTestsCutSecond(cursor.RangeOldest)
	switch {
	case !ok || key == "" || second != oldest:
	case cursor.ActiveCutKeys == githubTestsCutKeysOverflow || githubTestsCutKeysHave(cursor.ActiveCutKeys, key):
	case cursor.ActiveCutKeys == "" && len(key) > githubTestsMaxCutKeysBytes,
		cursor.ActiveCutKeys != "" && len(cursor.ActiveCutKeys)+1+len(key) > githubTestsMaxCutKeysBytes:
		cursor.ActiveCutKeys = githubTestsCutKeysOverflow
	case cursor.ActiveCutKeys == "":
		cursor.ActiveCutKeys = key
	default:
		cursor.ActiveCutKeys += "," + key
	}
	return cursor
}

// githubTestsDropCutRun removes a run's exact key from the active list: a later read of the same run in the active range
// that was not read whole (no verdict or an incomplete one) must not leave it carried. An overflowed list carries nothing.
func githubTestsDropCutRun(cursor githubTestsChunkCursor, key string) githubTestsChunkCursor {
	if key == "" || cursor.ActiveCutKeys == "" || cursor.ActiveCutKeys == githubTestsCutKeysOverflow {
		return cursor
	}
	kept := make([]string, 0, strings.Count(cursor.ActiveCutKeys, ",")+1)
	for _, listed := range strings.Split(cursor.ActiveCutKeys, ",") {
		if listed != key {
			kept = append(kept, listed)
		}
	}
	cursor.ActiveCutKeys = strings.Join(kept, ",")
	return cursor
}

// githubTestsFreezeCutKeys carries the active list into the next range at a peel. An overflowed list carries nothing.
func githubTestsFreezeCutKeys(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	cursor.CarriedCutKeys = ""
	if cursor.ActiveCutKeys != githubTestsCutKeysOverflow {
		cursor.CarriedCutKeys = cursor.ActiveCutKeys
	}
	cursor.ActiveCutKeys = ""
	return cursor
}

func githubTestsClearCutKeys(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	cursor.ActiveCutKeys, cursor.CarriedCutKeys = "", ""
	return cursor
}

// githubTestsCutKeysValid is the shape this code writes: an active list only with RangeOldest set (and the overflow mark
// allowed), a carried list only with RangeHi set (never the overflow mark), each of distinct well-formed keys within the
// byte cap.
func githubTestsCutKeysValid(cursor githubTestsChunkCursor) bool {
	if cursor.ActiveCutKeys != "" && (cursor.RangeOldest == "" || cursor.ActiveCutKeys != githubTestsCutKeysOverflow && !githubTestsCutKeyListValid(cursor.ActiveCutKeys)) {
		return false
	}
	return cursor.CarriedCutKeys == "" || cursor.RangeHi != "" && githubTestsCutKeyListValid(cursor.CarriedCutKeys)
}

func githubTestsCutKeyListValid(keys string) bool {
	if len(keys) > githubTestsMaxCutKeysBytes {
		return false
	}
	seen := map[string]struct{}{}
	for _, key := range strings.Split(keys, ",") {
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
