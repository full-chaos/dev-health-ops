package complexity

// fileselect.go ports the file-source SELECTION decisions from
// run_complexity_db_job (job_complexity_db.py:301-386) -- which of
// git_files / git_blame to read from, how many files each read is capped
// at, and whether a shortfall is truly unrecoverable (blame_unusable).
//
// It does NOT port the reads themselves (those are ClickHouse queries with
// no meaningful Go-vs-Python divergence risk) or _filter_files' should_process
// gate (already ported in scanconfig.go, oracle #3). What this file exists to
// get exactly right is the BUDGET ARITHMETIC across the two phases: Python's
// `max_files: int | None` means "unbounded" as None, and a Go int cannot
// represent that without a pointer -- every function below takes/returns
// `*int` for exactly that reason, and nil must be threaded through unchanged
// everywhere None would flow through unchanged in Python.

// MissingCount ports `missing = max(total_files - non_empty, 0)`
// (job_complexity_db.py:317).
func MissingCount(totalFiles, nonEmpty int) int {
	m := totalFiles - nonEmpty
	if m < 0 {
		m = 0
	}
	return m
}

// RemainingAfter ports the budget update at job_complexity_db.py:314-315:
//
//	if remaining is not None:
//	    remaining = max(remaining - len(git_files), 0)
//
// nil is Python's None (max_files was never set): an unbounded budget stays
// unbounded forever, so nil in is nil out regardless of consumed.
func RemainingAfter(remaining *int, consumed int) *int {
	if remaining == nil {
		return nil
	}
	n := *remaining - consumed
	if n < 0 {
		n = 0
	}
	return &n
}

// ShouldAttemptBlameFallback ports the gate at job_complexity_db.py:318:
//
//	if missing > 0 and (remaining is None or remaining > 0):
//
// A remaining budget of exactly 0 (every slot already spent on git_files)
// correctly refuses the fallback even though `missing` is positive --
// max_files caps the TOTAL file count, not just the git_files phase.
func ShouldAttemptBlameFallback(missing int, remaining *int) bool {
	if missing <= 0 {
		return false
	}
	return remaining == nil || *remaining > 0
}

// BlameUnusable ports the condition that fires the "blame fallback cannot
// recover them" warning and sets blame_unusable (job_complexity_db.py:322-357):
// it is true only when there ARE missing paths that survived the
// should_process filter (an empty missing_paths list is not a failure --
// there was simply nothing left to recover) AND git_blame has no usable
// line text for this repo.
func BlameUnusable(filteredMissingPaths int, hasBlameLineText bool) bool {
	return filteredMissingPaths > 0 && !hasBlameLineText
}

// EmptyGitFilesBlameUnusable ports the sibling condition in the non_empty==0
// branch (job_complexity_db.py:358-367): when a repo has NO git_files
// contents at all, blame is attempted only if there is at least one file to
// recover (`total_files > 0`) -- a repo with zero files anywhere is not a
// blame failure, it genuinely has nothing to scan.
func EmptyGitFilesBlameUnusable(totalFiles int, hasBlameLineText bool) bool {
	return totalFiles > 0 && !hasBlameLineText
}

// FileSourcePlan is the sequence of decisions run_complexity_db_job makes
// before issuing any DB read, given only the counts a lightweight COUNT
// query already has in hand (job_complexity_db.py:305 `_git_file_counts`).
// It mirrors the function's own control flow closely enough that each field
// cites the line it replaces, so a future change to the Python side is easy
// to re-diff against.
type FileSourcePlan struct {
	// GitFilesLimit is what _load_git_files is called with (line 312).
	// Only meaningful when NonEmptyBranch is true.
	GitFilesLimit *int
	// NonEmptyBranch is true for the `if non_empty > 0` branch (line 311);
	// false selects the `else` branch (line 358), which reads git_blame
	// directly with no git_files phase at all.
	NonEmptyBranch bool
	// AttemptBlameFallback is ShouldAttemptBlameFallback's result in the
	// NonEmptyBranch, or `has_blame_text` itself in the else branch (there is
	// no missing/remaining gate when there is no git_files phase to have
	// consumed any budget) -- see ShouldReadBlameInEmptyBranch below.
	AttemptBlameFallback bool
	// BlameLimit is `remaining` as passed to _load_missing_paths and
	// _load_blame_contents (lines 320, 339) in the NonEmptyBranch, or the
	// ORIGINAL max_files unchanged in the else branch (line 383, `remaining`
	// there is still the untouched input -- nothing has consumed it yet).
	BlameLimit *int
}

// PlanNonEmptyBranch ports lines 311-357: the repo has at least one
// git_files row with real contents.
func PlanNonEmptyBranch(maxFiles *int, totalFiles, nonEmpty, gitFilesReturned int) FileSourcePlan {
	remaining := RemainingAfter(maxFiles, gitFilesReturned)
	missing := MissingCount(totalFiles, nonEmpty)
	attempt := ShouldAttemptBlameFallback(missing, remaining)
	return FileSourcePlan{
		GitFilesLimit:        maxFiles,
		NonEmptyBranch:       true,
		AttemptBlameFallback: attempt,
		BlameLimit:           remaining,
	}
}

// PlanEmptyBranch ports lines 358-384: the repo has ZERO git_files rows
// with real contents, so the entire result comes from git_blame (or nothing,
// if git_blame has no usable text either).
func PlanEmptyBranch(maxFiles *int, hasBlameText bool) FileSourcePlan {
	return FileSourcePlan{
		NonEmptyBranch:       false,
		AttemptBlameFallback: hasBlameText,
		BlameLimit:           maxFiles,
	}
}
