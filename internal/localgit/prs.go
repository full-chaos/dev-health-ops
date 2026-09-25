package localgit

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// PullRequest is a GitPullRequest as processors/local.py infers it.
type PullRequest struct {
	Number      int
	Title       *string
	State       string
	AuthorName  *string
	AuthorEmail *string
	CreatedAt   time.Time
	MergedAt    *time.Time
	HeadBranch  *string
}

var (
	githubMergeRE = regexp.MustCompile(`(?m)^Merge pull request #(\p{Nd}+)`)
	bangNumberRE  = regexp.MustCompile(`!(\p{Nd}+)`)
)

const gitlabMarker = "See merge request"

// wordRune is Python's \w for str patterns.
func wordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsNumber(r)
}

// prNumber applies one of the merge regexes the way Python's search does:
// the first match whose trailing `\b` holds (the number is not directly
// followed by a word character) -- RE2's \b is ASCII-only, so it is checked
// here on the runes.
func prNumber(re *regexp.Regexp, message string) (int, bool) {
	for _, loc := range re.FindAllStringSubmatchIndex(message, -1) {
		end := loc[3]
		if end < len(message) {
			next := []rune(message[end:])[0]
			if wordRune(next) {
				continue
			}
		}
		number, ok := decimalValue(message[loc[2]:loc[3]])
		if !ok {
			continue
		}
		return number, true
	}
	return 0, false
}

// gitlabNumber is _GITLAB_MERGE_MR_RE.search: `\bSee merge request\b.*!(\d+)\b`.
// The marker must start and end on word boundaries; `.*` runs to the end of the
// marker's line and is greedy, so the LAST `!<digits>` of that line whose
// digits end on a boundary is the one taken.
func gitlabNumber(message string) (int, bool) {
	from := 0
	for {
		at := strings.Index(message[from:], gitlabMarker)
		if at < 0 {
			return 0, false
		}
		start := from + at
		end := start + len(gitlabMarker)
		from = start + 1
		if start > 0 {
			prev := []rune(message[:start])
			if wordRune(prev[len(prev)-1]) {
				continue
			}
		}
		if end < len(message) && wordRune([]rune(message[end:])[0]) {
			continue
		}
		line := message[end:]
		if cut := strings.IndexAny(line, "\n"); cut >= 0 {
			line = line[:cut]
		}
		matches := bangNumberRE.FindAllStringSubmatchIndex(line, -1)
		for i := len(matches) - 1; i >= 0; i-- {
			digitsEnd := matches[i][3]
			if digitsEnd < len(line) && wordRune([]rune(line[digitsEnd:])[0]) {
				continue
			}
			if number, ok := decimalValue(line[matches[i][2]:matches[i][3]]); ok {
				return number, true
			}
		}
	}
}

// decimalValue is int() of a run of Unicode decimal digits. Python's ints have no
// limit; here a value past int64 saturates at math.MaxInt64, which every caller
// treats the way it treats any too-large number (the UInt32 and Int32 columns
// refuse it), so a long run of digits can never wrap into a small valid number.
func decimalValue(digits string) (int, bool) {
	value := 0
	for _, r := range digits {
		if !unicode.IsDigit(r) {
			return 0, false
		}
		digit, ok := unicodeDigit(r)
		if !ok {
			return 0, false
		}
		if value > (math.MaxInt-digit)/10 {
			return math.MaxInt, true
		}
		value = value*10 + digit
	}
	return value, true
}

// unicodeDigit maps a Unicode decimal digit to its value: digits come in
// contiguous runs of ten starting at a rune whose value is zero.
func unicodeDigit(r rune) (int, bool) {
	if r >= '0' && r <= '9' {
		return int(r - '0'), true
	}
	for base := r; base > r-10 && unicode.IsDigit(base); base-- {
		if !unicode.IsDigit(base - 1) {
			return int(r - base), true
		}
	}
	return 0, false
}

// firstMeaningfulTitleLine is processors/local.py _first_meaningful_title_line.
func firstMeaningfulTitleLine(message string) *string {
	if message == "" {
		return nil
	}
	for _, line := range pythonparity.SplitLines(message) {
		line = pythonparity.Strip(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "Merge pull request #") || strings.HasPrefix(line, "Merge branch ") || strings.HasPrefix(line, "See merge request") {
			continue
		}
		return &line
	}
	return nil
}

// InferMergedPullRequests is infer_merged_pull_requests_from_commits: commits
// are walked newest first and a later (older) commit with the same number
// replaces an earlier one, so the OLDEST merge commit of a number wins.
func InferMergedPullRequests(commits []Commit, since *time.Time) []PullRequest {
	byNumber := map[int]PullRequest{}
	var order []int
	for _, commit := range commits {
		if since != nil && commit.CommittedAt.Before(*since) {
			continue
		}
		number, ok := prNumber(githubMergeRE, commit.Message)
		if !ok {
			number, ok = gitlabNumber(commit.Message)
		}
		if !ok {
			continue
		}
		merged := commit.CommittedAt
		if _, seen := byNumber[number]; !seen {
			order = append(order, number)
		}
		byNumber[number] = PullRequest{
			Number: number, Title: firstMeaningfulTitleLine(commit.Message), State: "merged",
			AuthorName: commit.AuthorName, AuthorEmail: commit.AuthorEmail,
			CreatedAt: commit.CommittedAt, MergedAt: &merged,
		}
	}
	out := make([]PullRequest, 0, len(order))
	for _, number := range order {
		out = append(out, byNumber[number])
	}
	return out
}

var openRefRE = regexp.MustCompile(`^refs/(?:pull|merge-requests)/(\p{Nd}+)/head$`)

// InferOpenPullRequests is infer_open_pull_requests_from_refs: every
// refs/pull/<n>/head or refs/merge-requests/<n>/head whose target resolves to a
// commit is an open PR created at that commit's committed time. `n` is any run
// of Unicode decimal digits (Python's \d and int()). A ref is resolved like
// SymbolicReference._get_commit: a commit is used, an annotated tag is peeled
// ONCE, and anything else (a tree, a blob, a tag of a tag, an object that is
// missing) raises in Python, which skips the ref.
func (r Repo) InferOpenPullRequests(ctx context.Context, now func() time.Time) ([]PullRequest, error) {
	paths, err := r.refPaths(ctx, "refs")
	if err != nil {
		return nil, err
	}
	dir, err := r.commonDir(ctx)
	if err != nil {
		return nil, err
	}
	gitDir, _ := r.gitDir()
	byNumber := map[int]PullRequest{}
	var order []int
	for _, name := range paths {
		match := openRefRE.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		number, ok := decimalValue(match[1])
		if !ok {
			continue
		}
		tip, skip, err := r.refCommit(ctx, gitDir, dir, name)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		commits, err := r.readCommits(ctx, []string{tip})
		if err != nil || len(commits) != 1 {
			continue
		}
		switch commits[0].TimeClass {
		case timeValueError:
			continue // ValueError: infer_open_pull_requests_from_refs skips the ref
		case timeOtherError:
			return nil, fmt.Errorf("ref %s: the tip commit has a committer time Python cannot represent (OSError/OverflowError, uncaught)", name)
		}
		head := name
		if _, seen := byNumber[number]; !seen {
			order = append(order, number)
		}
		byNumber[number] = PullRequest{Number: number, State: "open", CreatedAt: commits[0].CommittedAt, HeadBranch: &head}
	}
	rows := make([]PullRequest, 0, len(order))
	for _, number := range order {
		rows = append(rows, byNumber[number])
	}
	return rows, nil
}

// refCommit is `ref.commit`: the ref is dereferenced the way GitPython reads
// it (dereferenceRef), its object's type is read without failing on a missing
// object, and an annotated tag is peeled once. skip means Python skips the ref;
// an error is an exception it does not catch.
func (r Repo) refCommit(ctx context.Context, gitDir, dir, name string) (tip string, skip bool, err error) {
	hash, skip, err := dereferenceRef(gitDir, dir, name)
	if err != nil || skip {
		return "", true, err
	}
	kind, ok := r.objectType(ctx, hash)
	if !ok {
		return "", true, nil
	}
	tip, ok = r.peelToCommit(ctx, hash, kind)
	return tip, !ok, nil
}

// objectType is `git cat-file -t` for an object that may not exist.
func (r Repo) objectType(ctx context.Context, hash string) (string, bool) {
	out, err := r.run(ctx, "cat-file", "-t", hash)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

// peelToCommit is `_get_commit`: the ref's object when it is a commit; for a
// tag object, the object it names when THAT is a commit.
func (r Repo) peelToCommit(ctx context.Context, hash, kind string) (string, bool) {
	switch kind {
	case "commit":
		return hash, true
	case "tag":
		out, err := r.run(ctx, "cat-file", "tag", hash)
		if err != nil {
			return "", false
		}
		var object, objectKind string
		for _, line := range strings.Split(string(out), "\n") {
			if line == "" {
				break
			}
			if value, ok := strings.CutPrefix(line, "object "); ok && object == "" {
				object = value
			}
			if value, ok := strings.CutPrefix(line, "type "); ok && objectKind == "" {
				objectKind = value
			}
		}
		if object != "" && objectKind == "commit" {
			return object, true
		}
	}
	return "", false
}
