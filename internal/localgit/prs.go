package localgit

import (
	"context"
	"regexp"
	"strconv"
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

// decimalValue is int() of a run of Unicode decimal digits.
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

var openRefRE = regexp.MustCompile(`^refs/(?:pull|merge-requests)/(\d+)/head$`)

// InferOpenPullRequests is infer_open_pull_requests_from_refs: every
// refs/pull/<n>/head or refs/merge-requests/<n>/head is an open PR created at
// its tip commit's committed time.
func (r Repo) InferOpenPullRequests(ctx context.Context, now func() time.Time) ([]PullRequest, error) {
	out, err := r.run(ctx, "for-each-ref", "--format=%(refname)%00%(objectname)", "refs/pull", "refs/merge-requests")
	if err != nil {
		return nil, err
	}
	byNumber := map[int]PullRequest{}
	var order []int
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		name, hash, ok := strings.Cut(line, "\x00")
		if !ok {
			continue
		}
		match := openRefRE.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		number, err := strconv.Atoi(match[1])
		if err != nil {
			continue
		}
		created := now().UTC()
		if commits, err := r.readCommits(ctx, []string{hash}); err == nil && len(commits) == 1 {
			created = commits[0].CommittedAt
		}
		head := name
		if _, seen := byNumber[number]; !seen {
			order = append(order, number)
		}
		byNumber[number] = PullRequest{Number: number, State: "open", CreatedAt: created, HeadBranch: &head}
	}
	rows := make([]PullRequest, 0, len(order))
	for _, number := range order {
		rows = append(rows, byNumber[number])
	}
	return rows, nil
}
