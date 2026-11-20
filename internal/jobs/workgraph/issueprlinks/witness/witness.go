// Package witness is the native-first witness rule of work_graph_issue_pr
// (chris, 2026-10-15): which pull requests and issues hold a native link, and
// which work item ids name a pull request. issueprlinks retracts a fallback row
// by it, and the team repo ownership derivation (providersync) takes the same
// rule as the witness of that removal. It is a leaf: no logging and no store,
// so a package that must not log (providersync) can import it.
package witness

import (
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Provider names carried through to the parse result. These are the values
// Python's _parse_pr_dependency_source returns as the third tuple element.
const (
	ProviderGitHub = "github"
	ProviderGitLab = "gitlab"
)

// PRSource is a `work_item_dependencies.source_work_item_id` that names a pull
// request: the repo slug it lives in, its number, and which SCM minted the id.
type PRSource struct {
	RepoSlug string
	PRNumber uint32
	Provider string
}

// ParsePRSource ports work_graph/builder.py::_parse_pr_dependency_source
// (:256-278). It recognises the two SCM work-item id shapes Go providersync
// mints -- `ghpr:<owner>/<repo>#<n>` (github_work_items_rows.go:517,
// linear_work_items_route.go:892) and `gitlab:<group>/<project>!<n>`
// (linear_work_items_route.go:896) -- and returns ok=false for anything else,
// including tracker ids (`linear:`, `jira:`, `gh:`).
//
// The split is deliberately LAST-separator (Python's rsplit(sep, 1)), so a repo
// slug that itself contains the separator keeps it: `ghpr:a/b#c#12` parses as
// slug `a/b#c`, number 12.
//
// # Documented divergence from Python: non-ASCII digits
//
// Python gates the number with str.isdigit(), which is true for non-ASCII
// decimal digits (e.g. Arabic-Indic "٣") and for superscripts (e.g. "²"). It
// then calls int(), which accepts the former and RAISES ValueError on the
// latter -- so `ghpr:o/r#²` is an uncaught crash in the Python producer today,
// and `ghpr:o/r#٣` silently parses as 3. Go's strconv.ParseUint accepts ASCII
// only, so this function rejects both.
//
// This divergence is intentional and cannot fire on real data: every
// `ghpr:`/`gitlab:` id in `work_item_dependencies` is minted by Go providersync
// from a strconv.Itoa / URL path segment, so the number is always ASCII. It is
// recorded here (and covered by a test) rather than bug-compatibly reproduced,
// because reproducing it would mean either crashing on "²" or accepting a
// digit shape no producer emits.
//
// # Second, output-equivalent divergence: numbers above uint32
//
// Python's int() is unbounded, so `ghpr:o/r#4294967296` parses; the resulting
// lookup key can then never match a row, because `git_pull_requests.number` is
// `UInt32` -- so the dependency is dropped at the PR-existence gate. Go rejects
// it here instead. Both planes write NO link for such an id; only the rejection
// reason differs (`pr_not_found_or_out_of_window` vs `unparseable_source`).
// Recorded because the counters are load-bearing, and because "same output,
// different attribution" is the kind of difference an accounting assertion
// cannot see.
func ParsePRSource(value string) (PRSource, bool) {
	var (
		body      string
		separator string
		provider  string
	)
	switch {
	case strings.HasPrefix(value, "ghpr:"):
		body = strings.TrimPrefix(value, "ghpr:")
		separator = "#"
		provider = ProviderGitHub
	case strings.HasPrefix(value, "gitlab:"):
		body = strings.TrimPrefix(value, "gitlab:")
		separator = "!"
		provider = ProviderGitLab
	default:
		return PRSource{}, false
	}

	index := strings.LastIndex(body, separator)
	if index < 0 {
		return PRSource{}, false
	}
	repoSlug, number := body[:index], body[index+len(separator):]
	if repoSlug == "" || number == "" {
		return PRSource{}, false
	}
	// ParseUint rejects a sign, so "+1"/"-1" cannot reach the <= 0 gate the way
	// Python's isdigit()+int() pair does; both planes reject them.
	parsed, err := strconv.ParseUint(number, 10, 32)
	if err != nil || parsed == 0 {
		return PRSource{}, false
	}
	return PRSource{RepoSlug: repoSlug, PRNumber: uint32(parsed), Provider: provider}, true
}

// PullRequestKey names one pull request the way work_graph_issue_pr does.
type PullRequestKey struct {
	RepoID   uuid.UUID
	PRNumber uint32
}

// NativeLinks is every pull request and every issue that carries at least one
// native link in an organization.
type NativeLinks struct {
	pullRequests map[PullRequestKey]struct{}
	issues       map[string]struct{}
}

// NewNativeLinks is an empty index; Add puts a native link in it.
func NewNativeLinks() NativeLinks {
	return NativeLinks{
		pullRequests: make(map[PullRequestKey]struct{}),
		issues:       make(map[string]struct{}),
	}
}

// Add indexes one native link: its pull request and its issue.
func (native NativeLinks) Add(repoID uuid.UUID, prNumber uint32, workItemID string) {
	native.pullRequests[PullRequestKey{RepoID: repoID, PRNumber: prNumber}] = struct{}{}
	native.issues[workItemID] = struct{}{}
}

// Outranks reports whether a native link already covers the pull request or
// the issue, so no lower-tier link between them may exist.
func (native NativeLinks) Outranks(repoID uuid.UUID, prNumber uint32, workItemID string) bool {
	if _, linked := native.pullRequests[PullRequestKey{RepoID: repoID, PRNumber: prNumber}]; linked {
		return true
	}
	_, linked := native.issues[workItemID]
	return linked
}

// HasPullRequest reports whether the pull request has a native link.
func (native NativeLinks) HasPullRequest(repoID uuid.UUID, prNumber uint32) bool {
	_, linked := native.pullRequests[PullRequestKey{RepoID: repoID, PRNumber: prNumber}]
	return linked
}

// HasIssue reports whether the issue has a native link.
func (native NativeLinks) HasIssue(workItemID string) bool {
	_, linked := native.issues[workItemID]
	return linked
}

// PullRequests lists the natively linked pull requests in a stable order.
func (native NativeLinks) PullRequests() []PullRequestKey {
	out := make([]PullRequestKey, 0, len(native.pullRequests))
	for key := range native.pullRequests {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RepoID != out[j].RepoID {
			return out[i].RepoID.String() < out[j].RepoID.String()
		}
		return out[i].PRNumber < out[j].PRNumber
	})
	return out
}

// Issues lists the natively linked issues in a stable order.
func (native NativeLinks) Issues() []string {
	out := make([]string, 0, len(native.issues))
	for id := range native.issues {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Empty reports whether no native link exists.
func (native NativeLinks) Empty() bool {
	return len(native.pullRequests) == 0 && len(native.issues) == 0
}

// IsPullRequestWorkItem reports whether a work item id names a pull or merge
// request (ParsePRSource's grammar). Such an item is never the issue side of
// an issue <> pull request link.
func IsPullRequestWorkItem(workItemID string) bool {
	_, ok := ParsePRSource(workItemID)
	return ok
}
