package issuepredges

import (
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// RepoNamedRef is an issue reference that names its repository in the text:
// GitHub owner/repo#n or GitLab group/project#n (CHAOS-9457). A GitLab !n is a
// merge request, not an issue, and is never matched.
type RepoNamedRef struct {
	RawMatch string
	Provider string // github | gitlab
	Prefix   string // work_item_id prefix: "gh:" | "gitlab:"
	Repo     string // owner/repo or group[/sub]/project
	Number   string
	RefType  textrefs.RefType
}

var repoNamedRefPattern = regexp.MustCompile(
	`(?i)(?:(close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+)?` +
		`([A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)+)#([0-9]+)`)

// ExtractRepoNamedIssueRefs finds repository-qualified issue references. A path
// of exactly two segments is read as GitHub owner/repo and, with more segments,
// as a GitLab group path. A two-segment path is also a valid GitLab
// group/project, so it is offered to BOTH prefixes; the caller resolves against
// real issue rows only, and a hit on a single prefix is the match.
func ExtractRepoNamedIssueRefs(text string) []RepoNamedRef {
	var out []RepoNamedRef
	seen := map[string]bool{}
	for _, loc := range repoNamedRefPattern.FindAllStringSubmatchIndex(text, -1) {
		start := loc[0]
		if loc[2] < 0 {
			start = loc[4]
		}
		// Unicode-aware \b on both sides (RE2's is ASCII-only), and a repo path
		// is not the tail of a longer path, URL or dotted/dashed token.
		if !textrefs.WordBoundaryBefore(text, start) || !textrefs.WordBoundaryAfter(text, loc[1]) {
			continue
		}
		if start > 0 {
			if prev := text[start-1]; prev == '/' || prev == '.' || prev == '-' {
				continue
			}
		}
		repo := text[loc[4]:loc[5]]
		number := text[loc[6]:loc[7]]
		if len(number) > 1 && number[0] == '0' {
			continue
		}
		refType := textrefs.RefReferences
		if loc[2] >= 0 {
			refType = textrefs.RefCloses
		}
		raw := text[loc[0]:loc[1]]
		provs := []struct{ name, prefix string }{{"gitlab", "gitlab:"}}
		if strings.Count(repo, "/") == 1 {
			provs = append([]struct{ name, prefix string }{{"github", "gh:"}}, provs...)
		}
		for _, p := range provs {
			k := p.prefix + repo + "#" + number
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, RepoNamedRef{RawMatch: raw, Provider: p.name, Prefix: p.prefix, Repo: repo, Number: number, RefType: refType})
		}
	}
	return out
}
