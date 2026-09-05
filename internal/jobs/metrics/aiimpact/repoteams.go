package aiimpact

import (
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Team is the subset of a `teams` row build_repo_pattern_resolver reads
// (providers/teams.py:248).
type Team struct {
	ID           string
	Name         string
	RepoPatterns []string
}

// RepoPatternResolver ports RepoPatternTeamResolver (providers/teams.py:94)
// and its builder. It is what supplies team_id for every ai_impact PR: the
// attribution loader normalises its own team_id column to None
// (loaders/ai_impact.py:141, `raw.get("team_id") or None`, over a SQL that
// projects a literal ”), so compute_ai_impact_metrics_daily's
// `if team_id is None and team_resolver is not None` branch is taken for
// EVERY attributed PR. This resolver is therefore the sole source of the
// family's team dimension, not a fallback.
//
// Despite the name there is no glob matching at resolve time. A pattern
// containing '*' is reduced ONCE, at build time, to a literal prefix; every
// other pattern is an exact key. So resolution is a map lookup followed by an
// ordered prefix scan -- no fnmatch, no regex, and none of RE2-vs-Python's
// pattern-dialect divergences.
type RepoPatternResolver struct {
	exact    map[string]resolvedTeam
	prefixes []prefixRule
}

type resolvedTeam struct {
	ID   string
	Name string
}

type prefixRule struct {
	prefix string
	team   resolvedTeam
}

// BuildRepoPatternResolver ports build_repo_pattern_resolver (:248).
//
// Four behaviours are load-bearing and each is pinned by a test:
//
//   - str(pattern).strip().lower() uses CPython's semantics conceptually, but
//     the KEY this resolver builds is a comparison key, never persisted or
//     compared against a value Python itself lowered and stored -- so it uses
//     pythonparity.Fold, not Lower. Codex round chaos-4280-r1 (finding 6)
//     measured a REAL divergence in Lower for this call site: x/text's
//     Final_Sigma lookahead is capped at 31 case-ignorable runes where
//     CPython's is not (pythonparity.Lower's doc comment), so a pattern like
//     "AΣ"+"."*31+"B*" would resolve a DIFFERENT team than the repo name it
//     was meant to match. Fold has no position-dependent case rule at all
//     (every sigma spelling folds to one value), so it cannot exhibit that
//     boundary -- see pythonparity.Fold's doc comment for the narrower
//     residual it trades in exchange, and
//     TestNonASCIIPatternsAreComparedConsistently for both proven live.
//   - `prefix = p.rstrip("*").rstrip("/")` strips ALL trailing '*' then ALL
//     trailing '/', so "acme/**" and "acme/*" and "acme/" all reduce to
//     "acme". Go's strings.TrimRight has the same all-characters semantics --
//     it is TrimSuffix that would be wrong here.
//   - A pattern containing '*' whose reduction is EMPTY (e.g. "*", "**", "/*")
//     is DROPPED entirely, not turned into a match-everything rule. Getting
//     this wrong would silently attribute every repo to one team.
//   - prefixes.sort(key=lambda x: -len(x[0])) is DESCENDING by prefix length,
//     and Python's sort is STABLE, so equal-length prefixes keep insertion
//     order. Longest-match-wins is the intent; the stability is what makes
//     ties deterministic. sort.SliceStable reproduces both.
//
// The exact map is last-writer-wins across teams and patterns, matching
// Python's dict assignment.
func BuildRepoPatternResolver(teams []Team) *RepoPatternResolver {
	resolver := &RepoPatternResolver{exact: make(map[string]resolvedTeam)}
	for _, team := range teams {
		teamID := pythonparity.Strip(team.ID)
		teamName := pythonparity.Strip(team.Name)
		if teamID == "" || len(team.RepoPatterns) == 0 {
			continue
		}
		for _, rawPattern := range team.RepoPatterns {
			pattern := pythonparity.Fold(pythonparity.Strip(rawPattern))
			if pattern == "" {
				continue
			}
			if strings.Contains(pattern, "*") {
				prefix := strings.TrimRight(strings.TrimRight(pattern, "*"), "/")
				if prefix != "" {
					resolver.prefixes = append(resolver.prefixes, prefixRule{
						prefix: prefix, team: resolvedTeam{ID: teamID, Name: teamName},
					})
				}
				continue
			}
			resolver.exact[pattern] = resolvedTeam{ID: teamID, Name: teamName}
		}
	}
	sort.SliceStable(resolver.prefixes, func(i, j int) bool {
		return len(resolver.prefixes[i].prefix) > len(resolver.prefixes[j].prefix)
	})
	return resolver
}

// Resolve ports RepoPatternTeamResolver.resolve (:99). Returns nil when no
// rule matches, which is Python's (None, None).
func (resolver *RepoPatternResolver) Resolve(repoName string) *string {
	if resolver == nil || repoName == "" {
		return nil
	}
	key := pythonparity.Fold(pythonparity.Strip(repoName))
	if key == "" {
		// `if not repo_name` is checked BEFORE stripping in Python, so a
		// whitespace-only name reaches the lookups with an empty key and
		// simply matches nothing. Reproduced rather than short-circuited so
		// the two paths cannot diverge if the lookups ever gain a default.
		return nil
	}
	if team, found := resolver.exact[key]; found {
		id := team.ID
		return &id
	}
	for _, rule := range resolver.prefixes {
		if strings.HasPrefix(key, rule.prefix) {
			id := rule.team.ID
			return &id
		}
	}
	return nil
}

// TeamResolverFunc adapts this resolver to the TeamResolver signature Compute
// takes, mirroring job_daily.py's `lambda _repo_id, repo_name, _identity:
// repo_team_resolver.resolve(repo_name)` (:1817) -- repo_id and identity are
// deliberately unused on that call.
func (resolver *RepoPatternResolver) TeamResolverFunc() TeamResolver {
	return func(_ uuid.UUID, repoName string) *string { return resolver.Resolve(repoName) }
}
