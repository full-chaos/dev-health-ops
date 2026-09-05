"""Live-Python oracle for the RepoPatternTeamResolver port (CHAOS-4280).

Drives the PRODUCTION builder and resolver --
providers.teams.build_repo_pattern_resolver / RepoPatternTeamResolver.resolve
-- over a hostile team/pattern set and prints, for each probe repo name, the
team_id Python resolves it to. The Go side builds the identical inputs and
must agree on every probe.

This resolver is the SOLE source of ai_impact's team dimension (the attribution
loader normalises its own team_id to None), so a divergence here silently
mis-attributes every PR rather than failing loudly.

The probe set is built to separate the four behaviours the port could get
wrong, each of which produces a plausible-looking wrong answer rather than an
error:

  1. rstrip("*") then rstrip("/") strips ALL trailing occurrences, so "acme/**",
     "acme/*" and "acme/" all reduce to the SAME prefix "acme". A port using
     TrimSuffix/removesuffix strips one and leaves "acme/" or "acme/*".
  2. A '*' pattern reducing to EMPTY ("*", "**", "/*") is DROPPED. A port that
     kept it as a zero-length prefix would match EVERY repo and attribute the
     whole org to one team.
  3. prefixes are sorted DESCENDING by length, so the longest prefix wins even
     when a shorter one also matches.
  4. That sort is STABLE, so two prefixes of EQUAL length keep insertion order
     -- team declaration order decides, deterministically.

Exact matches take precedence over every prefix regardless of length, and the
exact map is last-writer-wins.
"""

from __future__ import annotations

import json

from dev_health_ops.providers.teams import build_repo_pattern_resolver

TEAMS = [
    # 1. Three spellings of the same prefix, all reducing to "acme".
    {"id": "team-star", "name": "Star", "repo_patterns": ["acme/**"]},
    # 2. Empty-reduction patterns -- MUST be dropped, not made match-all.
    {"id": "team-matchall", "name": "MatchAll", "repo_patterns": ["*", "**", "/*"]},
    # 3. A longer, more specific prefix that must beat "acme".
    {"id": "team-long", "name": "Long", "repo_patterns": ["acme/platform-*"]},
    # 4. Equal-length prefix declared AFTER team-long: on a length tie the
    #    EARLIER one must win (stable sort), so "acme/platform-x" -> team-long.
    {"id": "team-tie", "name": "Tie", "repo_patterns": ["acme/platfXrm-*"]},
    # 5. An exact pattern that must beat every prefix, however long.
    {"id": "team-exact", "name": "Exact", "repo_patterns": ["acme/platform-core"]},
    # 6. Case and surrounding whitespace are normalised on BOTH sides.
    {"id": "team-case", "name": "Case", "repo_patterns": ["  WIDGETS/Alpha  "]},
    # 7. A team with no patterns, and one with a blank id -- both skipped.
    {"id": "team-nopat", "name": "NoPat", "repo_patterns": []},
    {"id": "   ", "name": "Blank", "repo_patterns": ["blank/*"]},
    # 8. Duplicate exact key, later team wins (dict last-writer-wins).
    {"id": "team-dup-a", "name": "DupA", "repo_patterns": ["dup/repo"]},
    {"id": "team-dup-b", "name": "DupB", "repo_patterns": ["dup/repo"]},
    # 9. A pattern that is only whitespace -> skipped.
    {"id": "team-ws", "name": "WS", "repo_patterns": ["   "]},
]

PROBES = [
    "acme/anything",  # -> team-star via the reduced "acme" prefix
    "acme/platform-x",  # -> longest/stable-tie winner
    "acme/platform-core",  # -> team-exact, exact beats prefix
    "ACME/Platform-Core",  # -> same, case-normalised
    "  acme/platform-core ",  # -> same, whitespace-stripped
    "WIDGETS/ALPHA",  # -> team-case
    "widgets/alpha",  # -> team-case
    "totally/unrelated",  # -> None ... unless an empty prefix leaked in
    "blank/thing",  # -> None, the blank-id team was skipped
    "dup/repo",  # -> team-dup-b, last writer wins
    "",  # -> None
    "   ",  # -> None
]


def main() -> None:
    resolver = build_repo_pattern_resolver(TEAMS)
    print(json.dumps({probe: resolver.resolve(probe)[0] for probe in PROBES}))


if __name__ == "__main__":
    main()
