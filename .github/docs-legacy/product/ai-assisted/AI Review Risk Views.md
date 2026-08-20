# Build AI Review Load and AI Risk views

## Goal

Expose deeper diagnostic views for AI-created review pressure and AI-associated quality risk.

## AI Review Load view

Show:

- pickup latency
- review comments per LOC
- change request rate
- approval friction
- push iterations after first review
- reviewer concentration

## AI Risk view

Show:

- hotspot overlap
- complexity overlap
- low-test-delta PRs
- security findings
- reverts
- linked incidents

## Acceptance criteria

- Views support org, team, repo, and date filters
- Views compare AI-assisted and non-AI baselines
- Risk cards drill down to PR, file, test, scan, and incident evidence
- Missing data is explicitly represented
- No individual leaderboard or cross-person ranking is introduced
