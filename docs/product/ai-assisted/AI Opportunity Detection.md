# Add AI workflow opportunity detection

## Goal

Identify high-leverage workflows where AI or agents should be applied next.

## Candidate workflow types

- Repetitive boilerplate
- Test generation
- Dependency updates
- Mechanical migrations
- Documentation drift
- Flaky test triage
- Review prep for large PRs
- Incident retrospectives

## Recommendation model

Start rule-based and provenance-backed.

Each recommendation must include:

- recommendation type
- affected repo/team
- evidence references
- confidence level
- expected benefit category
- suggested next action

## Acceptance criteria

- Opportunity candidates are generated from observable patterns
- Recommendations link to supporting metrics and artifacts
- Recommendations can be filtered by team, repo, and type
- No recommendation is generated without evidence references
- Output is suitable for future generated reports
