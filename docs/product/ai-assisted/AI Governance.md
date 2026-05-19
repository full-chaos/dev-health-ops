# Add AI governance and policy visibility

## Goal

Give enterprise users visibility into whether AI-assisted work follows review, security, and policy controls.

## Governance signals

- AI declaration coverage
- Required human review coverage
- Sensitive repo policy coverage
- Security scan coverage
- License-risk scan coverage
- Model/tool allowlist coverage
- Policy violations

## Requirements

- Governance view must be org/team/repo scoped
- Policy state must be inspectable and evidence-backed
- Missing controls must be shown as unknown or missing, not passing
- Governance data must not require raw prompt/session capture

## Acceptance criteria

- AI Governance view exists
- Policy events can be ingested or computed
- Governance summary drills down to PRs and repos
- Violations and missing controls are visible
- View supports enterprise audit/export workflows later
