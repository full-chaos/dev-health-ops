---
page_id: use-ai-impact
summary: Read AI-associated delivery, review, and quality signals without treating estimates as causal proof.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /ai/impact product surface
applicability: current
lifecycle: active
---

# Read AI Impact

Use `/ai/impact` to frame a team or repository conversation about AI-associated delivery, review, and quality signals. At team scope, the pull request counts are taken at the team's share of each repository it owns, so a team that shares a repository can show a fraction; the ratios and averages are the repository's own. See [Team counts at the owner's share](../../reference/metrics/team-counts-at-owner-share.md).

1. Start with the selected scope, period, and attribution coverage.
2. Separate AI-assisted, agent-created, and unknown attribution states.
3. Read delivery lift and drag components together.
4. Inspect review, rework, test-gap, revert, and incident context where shown.
5. Follow evidence before proposing a workflow change.

An association can appear because of source coverage, work mix, or another system change. The view is not a productivity label, forecast, or causal experiment.
