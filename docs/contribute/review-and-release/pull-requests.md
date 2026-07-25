---
page_id: con-pr
summary: Keep pull requests bounded, explain the contract changed, and attach reproducible evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Prepare and review a pull request

A pull request should state:

- reader, user, operator, or developer outcome;
- source of truth and contract changed;
- scope and explicit non-goals;
- security, tenant, data, compatibility, accessibility, and rollback risk;
- tests and manual evidence;
- migration, deployment, documentation, and follow-up effects.

Separate unrelated IA, content, framework, hosting, and operational changes. Request human reviewers for the decision domain even when automation is green.
