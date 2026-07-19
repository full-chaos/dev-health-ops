---
page_id: contribute
summary: Set up the repository, understand the runtime and data boundaries, develop and test changes, and ship them through the supported review and release process.
content_type: landing
owner: engineering
source_of_truth:
  - AGENTS.md
  - docs/getting-started.md
  - docs/architecture.md
  - current repository workflows
applicability: current
lifecycle: active
hide:
  - toc
---

# Contribute

Contributor documentation explains how the Dev Health repositories fit together, how the local services and data stores run, where stable contracts live, and which checks are required for a change. It should let a developer work from the repository without reconstructing the system from issues and CI logs.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

First checkout
{: .fc-topic-card__label }

### [Get started](start/index.md)

Choose the correct repository, install its supported toolchain, start local dependencies, and verify the environment before changing code.

</article>

<article class="fc-topic-card" markdown>

System model
{: .fc-topic-card__label }

### [Architecture](architecture/index.md)

Understand the API, workers, connectors, processors, storage systems, frontend boundary, and the contracts that connect them.

</article>

<article class="fc-topic-card" markdown>

Implementation loop
{: .fc-topic-card__label }

### [Develop and test](development/index.md)

Use the repository commands, fixtures, migrations, debugging tools, and focused validation required for a bounded change.

</article>

<article class="fc-topic-card" markdown>

Shipping changes
{: .fc-topic-card__label }

### [Review and release](review-and-release/index.md)

Prepare a reviewable pull request, understand the aggregate CI gates, and use the supported release and rollback paths.

</article>

<article class="fc-topic-card" markdown>

Documentation
{: .fc-topic-card__label }

### [Contribute documentation](documentation/index.md)

Classify a page in the approved IA, write against authoritative sources, preview it, and run the reader-critical documentation checks.

</article>

</div>

## Public and internal guidance

These pages publish durable workflows and architecture that contributors need to work safely. Agent prompts, issue-specific plans, investigation notes, QA receipts, screenshot evidence, and rollout records remain internal because they explain one delivery effort rather than the supported product or engineering contract.

Start with [the repository map](start/repository-map.md) when you are unsure which codebase owns a change. Then use [the development environment guide](start/development-environment.md) to run the repository locally.
