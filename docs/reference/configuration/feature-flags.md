---
page_id: ref-config-flags
summary: Current feature keys, defaults, prerequisites, runtime owners, fallback behavior, and public availability.
content_type: configuration-reference
owner: platform-product
source_of_truth:
  - current feature registry and product navigation
  - src/dev_health_ops/licensing/registry.py
  - src/dev_health_ops/alembic/versions/0073_seed_ask_dev_wave_3_1_feature_flag.py
  - docs/architecture/licensing.md
  - deploy/go-workers/deployment.json
applicability: current
lifecycle: active
---

# Feature flags and availability

Feature documentation must distinguish product availability, organization entitlement, deployment readiness, and migration route ownership. A route or binary existing in source does not by itself make a customer workflow available.
{: .fc-page-lede }

## Required reference fields

For each supported feature, generate or verify:

- stable key and current label;
- owner and affected route, provider, job, or service;
- global default;
- license-tier or organization override behavior;
- required source, role, secret, schema, worker, or deployment capability;
- behavior when disabled;
- cleanup actions that remain available while disabled;
- review, rollout, and retirement trigger.

## Canonical incident ingestion

`canonical_incident_ingestion` is enabled by default for every license tier after the canonical cutover. The global feature decision remains a kill switch, and an organization may carry an explicit false override.

When disabled, the system blocks new canonical incident webhook enqueue, processing, and writes. Status, inspection, disconnect, credential deletion, binding revocation, and secret cleanup remain available so operators can recover safely.

Provider availability still applies:

- PagerDuty REST and verified Webhooks V3 are current supported sources when configured.
- Jira Service Management incident ingestion remains feature-off and release-blocked without live tenant proof. Do not present the implementation as generally available merely because code and unit contracts exist.

## Ask Dev and AI gates

Ask Dev has five independent feature decisions:

| Key | Controls | Default |
| --- | --- | --- |
| `ask_dev` | The in-app Ask Dev interaction layer for Context Fabric | Disabled until explicitly enabled for an organization or license |
| `ask_dev_contextual_entrypoints` | Typed, review-before-submit handoffs from approved product surfaces into Ask Dev | Disabled until explicitly enabled for an organization or license |
| `ask_dev_wave_3_1` | Server-owned question interpretation and the named-subject preflight | Disabled until explicitly enabled for an organization or license |
| `byo_llm` | Organization-managed LLM provider configuration | Existing tier and override behavior, unchanged |
| `agent_context_runtime` | Hosted ACR/MCP context capability | Existing explicit-purchase behavior, unchanged |

No gate grants any of the others. Enabling `ask_dev` does not enable contextual
entrypoints, enable Wave 3.1 interpretation, expose BYO LLM settings, or expose
Context Fabric Validation, and enabling any of those gates does not make base
Ask Dev available. The `ask_dev`, `ask_dev_contextual_entrypoints`, and
`ask_dev_wave_3_1` registry rows are globally
enabled only so their global feature decisions remain emergency kill switches;
their effective tier defaults are false for every tier. Inclusion in paid plans
may be considered later only through separate product change control; it is not
part of the current V1 entitlement.

When `ask_dev_contextual_entrypoints` is disabled, the permanent Ask Dev chat
window and `/dev` route continue to follow the base `ask_dev` decision. Approved
product surfaces do not offer contextual handoff triggers. The API capability
reports contextual entrypoints only when the base Ask Dev decision, the
independent contextual-entrypoints decision, and runtime readiness all permit
them; it never infers the contextual decision from `ask_dev`.

`ask_dev_wave_3_1` is an explicit-enable registration (`ask_dev_wave_3_1` in the
feature registry, seeded by Alembic revision `0073`). Every tier stays denied
until an organization or license override grants it, and the row is preserved on
rollback because those overrides may reference it. With the decision off, a run
behaves exactly as it did before Wave 3.1: the preflight is not constructed, and
neither is the question-understanding shadow seam, regardless of its own
environment variable. Evaluating the decision fails closed — a storage error
leaves it off rather than admitting an unreviewed path.

Do not infer Ask Dev access from a paid plan. Bundling it into all paid plans is
a deferred commercial possibility and requires a new accepted product decision,
registry/migration change, and verification matrix before implementation.

The current Wave 1 backend exposes authorized scope search and the bounded
eight-metric catalog/query API through this canonical feature decision, plus
dark additive conversation storage. It does not add Ask Dev orchestration or
the web experience. Operators should keep the entitlement disabled until the
documented launch gates pass. Disabling it later must hide or deny new Ask Dev
work without changing categorization BYO LLM behavior or ACR/MCP access;
expiry, deletion, and purge continue for content already stored.

## Go worker migration routes

The Go worker foundation is controlled by versioned migration state and the
deployment-selected queue contract rather than one generic user-facing feature
flag.

Current state:

- each Go worker process must receive an explicit registered queue set;
- deployment groups own queue concurrency, replicas, resources, and autoscaling;
- one River client serves all selected queues in one worker process;
- registered job kinds remain Celery-routed;
- no production job is admitted to River solely because a binary or container is present;
- readiness requires compatible roles, schema, registry, contracts, and complete compiled handlers;
- future route values such as shadow, canary, or River require job-specific parity and rollback evidence;
- groups may consume disjoint queues or intentionally overlap on a queue.

Document the checked-in route and deployment-group state as availability. Do
not describe a dormant process as an enabled production feature. A selected
queue set is not a feature flag and cannot remap a job kind to another queue.

## Preview and reserved features

A route marked preview or guarded by an unmet prerequisite must not be documented as generally available. AI Attribution remains omitted from the public task navigation until it is a supported destination. Context Fabric remains reserved for a later customer-task IA amendment.

## Failure behavior

Feature-off paths must fail closed at every producer, scheduler, reconciler, API, and webhook boundary that could create new work. Inspection and cleanup paths should remain available where they are needed to diagnose or safely disable the feature.
