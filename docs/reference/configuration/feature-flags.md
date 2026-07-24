---
page_id: ref-config-flags
summary: Current feature keys, defaults, prerequisites, runtime owners, fallback behavior, and public availability.
content_type: configuration-reference
owner: platform-product
source_of_truth:
  - current feature registry and product navigation
  - docs/architecture/licensing.md
  - docs/architecture/pagerduty-contract.md
  - deploy/go-workers/profiles.json
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

## Go worker migration routes

The Go worker foundation is controlled by versioned migration state and deployment profiles rather than one generic user-facing feature flag.

Current state:

- all Go profiles have zero minimum replicas;
- registered job kinds remain Celery-routed;
- no production job is admitted to River solely because a binary or container is present;
- readiness requires compatible roles, schema, registry, contracts, and complete compiled handlers;
- future route values such as shadow, canary, or River require job-specific parity and rollback evidence.

Document the checked-in route and profile state as availability. Do not describe a dormant process as an enabled production feature.

## Preview and reserved features

A route marked preview or guarded by an unmet prerequisite must not be documented as generally available. AI Attribution remains omitted from the public task navigation until it is a supported destination. Context Fabric remains reserved for a later customer-task IA amendment.

## Failure behavior

Feature-off paths must fail closed at every producer, scheduler, reconciler, API, and webhook boundary that could create new work. Inspection and cleanup paths should remain available where they are needed to diagnose or safely disable the feature.
