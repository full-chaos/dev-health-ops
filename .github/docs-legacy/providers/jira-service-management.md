# Jira Service Management provider contract

This page is the source contract for the JSM capability in the canonical operational
model. It is intentionally narrower than the Jira provider contract. JSM incidents are
the only operational entity in scope for this slice. Alerts are a separate capability.

## Capability matrix

| Atlassian surface | Outcome | Implementation | Readiness and evidence boundary |
| --- | --- | --- | --- |
| JSM incidents | **BLOCKED** | **GO** | Draft only. The candidate query is bounded JQL, then each candidate requires native Incident GET admission. Implementation is GO for code and unit contracts. Merge and release readiness are BLOCKED because no tenant is available for live proof. |
| JSM Ops Alerts | **BLOCKED** | Not in this slice | Separate alert capability. It is not ingested here and never becomes an incident by inference. No live evidence is claimed. |
| Standalone Opsgenie | **NO_GO** | Not supported | No standalone Opsgenie catalog entry or integration is part of this provider contract. |
| Jira Software Operations Information | **NO_GO** | Not supported | Write-only association is not an incident source and has no authoritative read path in this slice. |

The matrix does not claim live tenant validation. JSM incident implementation is GO for code
and unit contracts only. The capability remains a draft and rollout, merge, and release
readiness remain BLOCKED until a bounded sync produces auditable evidence.

## Authoritative read path

The provider owns fetching, authentication, pagination, retries, and normalization. The
read sequence is:

1. Call [`GET /rest/servicedeskapi/servicedesk`][service-desks] to enumerate service desks.
   Intersect their service-project keys with the configured JSM allowlist; fail closed if a
   configured key is not an enumerated JSM service project.
2. Run the enhanced Jira issue search API with this exact bounded JQL template:
   `project in (<allowed_service_project_keys>) AND "Ticket category" = Incidents AND updated >=
   "<window_start>" AND updated < "<window_end>" ORDER BY updated ASC, key ASC`.
3. For every candidate, call the native Incident API at the fixed host
   `https://api.atlassian.com/jsm/incidents/cloudId/<cloud_id>/v1/incident/<issue_id>`.
   Only HTTP 200 is admission. HTTP 404 is a negative admission, not a tombstone. Any
   other HTTP status, transport error, malformed response, or auth error fails closed.
4. Preserve the candidate as a Jira `WorkItem` when it meets ordinary Jira provider rules.
   Separately, emit an `OperationalIncident` only for a native-admitted candidate.
5. Do not emit an `OperationalAlert` row from a ticket, alert notification, or corroboration.

Official references:

* [JSM REST API overview][jsm-rest]
* [JSM service desk enumeration][service-desks]
* [Jira issue search and JQL][jql-search]
* [Opsgenie alert API, reference only][opsgenie-alerts]

Authentication and request controls follow the existing Jira provider boundary. Native
admission is nevertheless fixed to `api.atlassian.com`; a tenant site origin is not a valid
substitute for that host. This page does not claim an authentication mode, OAuth scopes, or
live credential validation for JSM.

Service desk enumeration is paginated with endpoint-specific `start`/`limit` and
`isLastPage` fields. Enhanced Jira JQL search uses an opaque `nextPageToken` (the legacy
`/rest/api/3/search` path is not the contract). Follow each cursor until its terminal
marker, cap page size and total work with the normal provider budget, and treat a truncated
page as a failed sync rather than a complete source snapshot.

## Identity and lifecycle

The external identity is the Jira issue id, qualified by `org_id`, `provider`, and
`provider_instance_id`. Mutable summaries, labels, service desk names, URLs, and statuses
never participate in canonical identity. The canonical id is therefore stable when a
ticket is edited or moved through its workflow.

Map the Jira workflow to canonical lifecycle fields while retaining the raw status. Record
created, updated, started, resolved, and closed timestamps only when supplied by the
source. A missing transition is unknown, not evidence that the incident never existed.
The bounded JQL listing contract does not provide tombstones. A ticket that disappears from a
later bounded snapshot is not silently deleted. A native Incident API `404` is a negative
admission result, not deletion. Emit a tombstone only when an authoritative source event
explicitly says the entity was deleted, or retain the prior row and record the observation gap
for reconciliation.

## Feature-off, rollback, and unsupported relationships

The JSM incident producer is feature-off by default. It must remain disabled when the Jira
provider credentials required for the tenant are absent. Enabling it without those inputs
is a configuration error, not permission to fall back to broad JQL.
Rollback disables the producer and stops new canonical writes. Existing canonical rows
remain immutable evidence and are not rewritten into alerts or legacy incidents. Re-enable
only after the same bounded sync, source configuration, and evidence checks pass.

## Live proof and release gate

T6A remains **BLOCKED** without a live tenant. A reviewer must run the draft against a tenant
with a bounded window and capture the service desk response, exact JQL body, native Incident
GET URL, status, and normalized result. The proof must include one known incident with native
GET 200 and one candidate with native GET 404. Confirm that 404 produces no tombstone and that
an injected non-404 error fails closed. Until those checks are recorded, do not call the JSM
incident matrix GO for merge or release. JSM Ops Alerts remain separate and are not
implemented by this contract.

This slice does not infer or persist unsupported relationships. In particular, it does not
link an incident to an alert merely because timestamps, labels, or text look similar; it
does not infer service ownership from an Opsgenie team; it does not use a Jira project or
issue-key prefix as a linked-issue donor; and it does not turn Jira Software Operations
Information write-only associations into read-side incident evidence. Any future edge
must carry explicit source evidence and relationship provenance under the canonical model.

[jsm-rest]: https://developer.atlassian.com/cloud/jira/service-desk/rest/
[service-desks]: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-servicedesk/#api-rest-servicedeskapi-servicedesk-get
[jql-search]: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/
[opsgenie-alerts]: https://developer.atlassian.com/cloud/jira/service-desk-ops/rest/v2/api-group-alerts/
