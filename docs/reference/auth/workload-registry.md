---
page_id: ref-auth-workload-registry
summary: The platform workloads the auth-service registry holds, the audiences and actions each may carry, and the seed and bind verbs that write the registry, with their audit events.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/workload-registry.json
  - contracts/auth/v1/vocabulary/audiences.json
  - cmd/auth-migrate/workloads.go
  - internal/auth/workload/store.go
applicability: current
lifecycle: active
---

# Workload registry

The auth-service registry holds the platform workloads that may exchange a credential for a workload token
(`workload-token.v1`). `auth-migrate workloads seed` writes the rows from
[`workload-registry.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/vocabulary/workload-registry.json).
A row names the audiences a token for that workload may carry and the catalog actions it may be narrowed to.
The exchange never widens a row: it issues a token for an audience in the row, with at most the actions in the row.
{: .fc-page-lede }

The table below is written by hand. A docs test compares it with the JSON, row by row, so it cannot drift.

## Workloads

| Workload | Audiences | Allowed actions |
| --- | --- | --- |
| `api` | `auth-service` | none |
| `auth-service` | none | none |
| `acr-api` | `auth-service`, `dev-health-api` | none |
| `web` | none | none |
| `query-api` | none | none |

- A row with no audience is a registry identity only. The exchange never resolves it: it refuses a request for any audience, and the log line
  and the counter carry the reason `no_audience`.
- No row has an allowed action today. A token for such a row carries no action.
- The registry has no worker row and no billing row. A row is added when that workload makes a credentialed call.
- Deployment bindings (trust domain, issuer, namespace, ServiceAccount) differ per environment. They are not in
  the JSON. See [bind a workload](#bind-a-workload).

## Audiences

A workload token carries only an audience whose classes include `workload_token`
([`audiences.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/vocabulary/audiences.json)).

| Audience | Services |
| --- | --- |
| `dev-health-api` | `dev-health-ops-api`, `dev-health-query-api` |
| `auth-service` | `dev-health-auth-service` |

The audience `query-api` is a different class (`effective_principal_envelope`). A workload token never carries it.

## Seed the registry

```sh
auth-migrate workloads seed
auth-migrate workloads seed -apply
```

Run it as the migration role (`AUTH_MIGRATION_DATABASE_URI_FILE`). The runtime role is refused.

- The dry run reads only. It prints `created`, `updated`, `unchanged` and `unlisted` counts. It writes no row.
- `seed` is idempotent. A second run with the same JSON changes nothing.
- A workload in the registry that is not in the JSON is listed as `unlisted` and left as it is. `seed` never deletes.
- Audit events: a new row writes `auth.workload.registered` (reason `principal_provisioned`). A changed audience
  list writes `auth.workload.audiences_changed`. A changed action list writes
  `auth.workload.allowed_actions_changed`. A row that is already equal writes no audit row.
- `seed` mints no credential and adds no binding.

## Bind a workload

A Kubernetes workload needs a binding: the cluster identity that the exchange maps to the row. The runbook
[Kubernetes federation](../../operate/runbooks/auth-workload-kubernetes-federation.md) gives the steps.

```sh
auth-migrate workloads bind -workload NAME -environment ENV -trust-domain DOMAIN -issuer ISSUER_URL -namespace NS -service-account SA
auth-migrate workloads bind -workload NAME -environment ENV -trust-domain DOMAIN -issuer ISSUER_URL -namespace NS -service-account SA -apply
```

- Audit event: `auth.workload.binding_added`.
- The dry run checks the arguments only. It does not read the registry.
- A binding is unique by trust domain, issuer, namespace and ServiceAccount name. A second `bind` with the same
  four values is refused (`federation tuple already bound`).
- `-uid` pins the ServiceAccount UID. Without it any UID of that name matches. With it the exchange refuses a different UID; the log line and the counter carry the reason
  `uid_mismatch`.
- `bind` refuses a workload that is not in the registry (`workload not registered`): run `seed` first.

## Where to look next

- [Kubernetes federation runbook](../../operate/runbooks/auth-workload-kubernetes-federation.md): setup and the
  troubleshooting table by reason.
- [Workload bootstrap credential runbook](../../operate/runbooks/auth-workload-bootstrap-credential.md):
  the Compose subject.
- [Auth service configuration](configuration.md#workload-token-exchange): the `AUTH_WORKLOAD_EXCHANGE*` settings.
- [Credential classes](credential-classes.md): the `workload_token` and `workload_bootstrap_credential` classes.
