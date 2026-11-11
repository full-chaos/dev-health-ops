---
page_id: admin-workload-administration
summary: Review platform workload registration and binding with an operator, and check the authority required for credential lifecycle events.
content_type: task-guide
owner: platform-product
source_of_truth:
  - contracts/auth/v1/vocabulary/workload-registry.json
  - cmd/auth-migrate/workloads.go
  - internal/auth/workload/store.go
  - internal/auth/authapi/acr_events.go
applicability: current
lifecycle: active
---

# Workload administration

A platform workload is a service identity in auth-service. It is not a workspace member.
A workload row sets the audiences and actions available to that identity. The row grants
no organization access. See the [Workload registry](../../reference/auth/workload-registry.md)
for the current rows and their limits.

## Know who can make the change

An operator runs `auth-migrate workloads` with the migration database role. The command
refuses the runtime role. A workspace administrator role does not give permission to seed
or bind a platform workload.

As a workspace administrator, review the intended service and its required access with
the operator. Do not request a shared workload identity for several services.

## Request registration or a binding

1. Find the service in the [registry reference](../../reference/auth/workload-registry.md).
   Check its owner, audiences and allowed actions.
2. Ask the operator to check the registry seed. The operator verb is
   `auth-migrate workloads seed`. It is a dry run without `-apply`. The output gives
   `created`, `updated`, `unchanged` and `unlisted` counts. A seed creates no credential
   and adds no binding. It leaves unlisted workloads in place.
3. Choose the deployment procedure with the operator:

   | Deployment or change | Operator procedure |
   | --- | --- |
   | Kubernetes identity | [Kubernetes federation](../../operate/runbooks/auth-workload-kubernetes-federation.md). The operator uses `auth-migrate workloads bind` after the workload is registered. |
   | Deployment without Kubernetes | [Workload bootstrap credential](../../operate/runbooks/auth-workload-bootstrap-credential.md). The operator uses `bootstrap-create`, `subject` and `bootstrap-rotate` under `auth-migrate workloads`. |
   | Web assertion signing key | [Web assertion key rotation](../../operate/runbooks/auth-workload-assertion-key-rotation.md). |

4. Ask the operator for the result and the audit event, with no credential value.
   A new workload writes `auth.workload.registered`. A change to its audiences or actions
   writes `auth.workload.audiences_changed` or `auth.workload.allowed_actions_changed`.
   An unchanged seed writes no audit row. A new binding writes `auth.workload.binding_added`.

The counts and audit events are operator evidence. Keep credential values out of the
administrative record. The runbooks give the operator steps and refusal reasons.

## Check before credential lifecycle publishing

The shipped seed gives `acr-api` the audiences `auth-service` and `dev-health-api`,
and **no allowed action**.

`acr-api` needs audience `auth-service` and action `credentials.project`. The action comes from the registry seed through `auth-migrate workloads seed -apply`, shipped with the change that turns publishing on. Do not add it by hand: the next seed apply removes it.

The credential-events route, `POST /internal/v1/acr/credential-events`, needs an enabled
live workload row that lists both. After caller authentication, the route returns `403`
if that row is absent, disabled or lacks either value. See
[ACR credential authority](../../reference/auth/acr-credential-authority.md) for the route
requirements.

For the wider service boundaries, see [Auth control plane](../../operate/architecture/auth-control-plane.md).
For credential types, see [Credential reference](../../reference/auth/credential-reference.md).
