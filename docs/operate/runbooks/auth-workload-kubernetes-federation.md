---
page_id: op-rb-auth-workload-kubernetes-federation
summary: Turn on the Kubernetes workload-token exchange (TokenReview), seed and bind a workload, check that it works, and find the cause of a refused or unavailable exchange by its reason.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Kubernetes workload federation

A workload that runs in Kubernetes presents its projected ServiceAccount token to the auth-service exchange
(`POST /internal/v1/token/exchange`). auth-service has the cluster API server review the token (TokenReview),
maps the reviewed identity to a registry binding, and answers a workload token (`workload-token.v1`) for one
audience. A workload without Kubernetes uses the
[bootstrap credential](auth-workload-bootstrap-credential.md) instead.

The route is internal. go-api never forwards it, and nothing here exposes it outside the cluster.

## Rules

- The exchange is off by default. The chart renders it only while `authService.workloadExchange.enabled` is `true`.
- Every example uses placeholders. Replace `NAME`, `ENV`, `TRUST_DOMAIN`, `CLUSTER_ISSUER`, `NS`, `SA` and `ISSUER_URL`
  with the values of your cluster. Never paste a ServiceAccount token, a credential or a secret value into a
  command, a ticket or a log.
- The registry verbs (`seed`, `bind`) run as the migration role (`AUTH_MIGRATION_DATABASE_URI_FILE`). The runtime
  role is refused. Every verb is a dry run without `-apply`.
- The caller gets one answer per outcome: `400 invalid_grant` for a refusal, `503 temporarily_unavailable` for an
  unavailable exchange, `400 invalid_request` for a request that is malformed. It never gets a reason. Only the
  auth-service log line and the counter `auth.workload.exchanges` (labels `subject`, `outcome`, `reason`,
  `audience`) carry the reason. Read them in [Troubleshooting by reason](#troubleshooting-by-reason).

## Before you start

- auth-service runs (`authService.phase` 1 or more) and `authService.accessToken.issuerURL` is set. The exchange
  signs its tokens under that issuer.
- You know the cluster's ServiceAccount token issuer (`iss`) and a trust domain name for the cluster. The trust
  domain is a name that you choose. It appears in every binding of this cluster.
- The chart cannot see the API server address for the network policy. List it in
  `authService.networkPolicy.egress.kubeAPI.to`.

## Turn the exchange on

Set the chart values and upgrade the release.

```yaml
authService:
  accessToken:
    issuerURL: https://ISSUER_URL
  workloadExchange:
    enabled: true
    trustDomain: TRUST_DOMAIN
    clusterIssuer: https://CLUSTER_ISSUER
    kubeAPIURL: https://kubernetes.default.svc
  networkPolicy:
    egress:
      kubeAPI:
        to:
          - ipBlock:
              cidr: API_SERVER_ADDRESS/32
        port: 443
```

With `enabled: true` the chart renders:

- the environment of the table below on the auth-service pod (`templates/auth-service.yaml`);
- a ClusterRole and a ClusterRoleBinding `<release>-auth-service-token-review`
  (`templates/auth-service-token-review-rbac.yaml`). They give auth-service's own ServiceAccount the verb `create`
  on `tokenreviews.authentication.k8s.io` and nothing else;
- the mount of that ServiceAccount's token, which auth-service uses as the reviewer token;
- egress from auth-service to the API server, and ingress from go-api's pods to the exchange port.

The render fails, with a message that names the value, when `phase` is below 1, `issuerURL`, `trustDomain` or
`clusterIssuer` is empty or padded, `kubeAPIURL` is empty, or the network policy is on and
`egress.kubeAPI.to` is empty.

| Environment | Set by | Rule |
| --- | --- | --- |
| `AUTH_WORKLOAD_EXCHANGE` | `workloadExchange.enabled` | `true` mounts the Kubernetes subject. It needs `AUTH_TOKEN_ISSUER_URL`. |
| `AUTH_WORKLOAD_TRUST_DOMAIN` | `trustDomain` | Required, at most 253 bytes, no surrounding whitespace. |
| `AUTH_WORKLOAD_EXCHANGE_CLUSTER_ISSUER` | `clusterIssuer` | Required, at most 512 bytes. The `iss` of the cluster's ServiceAccount tokens. |
| `AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL` | `kubeAPIURL` | An https origin: no path, query or credentials. Checked at start. |
| `AUTH_WORKLOAD_EXCHANGE_KUBE_CA_FILE` | the chart | Absolute path of the cluster CA bundle. The API server certificate must chain to it. No other root is trusted. |
| `AUTH_WORKLOAD_EXCHANGE_REVIEWER_TOKEN_FILE` | the chart | Absolute path of auth-service's own token. It is read again on every review. |

A bad value stops auth-service at start (exit 1) with a message that names the variable and never its value. See
[Auth service configuration](../../reference/auth/configuration.md#workload-token-exchange).

## Seed and bind a workload

Seed the registry once, then bind each workload to its cluster identity. The
[workload registry](../../reference/auth/workload-registry.md) lists the rows and their audiences.

### Seed the registry

```sh
auth-migrate workloads seed
auth-migrate workloads seed -apply
```

Audit events: a new row writes `auth.workload.registered`. A changed audience list writes
`auth.workload.audiences_changed`. A changed action list writes `auth.workload.allowed_actions_changed`. An
equal row writes no audit row.

### Bind the workload

```sh
auth-migrate workloads bind -workload NAME -environment ENV -trust-domain TRUST_DOMAIN -issuer https://CLUSTER_ISSUER -namespace NS -service-account SA
auth-migrate workloads bind -workload NAME -environment ENV -trust-domain TRUST_DOMAIN -issuer https://CLUSTER_ISSUER -namespace NS -service-account SA -apply
```

- Audit event: `auth.workload.binding_added`.
- `-trust-domain` must equal `AUTH_WORKLOAD_TRUST_DOMAIN`, and `-issuer` must equal
  `AUTH_WORKLOAD_EXCHANGE_CLUSTER_ISSUER`, byte for byte. A difference makes every exchange of the workload end
  with the reason `unknown`.
- `-environment` is a short name (`[a-z][a-z0-9-]*`, at most 32 characters). It is a label only. The exchange does
  not match on it.
- `-uid` is optional. It pins the ServiceAccount UID, so a ServiceAccount that is deleted and created again with
  the same name is refused (`uid_mismatch`). Without it the name alone matches.
- The dry run checks the arguments only. It does not read the registry.
- A failed `bind` prints `workloads error: bind failed (<class>)`: `workload not registered` (run `seed`),
  `federation tuple already bound` (the four values exist already) or `binding field refused` (a value is empty,
  too long or malformed).

## Present the token from the workload

The workload mounts a projected ServiceAccount token whose audience is `dev-health-workload-exchange`. A token
with any other audience, including the pod's default API token, is refused. The chart's `goApi.extraVolumes` and
`goApi.extraVolumeMounts` carry the projection for go-api. Name the mounted file in go-api's
`AUTH_WORKLOAD_SUBJECT_FILE` and set `AUTH_WORKLOAD_ISSUER` to auth-service's issuer URL. See
[Auth service configuration](../../reference/auth/configuration.md#provisioning-routes).

The file is read again for every exchange, so the kubelet's token rotation needs no restart.

## Check that it works

1. Read the auth-service log for the line `workload token issued` (level Info, `subject` `kubernetes`,
   `outcome` `issued`, `reason` `issued`).
2. Read the counter: `auth.workload.exchanges{subject="kubernetes",outcome="issued",reason="issued"}` rises with
   each exchange. Every series of the closed reason set exists at 0 from the start, so a missing series means the
   exchange is off, not that no refusal happened.
3. Each issued token writes the audit event `auth.workload.token_issued`. The row names the binding
   (`binding_id`), the audience and the actions.

A workload token lives at most 300 seconds, and never longer than the presented token has left.

## Troubleshooting by reason

Find the log line `workload exchange refused` (Warn) or `workload exchange unavailable` (Error), or the counter
series, for the failing workload. Read its `subject`, `outcome` and `reason`. Look the reason up in the table. The
table lists every reason of the closed set, and a docs test keeps it equal to `internal/auth/workloadexchange/reasons.go`.

`audience` in the line is `auth-service`, `dev-health-api`, or `none` for any other requested value.

| Subject | Outcome | Reason | Meaning | Check |
| --- | --- | --- | --- | --- |
| kubernetes | refused | `subject_token_invalid` | The API server did not vouch for the presented token. | The token audience is `dev-health-workload-exchange`; the token is not expired; the ServiceAccount and its pod still exist; the CA file is the cluster's CA. |
| kubernetes | refused | `unknown` | No binding matches the reviewed identity. | `-trust-domain`, `-issuer`, `-namespace` and `-service-account` of the `bind` equal the cluster's values; run `bind` if none exists. |
| kubernetes | refused | `disabled` | The workload row, its principal or its binding is disabled. | Find out why it was disabled. The `enable` verb clears the workload row, and with `-enable-principal` its principal (see the bootstrap runbook). No verb enables a binding. |
| kubernetes | refused | `uid_mismatch` | The binding pins a UID that differs from the ServiceAccount's. | The ServiceAccount was created again with a new UID. No verb changes the UID of a binding: ask the registry owner. |
| kubernetes | refused | `no_audience` | The workload row has no audience. | The row is a registry identity only. Check the audiences in the registry page and run `seed`. |
| kubernetes | refused | `store_error` | The registry read failed. | The auth-service log line `workload: resolve store failure`; the `postgres` readiness check; the database login. |
| kubernetes | refused | `resolve_refused` | The registry named a reason outside the closed set, or returned a row it cannot narrow. | auth-service and auth-migrate run the same build. Read the log line `workload: resolve refused`. |
| kubernetes | refused | `audience_refused` | The requested audience is not a workload audience, or not in the workload's row. | The `audience` the caller sends; the audiences of the row in the registry page. |
| kubernetes | refused | `scope_refused` | The requested actions are outside the row's allowed actions. | Every row has no allowed action today, so any requested action is refused. Ask for none. |
| kubernetes | refused | `lifetime_too_short` | The presented token has less than one second left. | The token rotation of the projected volume; the node clock. |
| kubernetes | unavailable | `tokenreview_unavailable` | The call to the API server failed. | The network policy egress (`egress.kubeAPI`); the ClusterRoleBinding of the TokenReview role; the CA file; the reviewer token file; the API server health. |
| kubernetes | unavailable | `saturated` | 16 TokenReview calls were already in flight. | The API server latency (a call is cut at 5 seconds); the rate of the callers. Retry. |
| kubernetes | unavailable | `signer_unavailable` | The signing key or its key set could not be read. | The `signing_key` readiness check; the key file mount. |
| kubernetes | unavailable | `audit_unavailable` | The audit row for the issued token could not be written. | The `postgres` readiness check; the audit store. |
| kubernetes | unavailable | `selfcheck_contract` | The minted token did not match the workload token contract. | A build mismatch. Report it with the request id of the log line. |
| kubernetes | unavailable | `selfcheck_unknown_kid` | The minted token names a key that the published key set does not hold. | The key set and the signing key id; a key rotation in progress. |
| kubernetes | unavailable | `selfcheck_signature` | The minted token did not verify under the published keys. | The signing key and the published key set belong together. |
| kubernetes | unavailable | `selfcheck_issuer` | The minted token's issuer is not the verifier's issuer. | `AUTH_TOKEN_ISSUER_URL` is the https issuer. |
| kubernetes | unavailable | `selfcheck_audience` | The minted token's audience did not verify. | A build mismatch. Report it with the request id. |
| kubernetes | unavailable | `selfcheck_not_yet_valid` | The minted token was not yet valid. | The clock of the auth-service node. |
| kubernetes | unavailable | `selfcheck_expired` | The minted token was already expired. | The clock of the auth-service node. |
| kubernetes | issued | `issued` | A workload token was issued. | None. |
| bootstrap | refused | `malformed` | The presented value does not have the shape of a bootstrap credential. | The mounted file holds the value that `bootstrap-create` wrote, with no edit and no newline added. |
| bootstrap | refused | `unknown` | No bootstrap credential has that digest. | The file belongs to this registry; the credential was created in this environment. |
| bootstrap | refused | `revoked` | The credential is revoked. | A rotation overlap ended, or a revoke or disable ran. Create or rotate a credential. |
| bootstrap | refused | `expired` | The credential is at or past its expiry. | Rotate before the 720 hour default expiry. |
| bootstrap | refused | `disabled` | The workload row or its principal is disabled. | Run the `enable` verb (see the bootstrap runbook). |
| bootstrap | refused | `no_audience` | The workload row has no audience. | Check the audiences in the registry page and run `seed`. |
| bootstrap | refused | `store_error` | The registry read failed. | The `postgres` readiness check; the database login. |
| bootstrap | refused | `resolve_refused` | The registry named a reason outside the closed set, or returned a row it cannot narrow. | auth-service and auth-migrate run the same build. |
| bootstrap | refused | `audience_refused` | The requested audience is not a workload audience, or not in the credential's audiences. | The `audience` the caller sends; the `-audience` of the credential; the row's audiences. |
| bootstrap | refused | `scope_refused` | The requested actions are outside the row's allowed actions. | Every row has no allowed action today. Ask for none. |
| bootstrap | refused | `lifetime_too_short` | The credential has less than one second left. | Rotate the credential. |
| bootstrap | unavailable | `signer_unavailable` | The signing key or its key set could not be read. | The `signing_key` readiness check. |
| bootstrap | unavailable | `audit_unavailable` | The audit row for the issued token could not be written. | The `postgres` readiness check. |
| bootstrap | unavailable | `selfcheck_contract` | The minted token did not match the workload token contract. | A build mismatch. Report it with the request id. |
| bootstrap | unavailable | `selfcheck_unknown_kid` | The minted token names a key that the published key set does not hold. | The key set and the signing key id. |
| bootstrap | unavailable | `selfcheck_signature` | The minted token did not verify under the published keys. | The signing key and the published key set belong together. |
| bootstrap | unavailable | `selfcheck_issuer` | The minted token's issuer is not the verifier's issuer. | `AUTH_TOKEN_ISSUER_URL` is the https issuer. |
| bootstrap | unavailable | `selfcheck_audience` | The minted token's audience did not verify. | A build mismatch. Report it with the request id. |
| bootstrap | unavailable | `selfcheck_not_yet_valid` | The minted token was not yet valid. | The clock of the auth-service node. |
| bootstrap | unavailable | `selfcheck_expired` | The minted token was already expired. | The clock of the auth-service node. |
| bootstrap | issued | `issued` | A workload token was issued. | None. |
| none | invalid | `invalid_request` | The request is malformed, or names a subject type that is off. | The form fields, the media type, and that `AUTH_WORKLOAD_EXCHANGE` or `AUTH_WORKLOAD_BOOTSTRAP` is `true` for the subject type. |

The registry's own Info line `workload: resolve refused` carries a finer cause than the exchange reason:
`unknown_tuple`, `workload_disabled`, `principal_disabled`, `binding_disabled`, `uid_mismatch`, `no_audience`,
`wrong_kind` or `bad_input`. A disabled workload row, principal and binding all read as `disabled` in the exchange;
the registry line tells them apart.

## Turn the exchange off

Set `authService.workloadExchange.enabled` to `false` and upgrade the release. The route unmounts (it stays mounted while `AUTH_WORKLOAD_BOOTSTRAP` is `true`), and the chart
removes the token-review ClusterRole and ClusterRoleBinding. Workload tokens that were issued stay valid for at
most 300 seconds. The registry rows and bindings are not changed.
