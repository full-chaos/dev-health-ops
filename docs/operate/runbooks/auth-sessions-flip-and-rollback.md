---
page_id: op-rb-auth-sessions-flip
summary: Roll the chart NetworkPolicies, then switch login and sessions to auth-service, read back each step, and roll back.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth sessions flip and rollback

This runbook covers two prod rolls, in this order: the NetworkPolicy roll (roll A), then the login flip (roll B). Each roll changes one thing. A NetworkPolicy fault and a sessions fault both show as failed logins and 5xx answers, so they never share a roll. Roll B starts only after roll A has run clean in prod.

Roll B also needs the prod ClickHouse to work and the owner's go. Each roll goes through the roll kit: "N START?", then the pm confirm, then the roll lane runs the kit step (`<kit step: roll A>`, `<kit step: roll B>`; exact step names are placeholders until the roll kit names them).

## Rules for every step

- Read prod only through `helm history`, `helm status`, pod and hook Job logs, and a browser.
- Never run `kubectl exec`. Never run `kubectl get secret` or `kubectl describe secret` in any form. Never read process arguments with a full-argv `ps`.
- A Secret value never appears in chat, a command argument, a log, a ticket or shell history. Secret creation uses the create-only scripts named below, which print Secret and key names only.
- A failed read that could print a value is stopped, not retried.
- Roll commands run through the deploy wrapper and the roll kit. This page gives no prod context or values path.

## States

- **Unflipped (dark):** the auth service runs, no ingress path reaches it, and every `goApi.authProxy.*` flag is `false`. The auth service mounts two routes (the JWKS document); discovery is off; the OIDC, OAuth, SAML and social-link routes answer 404. "Routes: 0" is not the dark check; the check is ingress paths = 0 plus the proxy flags off.
- **Flipped (sessions):** `goApi.authProxy.sessions=true`. Login and session routes go to auth-service. `localLogin`, `credentials` and `social` stay `false` in this roll.

## Roll A: NetworkPolicy roll

Roll A sets `networkPolicy.enabled=true` and `goWorkers.pgbouncer.postgres.networkPolicyCIDR`. It changes no auth flag.

| Step | Action | Source |
|---|---|---|
| A1 | Render the prod values with and without `networkPolicy.enabled`. Diff the output. List every NetworkPolicy the switch adds. | chart `templates/networkpolicy.yaml` |
| A2 | Set `goWorkers.pgbouncer.postgres.networkPolicyCIDR` from the deploy sources. Use a narrow CIDR. Read no Secret. The render refuses an external Postgres without it. | chart values `goWorkers.pgbouncer.postgres.networkPolicyCIDR` |
| A3 | Rehearse the real chart on the k3s dev venue. Every Deployment must still reach Postgres, ClickHouse Cloud egress, Valkey and the OTel collector; acr must reach FalkorDB. `networkPolicy.ingressControllerNamespace` must differ from the release namespace. Result: `<link>` (gate; fill after the venue reports). | CHAOS-10949 |
| A4 | Canary before the full roll. Check each policy-gated path: api to Postgres, api to Valkey, workers to ClickHouse Cloud egress, acr to FalkorDB. A refused path means STOP, report, and roll back (A7). | roll kit |
| A5 | Run the kit step `<kit step: roll A>`. | roll kit |
| A6 | Readback (below). | |
| A7 | Rollback: `helm rollback` to the previous revision. Name the revision from `helm history`: the revision before the A5 upgrade. Write it in the roll record before A5. | CHAOS-3270 |

Known limit: the release-wide policy admits every pod of the release namespace, so inside the namespace the policy is the only bound.

Roll A readback:

1. `helm status` shows the new revision deployed. Every Deployment is fully Ready with 0 restarts.
2. Pod logs for api, workers and acr show no connection-refused or timeout lines for 15 minutes.
3. Health and meta answer 200. Auth is still dark.
4. Load the admin pages in a browser against the prod URL with the readback credential held by the roll lane. The console shows no error. API answers alone are not a readback.

## Roll B: login flip

### Gates

- Roll A has run clean in prod.
- Prod ClickHouse works.
- The owner has given the go for the sessions flip; the sessions-flip leaf tickets are merged.
- The previous helm revision number is written in the roll record.

### B1 Valkey Secret (REQUIRED, script TBD)

Auth-service rate limits use the in-chart Valkey. The Secret holds `AUTH_VALKEY_URI` (chart `authService.valkey.secretName` and `secretKey`). The Secret must exist before `authService.valkey.secretName` is set.

The create-only host script for this Secret is not yet written; the runbook names it once it is filed. Required behaviour:

- Create only. Refuse if the Secret exists.
- Take key names from the chart sources.
- Put no value in a command argument, a log or shell history (read it from stdin or a file descriptor).
- One key per call.
- pm reviews the script sha before the roll, as for the key-set script.

### B2 Valkey ACL file

The ACL file lives in the same Secret under the key `users.acl` (chart `valkey.acl.secretName` and `secretKey`). It defines every user: the default user line and the `auth-service-ratelimit` user with a hashed password. Create the Secret first, then set `valkey.acl.enabled=true`. The chart refuses `valkey.acl.enabled=true` unless `networkPolicy.enabled=true`, which is why roll A comes first. The Valkey Deployment restarts once. To rotate, restart Valkey and auth-service; run no `kubectl exec`.

Known limit: the default Valkey user can still reach the rate-limit keys; the network policy is the bound.

### B3 Key Secrets

Create the new key-set Secrets with `scripts/create-auth-keyset-secrets.sh`, approved by pm for this roll only (script sha256 `befa4a8d077e890490e63b0c4022334210fd067c77ac0b52351d7991330a6e03`).

- Run one `--key` per call. A multi-key run that fails on the second key leaves the first created, and a rerun then stops on it.
- The script creates only. An existing Secret stops the run.
- A classifier denial goes to the owner as is.
- Any change to the script is a new sha and a new pm review.

The key-set names and key ids sit in the deploy prod values under `authService.keySet.keys`. The existing single-key Secret stays the active entry.

### B4 Roll

Run the kit step `<kit step: roll B>` with:

- `goApi.authProxy.sessions=true`
- `authService.sessionKeys` and `authService.sessionKeys.sessionReaders.goApi` and `.queryApi` set to the two reader logins (the render fails while either is empty)
- `authService.valkey.secretName` set to the B1 Secret
- `valkey.acl.enabled=true` with the B2 Secret

Other `goApi.authProxy.*` flags stay `false`.

### B5 Readback

1. `helm history` shows the new revision deployed; `helm status` is clean.
2. The auth-migrate and grant hook Job logs show success. Read no database directly.
3. The go-api log line "auth proxy routing" shows sessions on.
4. Auth-service pods are Ready, and the log shows no rate-limit store error.
5. 15 minutes of pod logs show 0 errors and 0 go-api 5xx.
6. In a browser on the prod URL: sign in, load a page that needs the session, sign out. Then load the admin pages. The console shows no error.

If B1 to B3 are done and B4 is not, login is still unflipped.

### B6 What users see

E-mail verify and reset links that go-api issued before the flip no longer work. Auth-service accepts only the link credentials it issued itself. The user asks for a new link. See the end-user troubleshooting page.

### B7 Rollback of the flip

Primary: run `helm upgrade` with `goApi.authProxy.sessions=false`. This always works: the grant hook runs on every upgrade and restores the grants (15 s in the rehearsal).

Use `helm rollback` only to a revision rendered with the chart that carries the pre-rollback grant hook (CHAOS-10719). That chart re-grants on rollback, because the grant hook also runs before a rollback. A rollback to an older revision does not re-grant: the go-api pod refuses to start and the rollback times out.

Fallback: if the target revision is older than that chart, or the grant hook refuses (a River schema newer than the target image knows), run `helm rollback --no-hooks`, then `helm upgrade` with `goApi.authProxy.sessions=false`. With `riverMigrate.enabled=false` a rollback re-grants nothing.

The roll that carries the CHAOS-10719 chart must land before the baseline revision of roll B.

Name the baseline revision from `helm history`: the revision before the B4 upgrade. After a rollback, links issued by auth-service during the flip stop working; users ask again.
