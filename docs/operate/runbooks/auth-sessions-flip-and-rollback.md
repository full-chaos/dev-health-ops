---
page_id: op-rb-auth-sessions-flip
summary: Roll the chart NetworkPolicies, then switch the session routes to auth-service, read back each step, and roll back.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth sessions flip and rollback

This runbook covers two prod rolls, in this order: the NetworkPolicy roll (roll A), then the sessions flip (roll B, the first prod auth flip). Each roll changes one thing. A NetworkPolicy fault and a sessions fault both show as failed logins and 5xx answers, so they never share a roll. Roll B starts only after roll A has run clean in prod.

Roll B also needs the prod ClickHouse to work and the owner's go. Each roll goes through the roll kit: "N START?", then the pm confirm, then the roll lane runs the kit step (`<chaos roll step: NetworkPolicy roll>`, `<chaos roll step: sessions-flip roll>`; the roll plans by chaos are not written yet, so these names are placeholders). The kit phases are `pre`, `groups`, `drain`, `prep`, `refresh`, `deploy`, `stagecheck` and `rollback-ops`.

## Rules for every step

- Read prod only through `helm history`, `helm status`, pod and hook Job logs, and a browser.
- Never run `kubectl exec`. Never run `kubectl get secret` or `kubectl describe secret` in any form. Never read process arguments with a full-argv `ps`.
- A Secret value never appears in chat, a command argument, a log, a ticket or shell history. Secret creation uses the create-only scripts named below, which print Secret and key names only.
- A failed read that could print a value is stopped, not retried.
- Roll commands run through `<deploy wrapper>` and the roll kit. This page gives no prod context or values path.

## Status of the prerequisites

The chart keys in B1 to B4 that come from open leaves are not on ops main yet: CHAOS-10684 (`authService.valkey`), CHAOS-10921 (`valkey.acl`) and CHAOS-10635 with CHAOS-10907 (`authService.keySet`). A render of main does not accept them. This page names them as written in those leaves. Check each key against the chart at the roll cut before the roll, and confirm that every leaf has merged. The session reader and successor-key values in B4 are on main today.

## States

- **Unflipped (dark):** the auth service runs, no ingress path reaches it, and every `goApi.authProxy.*` flag is `false`. The auth service mounts two routes (the JWKS document); discovery is off; the OIDC, OAuth, SAML and social-link routes answer 404. "Routes: 0" is not the dark check; the check is ingress paths = 0 plus the proxy flags off.
- **Flipped (sessions):** `goApi.authProxy.sessions=true`. The session routes (`POST /api/v1/auth/refresh` and `POST /api/v1/auth/logout`) go to auth-service. Login (`localLogin`), register and e-mail link routes (`credentials`) and `social` stay `false`: go-api still serves them in this roll. Each of those is its own later flip, one per roll.

## Roll A: NetworkPolicy roll

Roll A sets `networkPolicy.enabled=true` and `goWorkers.pgbouncer.postgres.networkPolicyCIDR`. It changes no auth flag.

| Step | Action | Source |
|---|---|---|
| A1 | Render the prod values with and without `networkPolicy.enabled`. Diff the output. List every NetworkPolicy the switch adds. | chart `templates/networkpolicy.yaml` |
| A2 | Set `goWorkers.pgbouncer.postgres.networkPolicyCIDR` from the deploy sources. Use a narrow CIDR. Read no Secret. The render refuses an external Postgres without it. | chart values `goWorkers.pgbouncer.postgres.networkPolicyCIDR` |
| A3 | Rehearse the real chart on the k3s dev venue. Every Deployment must still reach Postgres, ClickHouse Cloud egress, Valkey and the OTel collector; acr must reach FalkorDB. `networkPolicy.ingressControllerNamespace` must differ from the release namespace. Result: `<link>` (gate; fill after the venue reports). | CHAOS-10949 |
| A4 | Canary before the full roll. Check each policy-gated path: api to Postgres, api to Valkey, workers to ClickHouse Cloud egress, acr to FalkorDB. A refused path means STOP, report, and roll back (A7). | roll kit |
| A5 | Run the kit step `<chaos roll step: NetworkPolicy roll>`. | roll kit |
| A6 | Readback (below). | |
| A7 | Rollback: the previous helm revision, through the roll-kit phase `rollback-ops`. Name the revision from `helm history`: the revision before the A5 upgrade. Write it in the roll record before A5. | CHAOS-3270 |

Known limit: the release-wide policy admits every pod of the release namespace, so inside the namespace the policy is the only bound.

Roll A readback:

1. `helm status` shows the new revision deployed. Every Deployment is fully Ready with 0 restarts.
2. Pod logs for api, workers and acr show no connection-refused or timeout lines for 15 minutes.
3. Health and meta answer 200. Auth is still dark.
4. Load the admin pages in a browser against the prod URL with the readback credential held by the roll lane. The console shows no error. API answers alone are not a readback.

## Roll B: sessions flip

### Gates

- Roll A has run clean in prod.
- Prod ClickHouse works.
- The owner has given the go for the sessions flip; the sessions-flip leaf tickets are merged.
- The rollback rehearsal is done: kind rehearsal of the flip and its rollback, record run https://github.com/full-chaos/dev-health-ops/actions/runs/39862584421 (strict, 0 findings; CHAOS-10407). A `helm rollback` rehearsal on the CHAOS-10719 chart is the second gate: result: `<link>`.
- The previous helm revision number is written in the roll record.

### B1 Valkey Secret

From CHAOS-10684, auth-service rate limits use the in-chart Valkey as a shared store; until that leaf merges each replica counts for itself. One Secret, `auth-valkey`, holds both keys: `AUTH_VALKEY_URI` (chart `authService.valkey.secretName`) and `users.acl` (chart `valkey.acl.secretName`). Both come from one generated password; the ACL file holds only its hash.

Create it with `SECRET-CREATE-10684.sh` (sha256 `581412198c5c02480c9a763537d35e8173a028a4cde9ec51f238996820c2be7b`, pm-approved under D6254 v2). The script:

- creates only and stops with a fixed message if the Secret exists;
- keeps the password only in owner-only temporary files that are removed on exit;
- runs one `kubectl create secret` command and nothing else: no exec, get, describe or apply;
- prints fixed text and key names only.

Any change to the script is a new sha and a new pm review. A classifier denial goes to the owner as is. Create the Secret before the B4 upgrade sets `authService.valkey.secretName`. To rotate, delete and recreate the Secret, then restart Valkey and auth-service.

### B2 Valkey ACL file

The ACL file is the `users.acl` key of the B1 Secret (chart `valkey.acl.secretName` and `secretKey`). It defines every user: the default user line and the `auth-service-ratelimit` user with a hashed password. Create the Secret first, then set `valkey.acl.enabled=true`. The chart refuses `valkey.acl.enabled=true` unless `networkPolicy.enabled=true`, which is why roll A comes first. The Valkey Deployment restarts once. A restart is the only way Valkey rereads the file; run no `kubectl exec`.

Known limit: the default Valkey user can still reach the rate-limit keys; the network policy is the bound.

### B3 Key Secrets

Create the new key-set Secrets with `scripts/create-auth-keyset-secrets.sh`, approved by pm for this roll only (script sha256 `befa4a8d077e890490e63b0c4022334210fd067c77ac0b52351d7991330a6e03`).

- Run one `--key` per call. A multi-key run that fails on the second key leaves the first created, and a rerun then stops on it.
- The script creates only. An existing Secret stops the run.
- A classifier denial goes to the owner as is.
- Any change to the script is a new sha and a new pm review.

The key-set names and key ids sit in the deploy prod values under `authService.keySet.keys`. The existing single-key Secret stays the active entry.

### B4 Roll

Run the roll step `<chaos roll step: sessions-flip roll>` with these values (chart keys on main):

- `goApi.authProxy.sessions=true`
- `authService.migrate.sessionReaders.goApi` and `authService.migrate.sessionReaders.queryApi`: the go-api login and the query-api registry login. The render fails while either is empty. They must be two distinct existing logins, and neither may be the auth-migrate login.
- `authService.sessionKeys.successor.secretName`, with `secretKey` `AUTH_SESSION_SUCCESSOR_KEY` (32 bytes, base64url without padding): the Secret must exist first. Its create-only host script is not named in any leaf yet: `<create-only script for the successor key: CHAOS-11031>`; it follows the B3 rules (one key per call, create only, no value in arguments).
- From the open leaves (B1 to B3): `authService.valkey.secretName`, `valkey.acl.enabled`, `valkey.acl.secretName`, and the `authService.keySet.keys` entries.

Other `goApi.authProxy.*` flags stay `false`. The access-token key is the Secret go-api already reads as `JWT_SECRET_KEY`; nothing sets it here.

### B5 Readback

1. `helm history` shows the new revision deployed; `helm status` is clean.
2. The auth-migrate and grant hook Job logs show success. Read no database directly.
3. The go-api log line "auth proxy routing" shows `sessions` true and `local_login` and `credentials` false. That line states the flags only; the session routes are proven by step 6.
4. Auth-service pods are Ready, and the log shows no rate-limit store error.
5. 15 minutes of pod logs show 0 errors and 0 go-api 5xx.
6. In a browser on the prod URL: sign in (go-api still serves login), load a page that needs the session, sign out (logout goes to auth-service). Then load the admin pages. The console shows no error.

If B1 to B3 are done and B4 is not, login is still unflipped.

### B6 What users see

Nothing changes at sign-in: login stays with go-api. A refresh token that go-api issued before the flip is exchanged once, at the user's first refresh after the flip, for an auth-service session; the user stays signed in. A logout that presents a go-api refresh token answers 200 and revokes nothing; the user is signed out only when the token expires.

E-mail verify and reset links change with the later `credentials` flip, not with this roll. After that flip, links that go-api issued before it no longer work, because auth-service accepts only the link credentials it issued itself; the user asks for a new link. State this in that flip's roll plan.

### B7 Rollback of the flip

Primary: run `helm upgrade <release> <chart> --namespace <namespace> --values <production values file> --set goApi.authProxy.sessions=false` (release, namespace and values come from the deploy sources and the roll kit; `<deploy wrapper>` is the sanctioned form). This always works: the grant hook runs on every upgrade and restores the grants (13 to 17 s in the rehearsal, record run above).

Use `helm rollback` only to a revision rendered with the chart that carries the pre-rollback grant hook (CHAOS-10719). That chart re-grants on rollback, because the grant hook also runs before a rollback. A rollback to an older revision does not re-grant: the go-api pod refuses to start and the rollback times out.

Fallback: if the target revision is older than that chart, or the grant hook refuses (a River schema newer than the target image knows), run `helm rollback --no-hooks`, then `helm upgrade` with `goApi.authProxy.sessions=false`. With `riverMigrate.enabled=false` a rollback re-grants nothing.

The roll that carries the CHAOS-10719 chart must land before the baseline revision of roll B.

Name the baseline revision from `helm history <release> --namespace <namespace>` (output lists revisions only): the revision before the B4 upgrade. 
