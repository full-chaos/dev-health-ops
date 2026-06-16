# GitHub App setup (frictionless install)

This guide creates the **Dev Health GitHub App** that powers one‑click "Connect
GitHub" (install the App instead of pasting a PAT) and unified GitHub sign‑in.
It is the operator counterpart to [GitHub App auth](github-app-auth.md) and the
plan in [`docs/plans/github-app-marketplace.md`](../plans/github-app-marketplace.md).

> Replace `YOUR_HOST` with your public base URL. In the Docker stack, web and the
> ops API sit behind the same host via Traefik. For purely local testing you do
> **not** need a public URL for the core flow — see
> [Local testing without a tunnel](#local-testing-without-a-tunnel).

## 1. Create the App

GitHub → **Settings → Developer settings → GitHub Apps → New GitHub App**.

### Identifying and authorizing users

| Field | Value |
| --- | --- |
| **User authorization callback URL** | `https://YOUR_HOST/admin/integrations/github-app/callback` |
| **Request user authorization (OAuth) during installation** | ✅ **Checked** |
| **Expire user authorization tokens** | ✅ leave checked (the code is used once at install) |
| **Enable Device Flow** | ⬜ unchecked |

GitHub Apps support up to 10 registered callback URLs. If you register more than
one (e.g. a localhost URL and a production URL), set `GITHUB_APP_CALLBACK_URL` to
the exact URL for the current environment so the install flow redirects
deterministically — otherwise GitHub uses the first registered callback URL.

Because OAuth‑during‑install is on, GitHub greys out **Setup URL** and uses the
callback URL above instead — that is expected; leave Setup URL blank.

### Post installation

- **Setup URL** — blank (unavailable).
- **Redirect on update** — ⬜ **leave unchecked.** The callback requires the
  signed `state` that only our "Connect" button supplies; GitHub's update‑redirect
  would omit it and land on `?github_app=error`. Repo changes are picked up by the
  scheduled sync.

### Webhook

| Field | Value |
| --- | --- |
| **Active** | ✅ (uncheck for [local testing](#local-testing-without-a-tunnel)) |
| **Webhook URL** | `https://YOUR_HOST/api/v1/webhooks/github` |
| **Secret** | a random value — generate with `openssl rand -hex 32` |

The same secret must be set as `GITHUB_WEBHOOK_SECRET` on the ops backend; the
`X-Hub-Signature-256` check rejects deliveries that don't match.

### Permissions — all **Read‑only** (this App never writes)

**Repository**

| Permission | Access | Why |
| --- | --- | --- |
| Metadata | Read‑only | Mandatory baseline |
| Contents | Read‑only | Commit history, blame, complexity |
| Pull requests | Read‑only | Review flow, cycle time, rework |
| Issues | Read‑only | Work items + incidents |
| Checks | Read‑only | CI check runs |
| Actions | Read‑only | Workflow runs (CI/CD, DORA) |
| Deployments | Read‑only | Delivery / change‑failure signals |
| Commit statuses | Read‑only | CI status (optional) |

**Organization**

| Permission | Access | Why |
| --- | --- | --- |
| Members | Read‑only | Group activity by team / identity mapping |

**Account** (granted per‑user via the OAuth flow)

| Permission | Access | Why |
| --- | --- | --- |
| Email addresses | Read‑only | Link the installer to their Dev Health account |

### Subscribe to events

Leave **Installation target / Meta / Security advisory** unchecked. The
`installation` event (install / uninstall / suspend / unsuspend) is delivered
automatically and is what the lifecycle handler consumes. (Optional: tick **Meta**
for a ping when the App is deleted.)

Do **not** subscribe to `push` / `pull_request` here — those drive the legacy
real‑time webhook‑sync path that expects a `GITHUB_TOKEN` PAT, not the
per‑installation App token. App installs rely on the scheduled sync.

### Where can this GitHub App be installed?

- **"Only on this account"** for internal testing.
- **"Any account"** when onboarding external orgs (also required for the deferred
  Marketplace listing, CHAOS‑2236).

Click **Create GitHub App**, then on the App page **generate a private key**
(downloads a `.pem`) and **generate a client secret** — neither is shown until you
generate it.

## 2. Wire the credentials

```bash
# ops backend
GITHUB_APP_SLUG=<app URL slug, e.g. dev-health>
GITHUB_APP_ID=<App ID>
GITHUB_APP_CLIENT_ID=<Client ID>
GITHUB_APP_CLIENT_SECRET=<generated client secret>
GITHUB_WEBHOOK_SECRET=<the webhook secret from step 1>
GITHUB_APP_CALLBACK_URL=https://YOUR_HOST/admin/integrations/github-app/callback
# private key — choose ONE (see below)
GITHUB_APP_PRIVATE_KEY_PATH=/run/secrets/github-app.pem
# GITHUB_APP_PRIVATE_KEY="<single-line PEM with \n escapes — see 'Providing the private key' below>"

# web (unified social login on the same App)
AUTH_GITHUB_ID=<same Client ID>
AUTH_GITHUB_SECRET=<same client secret>
```

### Providing the private key (PEM)

The PEM is multi‑line, which env files handle inconsistently. Two supported ways:

**A. File path (recommended).** Mount the `.pem` and point at it. No escaping:

```bash
GITHUB_APP_PRIVATE_KEY_PATH=/run/secrets/github-app.pem
```

**B. Inline `GITHUB_APP_PRIVATE_KEY`.** The value may be a single line with
escaped `\n` (what `.env` files, Docker env, and most secret managers store);
the backend converts `\n` back to real newlines, and a PEM that already has real
newlines is left unchanged. Produce the one‑liner with:

```bash
# turns the multi-line .pem into a single \n-escaped line
awk 'NF {printf "%s\\n", $0}' github-app.pem
```

Then in the env file (quote it):

```dotenv
GITHUB_APP_PRIVATE_KEY="<paste the \n-escaped one-liner from the awk command above>"
```

> Docker Compose `env_file` does not interpret quotes or multi‑line values, so the
> `\n`‑escaped single line is the form to use there. Alternatively inject it from a
> shell that preserves real newlines: `export GITHUB_APP_PRIVATE_KEY="$(cat github-app.pem)"`.

## 3. Connect

In the app: **Admin → Integrations → GitHub → "Connect GitHub App"**. You're sent
to GitHub to install + pick repositories, then returned connected — no tokens to
paste. The callback verifies the installation against `GET /user/installations`
(installer must have access), links it to your org, and writes the App‑mode
credential the sync pipeline uses.

## Local testing without a tunnel

Only the **webhook** needs a publicly reachable URL (GitHub POSTs to it). The
install + OAuth callback works on `localhost` because GitHub redirects the
**browser** (which is on your machine), and the backend's OAuth/`/user/installations`
calls are outbound:

- Set **User authorization callback URL** = `http://localhost:3000/admin/integrations/github-app/callback`.
- Set `GITHUB_APP_CALLBACK_URL=http://localhost:3000/admin/integrations/github-app/callback` so
  the install flow returns to localhost even if a production callback URL is also registered.
- In **Webhook**, **uncheck "Active"** (drops the required URL). You lose only
  automatic credential‑deactivation on uninstall during the test.

The full Connect → install → credential → sync happy path is testable this way.

Reach for a tunnel (e.g. `cloudflared`) **only** to exercise the webhook lifecycle:

```bash
cloudflared tunnel --url http://localhost:8000   # ops API
# Webhook URL = https://<tunnel-host>/api/v1/webhooks/github  (+ matching GITHUB_WEBHOOK_SECRET)
```

The callback can stay on `localhost`, or move it to the tunnel host too.
