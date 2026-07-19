# Documentation delivery runbook

**Worker:** `dev-health-docs`  
**Canonical domain:** `docs.fullchaos.dev`  
**Delivery:** Workers Static Assets, no Worker script  
**Linear:** CHAOS-3013 and CHAOS-3014

Repository code configures and validates the delivery mechanism. Account-level Cloudflare Access, DNS, API-token scope, and GitHub environment approvals must be reviewed in their respective control planes before production is enabled.

## Local development

The Wrangler configuration serves `.build/docs-cloudflare`. That directory is generated output and does not exist in a clean checkout, so running `wrangler dev` directly before a build fails by design.

Install the documentation dependencies once:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-docs.txt
```

For the normal content and style authoring loop, run MkDocs directly:

```bash
make docs:v2-serve
```

Open `http://127.0.0.1:8000`. This mode has live reload and is the fastest way to review content, navigation, layout, and theme changes.

To build the strict candidate and run the reader-critical documentation checks:

```bash
make docs:v2-check
```

To prepare the exact preview asset tree and serve it through the local Workers runtime:

```bash
make docs:cloudflare-dev
```

Open `http://localhost:8787`. The target performs the strict build and checks, creates `.build/docs-cloudflare` with preview redirects, headers, `noindex`, `robots.txt`, and source metadata, then starts the pinned Wrangler version.

The raw equivalent is:

```bash
python scripts/validate_docs_v2_publication.py
python -m mkdocs build --strict --config-file mkdocs.prototype.yml
python scripts/check_built_site_links.py --site-dir .build/docs-prototype
python scripts/check_docs_candidate_search.py \
  --site-dir .build/docs-prototype \
  --queries .github/documentation-program/phase-10/search-acceptance.json
python scripts/check_docs_candidate_accessibility.py \
  --site-dir .build/docs-prototype \
  --css docs-prototype/stylesheets/extra.css
python scripts/check_docs_candidate_facts.py
python scripts/prepare_docs_cloudflare.py \
  --source .build/docs-prototype \
  --output .build/docs-cloudflare \
  --mode preview \
  --redirects .github/documentation-program/phase-9/redirects.tsv \
  --source-revision "$(git rev-parse HEAD)"
npx --yes wrangler@4.112.0 dev --config wrangler.jsonc
```

Local development does not require the remote Worker to exist.

## One-time account setup

### 1. Protect Worker preview URLs

In the Cloudflare dashboard:

1. Open **Workers & Pages** → `dev-health-docs` → **Settings** → **Domains & Routes**.
2. Enable **Preview URLs**.
3. Enable Cloudflare Access for preview URLs.
4. Restrict the Access policy to the documentation reviewers or approved Full Chaos identity domain.
5. Verify an anonymous browser is denied and an approved reviewer can sign in.

Preview URLs are public when enabled without Access. The generated preview also sends `X-Robots-Tag: noindex, nofollow` and a disallowing `robots.txt`, but indexing controls are not authorization.

### 2. Create a least-privilege Cloudflare token

Create a dedicated token for documentation delivery. Limit it to the Full Chaos account, the `dev-health-docs` Worker, and the `fullchaos.dev` zone operations required for Worker versions, deployments, routes, and the custom domain. Do not reuse a broad personal token.

Store these secrets for the repository or the protected `docs-production` environment:

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

### 3. Configure GitHub controls

Create or review the `docs-production` environment:

- require a human reviewer;
- prohibit self-review where supported;
- restrict deployment branches to `main`;
- store production Cloudflare secrets in the environment rather than broad repository scope where possible.

Create these variables with a default value of `false`:

- `DOCS_CLOUDFLARE_PREVIEWS_ENABLED`
- `DOCS_CLOUDFLARE_PRODUCTION_ENABLED`

Set the preview variable to `true` only after Access is verified. Set the production variable to `true` only after the custom domain, token scope, Phase 10 gate, and Phase 12 go/no-go process are ready.

## Pull-request preview

For a same-repository pull request that changes documentation delivery inputs, the workflow:

1. runs the Phase 10 publication, strict-build, link, search, accessibility-structure, and fact checks;
2. prepares a preview asset directory with the approved redirects, security headers, `noindex`, and a disallowing `robots.txt`;
3. always uploads the prepared directory as a GitHub artifact;
4. when previews are enabled and secrets exist, runs `wrangler versions upload` with a stable `pr-<number>` alias;
5. comments on the pull request with the Access-protected Worker preview URL and version ID.

Fork pull requests never receive Cloudflare credentials or create Worker versions.

## Production deploy

Production is manual. Use **Documentation Cloudflare delivery** → **Run workflow** with:

- action: `deploy`
- confirmation: `docs.fullchaos.dev`

The workflow must run from the reviewed `main` revision. It repeats the reader-critical quality gate, prepares production assets without `noindex`, deploys the static Worker and custom domain, then verifies:

- `/`, `/use/`, and the Investment calculation reference return `200`;
- an intentional missing path returns `404`;
- the required response headers are present;
- production does not return `X-Robots-Tag: noindex`;
- a representative legacy path returns a permanent redirect to the approved canonical target.

Record the source commit, new version ID, prior production version ID, workflow run, smoke result, and approver in CHAOS-3014.

## Rollback rehearsal and execution

Before the first canonical cutover, rehearse rollback with a non-production version and record the evidence.

For an actual rollback, run the same workflow with:

- action: `rollback`
- version ID: the explicit known-good Worker version
- confirmation: `docs.fullchaos.dev`

The workflow invokes `wrangler rollback` and then repeats the canonical-host smoke check. A rollback changes the active Worker version at 100% traffic. This documentation Worker has no stateful bindings, so the rollback scope is static assets and configuration only.

Never use an unspecified implicit rollback target during an incident. Retain and verify the known-good version ID before each production deployment.

## Redirects and old-host retirement

The generated `_redirects` file contains the approved Phase 9 path migrations. The delivery build fails on duplicate or invalid path rules.

`_redirects` does not perform a host-level redirect from `dev-health-docs.fullchaos.workers.dev`. At cutover, choose one explicit action:

1. disable the old workers.dev production route after confirming it is only a WIP preview; or
2. retain a minimal host redirect only when measured inbound traffic or durable links justify it.

Do not leave the old WIP site independently indexable alongside `docs.fullchaos.dev`.

## Headers and caching

Generated security headers apply to all static responses:

- `X-Content-Type-Options: nosniff`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `X-Frame-Options: SAMEORIGIN`
- a restrictive browser permissions policy

Cloudflare supplies validation-based caching for ordinary static responses. Hashed theme assets under `/assets/` receive long immutable caching. Stable-name custom stylesheets are left on the default revalidation behavior.

A Content Security Policy is intentionally deferred until it is tested against MkDocs Material, search, code copy, Mermaid, and any analytics or feedback endpoint. Do not deploy an unverified restrictive policy merely to satisfy a checklist.

## Failure handling

| Failure | Response |
| --- | --- |
| Quality check fails | Do not upload or deploy. Fix the source or gate. |
| Cloudflare credential missing | Preview stays as a GitHub artifact; production fails closed. |
| Preview inaccessible to approved reviewer | Review Access policy, not documentation code. |
| Preview accessible anonymously | Disable preview uploads and fix Access immediately. |
| Deploy succeeds but smoke fails | Roll back to the recorded known-good version. |
| Redirect mismatch | Correct the Phase 9 manifest or generated output; do not hand-edit the deployed file. |
| Custom domain fails | Verify zone, Worker route, certificate, and DNS state before retrying. |
| Old preview remains indexed | Remove/redirect the old host and request re-crawl after canonical launch. |

## Evidence required to close Phase 11

- accepted ADR;
- green preview/build workflow;
- Access denial and approved-user evidence;
- least-privilege token review;
- `docs-production` approval configuration evidence;
- successful non-production Worker version upload;
- custom-domain and certificate verification;
- representative redirects and headers verified on the target host;
- rollback rehearsal with version IDs and elapsed recovery time;
- application help links checked against the canonical domain;
- no production activation before Phase 12 go/no-go.
