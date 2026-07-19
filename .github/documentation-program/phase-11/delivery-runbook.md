# Documentation delivery runbook

**Worker:** `dev-health-docs`  
**Canonical domain:** `docs.fullchaos.dev`  
**Delivery:** Workers Static Assets, no Worker script  
**Linear:** CHAOS-3013 and CHAOS-3014

Repository code configures and validates the delivery mechanism. Account-level Cloudflare Access, API-token scope, and GitHub environment approvals must be reviewed in their respective control planes before production is enabled.

## Local development

### Prerequisites

Use Python 3.12 and a currently supported Node.js release. From a clean checkout:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-docs.txt
```

On PowerShell, activate the environment with `.venv\Scripts\Activate.ps1`.

### Fast authoring loop

Use MkDocs directly for content, navigation, layout, and theme work:

```bash
make docs:v2-serve
```

Open `http://127.0.0.1:8000`. This mode has live reload and is the normal authoring loop. It does not emulate Cloudflare redirects, headers, indexing controls, or static-asset routing.

The equivalent command without Make is:

```bash
python -m mkdocs serve \
  --strict \
  --config-file mkdocs.prototype.yml \
  --dev-addr 127.0.0.1:8000
```

### Full reader-critical validation

Run the same deterministic gate used by documentation CI:

```bash
make docs:v2-check
```

This validates the publication inventory and IA, performs a strict build, checks rendered links and assets, runs task-language search acceptance, audits structural accessibility invariants, and verifies selected canonical facts.

### Cloudflare-shaped local preview

Build and prepare the exact preview asset tree, then serve it through the local Workers runtime:

```bash
make docs:cloudflare-dev
```

Open `http://localhost:8787`. The target first creates `.build/docs-cloudflare` with preview redirects, security headers, `noindex`, `robots.txt`, and a source-revision manifest, then runs the pinned Wrangler version against `wrangler.jsonc`.

The equivalent commands without Make are:

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

Local development does not require the remote `dev-health-docs` Worker to exist. Static assets are served from the local `.build/docs-cloudflare` directory.

## Automatic Worker recreation and connection

`wrangler.jsonc` is the source of truth for the Worker identity, static assets, and custom domain. Do not manually create a replacement Worker or manually attach `docs.fullchaos.dev`.

The first approved production command:

```bash
npx --yes wrangler@4.112.0 deploy --config wrangler.jsonc --strict
```

will:

1. create `dev-health-docs` automatically when the Worker does not exist;
2. upload the prepared Workers Static Assets version and make it active;
3. apply the `docs.fullchaos.dev` custom domain declared in `wrangler.jsonc`; and
4. let Cloudflare create the required DNS record and certificate for that custom domain.

Deleting the former Worker removed its versions, deployment history, preview aliases, and account-side policies. The deployment recreates the same logical Worker name, not the deleted resource history.

Because `wrangler deploy` both recreates the Worker and activates the custom domain, run it only through the protected production workflow after the Phase 12 go/no-go decision. Local authoring, validation, and GitHub build artifacts do not require this bootstrap deployment.

## One-time account setup

### 1. Create a least-privilege Cloudflare token

Create a dedicated token for documentation delivery. Limit it to the Full Chaos account, the `dev-health-docs` Worker, and the `fullchaos.dev` zone operations required for Worker versions, deployments, routes, and the custom domain. Do not reuse a broad personal token.

Store these secrets for the repository or the protected `docs-production` environment:

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

### 2. Configure GitHub controls

Create or review the `docs-production` environment:

- require a human reviewer;
- prohibit self-review where supported;
- restrict deployment branches to `main`;
- store production Cloudflare secrets in the environment rather than broad repository scope where possible.

Create these variables with a default value of `false`:

- `DOCS_CLOUDFLARE_PREVIEWS_ENABLED`
- `DOCS_CLOUDFLARE_PRODUCTION_ENABLED`

Set the production variable to `true` only for an approved deployment. Keep the preview variable `false` until the recreated Worker exists and Access has been verified.

### 3. Run the approved bootstrap deployment

Use **Documentation Cloudflare delivery** → **Run workflow** with:

- action: `deploy`
- confirmation: `docs.fullchaos.dev`

The protected workflow runs the complete quality gate, prepares production assets, recreates `dev-health-docs` when absent, connects the custom domain, verifies the host, and records the resulting Worker version.

### 4. Protect Worker preview URLs

After the Worker exists, in the Cloudflare dashboard:

1. Open **Workers & Pages** → `dev-health-docs` → **Settings** → **Domains & Routes**.
2. Enable **Preview URLs**.
3. Enable Cloudflare Access for preview URLs.
4. Restrict the Access policy to the documentation reviewers or approved Full Chaos identity domain.
5. Verify an anonymous browser is denied and an approved reviewer can sign in.
6. Set `DOCS_CLOUDFLARE_PREVIEWS_ENABLED=true` only after that verification passes.

Preview URLs are public when enabled without Access. The generated preview also sends `X-Robots-Tag: noindex, nofollow` and a disallowing `robots.txt`, but indexing controls are not authorization.

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

`_redirects` does not perform a host-level redirect from an earlier `dev-health-docs.fullchaos.workers.dev` deployment. Because the old Worker was deleted, verify that no separate old hostname remains live or indexable before canonical launch.

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
| Custom domain fails | Verify zone ownership, conflicting DNS records, Worker configuration, certificate state, and token permissions before retrying. |

## Evidence required to close Phase 11

- accepted ADR;
- green preview/build workflow;
- Access denial and approved-user evidence;
- least-privilege token review;
- `docs-production` approval configuration evidence;
- successful Worker recreation and production version deployment through Wrangler;
- custom-domain and certificate verification;
- representative redirects and headers verified on the target host;
- rollback rehearsal with version IDs and elapsed recovery time;
- application help links checked against the canonical domain;
- no production activation before Phase 12 go/no-go.