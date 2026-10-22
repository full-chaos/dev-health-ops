# Unified documentation delivery on Cloudflare Workers

**Status:** Accepted  
**Date:** 2026-07-16  
**Scope:** Public documentation and the separately hosted product demo

## Decision

Dev Health publishes one public documentation site at
`https://docs.fullchaos.dev` and one separate product demo at
`https://demo.fullchaos.dev`.

- `ops/docs` is the canonical public documentation source.
- Documentation and demo assets use Cloudflare Workers Static Assets.
- Cloudflare Workers Builds (worker `dev-health-docs`) builds and deploys production from `main`; GitHub Actions only builds, validates, and smoke-tests.
- Deployment workflows use exactly `wrangler@4.107.0` with compatibility date
  `2026-07-16`.
- Production custom domains remain anonymously readable.
- Documentation releases retain immutable, versioned artifacts. Deployments
  emit source SHA/ref, release version or `main`, a sorted content digest,
  preview URL, Worker version ID, deployment ID, and previous version ID.
- The legacy GitHub Pages surfaces remain redirect shims until the Workers
  routes, headers, redirects, and rollback path are proven.

## Consequences

The public site has one canonical home instead of repository- and
audience-specific duplicates. The demo remains a root-domain static export, so
it can change independently without becoming a documentation host. Cloudflare
Pages is not a production or preview path for either site.

Deployment is owned by Cloudflare Workers Builds, which builds and deploys
from `main`. Rollback is a Cloudflare dashboard step or `make docs:rollback`.
The GitHub workflow builds and validates the assets and can smoke-test the
live host; it holds no deployment credentials.

## Traceability

The complete Linear scope, ownership, automated proofs, and completion actions
are maintained in [the unified documentation coverage matrix](../coverage-matrix.md).
