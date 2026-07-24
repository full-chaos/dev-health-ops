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
- GitHub Actions is the only preview and production deployment authority.
  Workers Builds is disabled and deployment workflows verify that state through
  API readback.
- Deployment workflows use exactly `wrangler@4.107.0` with compatibility date
  `2026-07-16`.
- Previews share one Cloudflare Access service-token policy. Production custom
  domains remain anonymously readable.
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

Deployment and rollback are intentionally workflow-owned: a local command may
assemble and test assets, but it cannot publish them. A workflow must fail when
trusted deployment credentials, required checks, artifacts, or deployment
outputs are absent.

## Traceability

The complete Linear scope, ownership, automated proofs, and completion actions
are maintained in [the unified documentation coverage matrix](../coverage-matrix.md).
