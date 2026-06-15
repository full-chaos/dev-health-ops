# GitLab Token Permissions & Scopes

This guide explains the GitLab access-token scopes, project roles, and project
features required for each sync target, and what happens when a permission is
missing.

> **TL;DR** — A GitLab token that lacks a scope/role (or a project that has a
> feature disabled) returns **HTTP 403 Forbidden**. As of the GitLab-403
> graceful-degrade change, an enrichment endpoint returning 403 **no longer
> aborts the whole sync** — the core git/MR data still syncs and the skipped
> target is recorded in the sync result. Fix the permission to restore the
> missing data.

## Token types

Any of the following work, in order of decreasing blast radius:

| Token type | Best for | Notes |
| --- | --- | --- |
| **Personal Access Token (PAT)** | A user syncing their own projects | Inherits the user's role on each project. |
| **Group Access Token** | Syncing many projects under one group | Assign the token a role at the group level. |
| **Project Access Token** | A single project | Most-scoped; create one per project. |

Set the token via `GITLAB_TOKEN` (env) or `--auth` / the sync-config
credential. For self-hosted GitLab, also set `--gitlab-url` /
`credentials.url`.

## Required scopes & roles per sync target

GitLab authorization is **two-dimensional**: the token needs the right **API
scope** *and* the token's identity needs a high-enough **project role**. Both
must be satisfied or the API returns 403.

| Sync target | Endpoint(s) | API scope | Min. project role | Project feature that must be enabled | On 403 |
| --- | --- | --- | --- | --- | --- |
| `git` (commits, blame) | `/repository/...` | `read_api`, `read_repository` | Reporter | Repository | **Sync fails** — core data |
| `prs` (merge requests, reviews) | `/merge_requests` | `read_api` | Reporter | Merge requests | **Sync fails** — core data |
| `cicd` (pipelines) | `/pipelines` | `read_api` | Reporter | CI/CD | Pipeline data skipped |
| `deployments` | `/deployments`, `/releases` | `read_api` | Reporter | Environments / Releases | **Degrades** — recorded empty, sync continues |
| `feature-flags` | `/feature_flags` | `api` | **Developer** | Feature Flags | **Degrades** — recorded `skipped: forbidden`, sync continues |

Notes:

- **Feature flags are the strictest.** GitLab's Feature Flags API requires the
  **Developer** role (not Reporter) and the broader **`api`** scope (not just
  `read_api`). The Feature Flags project feature must also be enabled
  (*Settings → General → Visibility, project features → Feature flags*). A
  read-only `read_api` token, or a Reporter, will get 403 here even if the
  rest of the sync works.
- **Deployments** need the project's **Environments** (and optionally
  **Releases**) features enabled. A project that never deploys through GitLab
  environments will legitimately return no deployments.
- `read_api` is sufficient for everything except feature flags; prefer it over
  the full `api` scope unless feature-flag sync is required.

## How to set permissions

1. **Create/scope the token**
   - *User/Group/Project* → **Settings → Access Tokens**.
   - Select scope: `read_api` + `read_repository` for most targets; add `api`
     if you sync feature flags.
   - Set the token's **role** to **Developer** if feature-flag sync is needed,
     otherwise **Reporter** is enough.
2. **Enable the project features** you sync (Feature Flags, Environments) under
   *Settings → General → Visibility, project features*.
3. Re-run the sync. The previously-skipped targets should populate.

## Behavior when a permission is missing

A non-transient GitLab **403** (feature disabled or insufficient role/scope) is
**not retried** — GitLab signals rate limits with **429**, never 403, so
retrying a 403 only wastes calls. Instead:

- **`feature-flags`**: the connector raises a non-retryable
  `AuthenticationException`; the sync runtime records
  `{"status": "skipped", "reason": "forbidden", "detail": ...}` under
  `feature_flags` in the sync result and continues.
- **`deployments`**: already failure-soft — the deployment fetch logs the error
  and returns an empty list, so the rest of the sync proceeds.
- **`git` / `prs`**: these are core targets; a 403 there still fails the sync
  (with the underlying GitLab message) because there is no useful sync without
  them.

Check the sync result / worker logs for `skipped`/`forbidden` markers to see
which enrichment data was withheld and why.
