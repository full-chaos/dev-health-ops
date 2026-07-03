# CI/CD examples

Runnable, copy-paste examples for pushing batches from common automation
environments. Every example follows the same **validate → push → poll** shape
covered in [Examples & Quickstart](examples.md), so a malformed batch fails the
job before it reaches the ingest API, and the job only succeeds once the batch
reaches a terminal `completed` / `partial` status.

Each example is available in two flavours: the `dev-hops push` CLI (less code)
and raw `curl` + `jq` (no dev-hops dependency). The full runnable files live in
[`examples/customer-push/`](https://github.com/full-chaos/dev-health-ops/tree/main/examples/customer-push).

## Security requirements

!!! note "Required environment for the `dev-hops` CLI examples"
    `dev-hops push batch` / `push status` require **`FULLCHAOS_API_URL`**,
    **`FULLCHAOS_INGEST_TOKEN`**, and **`FULLCHAOS_ORG_ID`** (flag or env) — the command
    exits `2` with a usage error if any is unset. Store the token as a secret and the org id
    as a non-secret variable. The raw-`curl` examples don't need `FULLCHAOS_ORG_ID` (the
    token already binds the org server-side).

These apply to **every** example below — they are not optional:

- **Store the ingest token in the platform's secret store**, never in the repo:
  GitHub Actions → repository/organization **Secrets**; GitLab CI → **masked +
  protected** CI/CD variables; systemd/cron → an `EnvironmentFile` at mode `0600`
  owned by the service user.
- **Use a least-privilege token.** A pushing job needs only the `ingest:write`
  scope (plus `schema:read` for the pre-flight limits check and `ingest:status`
  for polling). Do not reuse a broad admin token.
- **Never send provider credentials to FullChaos.** Customer push means *your*
  automation reads *your* systems and pushes derived records. Your GitHub/GitLab
  PATs, Jira tokens, etc. stay in your environment — the only credential that
  ever reaches FullChaos is the `fcpush_…` ingest token.
- **Rotate tokens regularly.** Mint a new token via the sources/tokens API,
  update the secret, then revoke the old one — overlap the two during cutover so
  in-flight jobs don't fail. See the [Setup Guide](setup-guide.md).

## GitHub Actions

Copy into your repository as `.github/workflows/fullchaos-push.yml`. Set
`FULLCHAOS_INGEST_TOKEN` as a repository secret.

=== "dev-hops CLI"

    ```yaml
    --8<-- "github-actions.yml"
    ```

=== "curl"

    ```yaml
    --8<-- "github-actions-curl.yml"
    ```

## GitLab CI

Copy into your project as `.gitlab-ci.yml`. Set `FULLCHAOS_INGEST_TOKEN` as a
**masked + protected** CI/CD variable.

=== "dev-hops CLI"

    ```yaml
    --8<-- "gitlab-ci.yml"
    ```

=== "curl"

    ```yaml
    --8<-- "gitlab-ci-curl.yml"
    ```

## Generic runner

No GitHub/GitLab assumptions — a portable POSIX `sh` script that runs in any
Docker container, Jenkins shell step, or on a laptop. Requires only `curl` and
`jq`. Pass the batch envelope as the first argument:

```bash
FULLCHAOS_API_URL=https://app.fullchaos.example \
FULLCHAOS_INGEST_TOKEN=fcpush_… \
./generic-runner.sh batch.json
```

```sh
--8<-- "generic-runner.sh"
```

## cron / systemd (self-hosted)

For self-hosted customers pushing on a schedule without a CI platform. The
service runs the generic runner; the timer drives it hourly. Keep the token in
`/etc/fullchaos/push.env` at mode `0600`.

=== "systemd service"

    ```ini
    --8<-- "fullchaos-push.service"
    ```

=== "systemd timer"

    ```ini
    --8<-- "fullchaos-push.timer"
    ```

The timer file also documents the equivalent plain-`crontab` line for hosts
without systemd.
