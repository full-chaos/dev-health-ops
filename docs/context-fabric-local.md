# Test Context Fabric locally

Context Fabric, also called the Agent Context Runtime (ACR), is a separate
private service that reads Dev Health evidence and organization entitlements
from `dev-health-ops`. It is not part of the default open-source/self-hosted Ops
Compose stack.

Use the isolated developer fixture when you need to test the hosted ACR API,
the host-local MCP sidecar, and one of the bundled OpenCode, Claude Code, Codex,
or Cursor packages together.

## Why the fixture is separate

The normal `compose.yml` is optimized for Ops development and exposes
ClickHouse without transport TLS. ACR deliberately requires certificate-
verified ClickHouse and HTTPS service boundaries. Adding the ACR containers to
the normal project would either weaken that contract or require every Ops
contributor to run private ACR infrastructure.

The local launcher instead creates a unique `acr-e2e-*` Compose project with
its own containers, volumes, network, databases, entitlement, credentials, and
seed data. It builds the current Ops checkout, but it does not join or mutate a
running `dev-health` project.

## Checkout layout

Keep the repositories as siblings for the zero-configuration path:

```text
workspace/
  dev-health-ops/
  dev-health-acr/
  dev-health-web/
```

Developers without access to the private `dev-health-acr` repository can use
the normal Ops and Web development paths but cannot start this fixture.

## Prerequisites

- Docker Engine or Docker Desktop with Compose v2 and Buildx
- Go 1.25 or newer
- `openssl`, `curl`, `git`, `jq`, `python3`, and `pgrep`
- at least one supported client CLI for interactive plugin testing

Node.js and pnpm are required only for the separate Web harness.

## Start from Ops

```bash
bash scripts/context-fabric-local.sh
```

For a different checkout layout:

```bash
DEV_HEALTH_ACR_DIR="../dev-health-acr" \
DEV_HEALTH_WEB_DIR="../dev-health-web" \
  bash scripts/context-fabric-local.sh
```

The script delegates to ACR's verified Compose lifecycle using this Ops checkout
as the API and entitlement source. It builds the current sources, starts the
isolated TLS stack, creates an organization, assigns the
`agent_context_runtime` entitlement, seeds `acme/live-e2e`, creates and rotates
a scoped ACR credential, and verifies both MCP read tools.

When ready, it prints a generated `client.env` path and keeps the stack alive.
The environment file contains paths, not token contents.

## Connect a sidecar or plugin

In another shell, source the exact path printed by the launcher:

```bash
source <printed-client-env-path>
acr-mcp doctor --live
```

Use this explicit test request in the client:

> Use Context Fabric for repository `acme/live-e2e` on branch `main` to inspect
> live evidence. Call `context_for_task`, then expand one evidence ID returned by
> that response with `source_evidence`.

The generated environment exports `DEV_HEALTH_ACR_DIR`, so the client package
instructions are available at:

```text
$DEV_HEALTH_ACR_DIR/docs/local-development.md
$DEV_HEALTH_ACR_DIR/clients/<client>/README.md
```

The client configuration registers `acr-mcp serve`; it does not store the ACR
credential in the project. Returned evidence remains untrusted data, pre-plan is
explicit opt-in, and writeback remains disabled by default.

## Use an existing ACR image

The launcher builds the current ACR checkout unless `ACR_E2E_IMAGE` is set to an
immutable image reference:

```bash
ACR_E2E_IMAGE="registry.example/acr-api@sha256:<digest>" \
  bash scripts/context-fabric-local.sh
```

A mutable tag is rejected. The Ops API image is always built from the selected
Ops checkout so the entitlement and fixture behavior match the code under test.

## Stop and clean up

Press Ctrl-C in the launcher terminal. The lifecycle trap removes only resources
owned by the generated `acr-e2e-*` project. It reports a failure if any owned
container, volume, or network remains.

Installed client packages are user-scoped and intentionally survive fixture
teardown. Use each package's uninstall command when finished.

## Web testing

The MCP fixture does not place Web assertion keys into this checkout or into
`dev-health-web`. Run the Web-owned Context Fabric browser/BFF harness
separately:

```bash
cd ../dev-health-web
corepack enable
pnpm install --frozen-lockfile
pnpm test:e2e:context-fabric
```

The full live Web assertion path remains owned by the ACR SVS suite.

## Troubleshooting

- **Launcher not found:** set `DEV_HEALTH_ACR_DIR` to the private ACR checkout.
- **Web checkout not found:** set `DEV_HEALTH_WEB_DIR`; the path is recorded for
  the follow-on Web checks even though the plugin fixture does not start Web.
- **Docker resource collision:** choose another isolated name with
  `bash scripts/context-fabric-local.sh --project acr-e2e-local-<suffix>`.
- **ACR image build rejects a dirty tree:** the developer launcher opts into
  dirty local builds and labels the image accordingly. An explicit environment
  override can still set `CONTAINER_ALLOW_DIRTY=0` to require a clean checkout.
- **Client cannot find `acr-mcp`:** source the generated `client.env` in the same
  shell before launching the client.
- **Client gets an authorization error:** use the seeded repository
  `acme/live-e2e`; the generated credential is intentionally scoped to it.
