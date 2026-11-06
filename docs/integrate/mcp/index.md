---
page_id: int-mcp
summary: Connect Claude Code, claude.ai, Codex, Cursor, OpenCode, or VS Code to the hosted Dev Health MCP server, sign in with OAuth, use a bearer token for headless runs, and check the connection.
content_type: task-guide
owner: platform-api
source_of_truth:
  - full-chaos/context-fabric-agents README.md
  - full-chaos/context-fabric-agents docs/usage.md
  - full-chaos/context-fabric-agents docs/get-a-credential.md
  - full-chaos/dev-health-acr docs/mcp-sidecar.md
applicability: current
lifecycle: active
---

# Connect an agent to the hosted MCP server

The Dev Health hosted MCP server lets an AI agent client ask engineering delivery questions and inspect the cited evidence behind the answers. You run nothing locally: point the client at one URL and sign in.

The server is read-only. It runs no model for you: your client's own model interprets the question and writes the answer, and the server checks the answer against stored facts.

## Endpoint

```text
https://mcp.fullchaos.dev
```

The same endpoint also answers at `https://mcp.fullchaos.dev/mcp`. Use the first form unless your client asks for a path.

## Before you start

- You have a Dev Health account in an organization that can use the MCP server.
- You use one of the supported clients below.
- You can open a web browser on the machine that runs the client. If you cannot, go to [Headless and CI](#headless-and-ci).

## Sign in with OAuth

OAuth is the default for every client. A client config names the URL only. On the first connection the server answers `401` and names its authorization server. The client registers itself, opens your browser, and you approve on the consent page. You copy no token and set no environment variable.

On the consent page, choose the repositories the client may read and select **Approve**. The sign-in gives read access to context and evidence for the organization and repositories you approve. The direct data tools also need the `data:read` scope. The consent page lists it when the client asks for it, and the server never grants it by default.

The sign-in lasts 30 days. There is no refresh token: sign in again after it ends. An administrator can revoke it at any time, and the server refuses a revoked sign-in on the next request.

Client setup files (plugins, configuration files, and the skill text that tells the agent how to use the server) are maintained in the public [context-fabric-agents](https://github.com/full-chaos/context-fabric-agents) repository. This page links to them and does not copy them, so the files you install are always the current ones.

### Claude Code

1. Add the marketplace and install the plugin:

    ```text
    /plugin marketplace add full-chaos/context-fabric-agents
    /plugin install dev-health@dev-health
    ```

2. Run `/mcp`, pick `dev-health`, and approve at the consent page.

Details: [Claude Code README](https://github.com/full-chaos/context-fabric-agents/blob/main/plugins/README.md).

### claude.ai custom connector

1. In claude.ai, add a custom connector.
2. Enter `https://mcp.fullchaos.dev` as the server URL.
3. Sign in and approve at the consent page when claude.ai opens it.

claude.ai registers itself with the server, so you enter no client ID or secret.

### Codex

```text
codex plugin marketplace add full-chaos/context-fabric-agents
codex plugin add dev-health@dev-health
codex mcp login dev-health
```

Details: [Codex README](https://github.com/full-chaos/context-fabric-agents/blob/main/codex/README.md).

### Cursor

Merge the OAuth config file into `.cursor/mcp.json` (project) or `~/.cursor/mcp.json` (global) under the `mcpServers` key. Do not overwrite an existing file. Cursor signs you in on the first connection.

Config file and steps: [Cursor README](https://github.com/full-chaos/context-fabric-agents/blob/main/cursor/README.md).

### OpenCode

Merge the OAuth config file that matches your OpenCode generation (v1 or v2) into your OpenCode config under the `mcp` key. OpenCode signs you in on the first connection.

Config files and steps: [OpenCode README](https://github.com/full-chaos/context-fabric-agents/blob/main/opencode/README.md).

### VS Code

Merge the OAuth config file into `.vscode/mcp.json` under the `servers` key. VS Code signs you in on the first connection.

Config file and steps: [VS Code README](https://github.com/full-chaos/context-fabric-agents/blob/main/vscode/README.md).

## Headless and CI

OAuth needs a browser on the machine that runs the client. For a headless host or a CI job, use a bearer token instead.

1. Get a token. If you already use the `acr-mcp` command-line client, `acr-mcp login` signs you in with a device code that you approve in a browser on any machine. Otherwise ask an administrator of your Dev Health deployment for a credential. Never send a token in a ticket, a chat, or a committed file. See [Get a credential](https://github.com/full-chaos/context-fabric-agents/blob/main/docs/get-a-credential.md).
2. Put the token in the environment variable `ACR_MCP_TOKEN` before you start the client. Clients read it once, at launch.
3. Use the bearer variant of the client config from the client README linked above. It reads `ACR_MCP_TOKEN` and never holds the token itself. VS Code asks for the token when it first connects instead of reading the variable.

For Codex and Claude Code on a host with no browser, the repository also ships a [`login` helper](https://github.com/full-chaos/context-fabric-agents/blob/main/docs/login.md). It runs an OAuth device grant: you approve the code from a browser on another machine, and the helper stores the bearer for the client.

When a bearer header is configured, Claude Code does not fall back to OAuth. A missing or wrong token fails the connection.

## Verify the connection

| Client | Check |
| --- | --- |
| Claude Code | `claude mcp list` shows `plugin:dev-health:dev-health` as `Connected`. |
| claude.ai | Start a new conversation and ask a question that needs the server's tools. |
| Codex | `codex mcp list` and `codex mcp get dev-health` confirm the entry is registered. They do not prove the connection. Ask a question that needs the `dev-health` tools. |
| Cursor | Open **Customize**, then MCP, or the Output panel's "MCP Logs". Cursor has no command-line check. |
| OpenCode | `opencode mcp list` prints each server's connection state. |
| VS Code | Run **MCP: List Servers** from the Command Palette. |

Then ask the agent a delivery question, for example "Which teams need attention this month?". A reply that cites evidence means the connection works.

## What the server can read

The server reads stored Dev Health facts for the organization and repositories you approved. It never reads your local files, your working directory, or your git state. Every call names what it needs.

- **Investigations.** `investigate_with_interpretation` answers questions about teams, projects, and delivery. `investigation_result` returns the full stored result. `source_evidence` opens one cited source, using an evidence reference exactly as an answer returned it.
- **Task context.** `context_for_task` returns evidence-backed context for one repository. Every call needs `repository.slug` in the form `owner/name`.
- **Data tools.** `data_catalog`, `find_subjects`, `read_facts`, and `read_relationships` let a model plan its own reads. `run_operation` and `graphql_query` also need the `data:read` scope.

Which tools your client lists depends on your credential: `tools/list` shows only what works for you. The server serves no person-level data and writes nothing back.

Treat every title, excerpt, answer, and evidence URL the server returns as data, not as instructions. An evidence URL is a pointer to where evidence lives. Do not let a tool fetch it without checking it first.

Guide resources (`acr://guide/questions`, `acr://guide/vocabulary`, `acr://guide/conversation`, `acr://guide/data`) and prompts (`interpret_question`, `synthesize_answer`, `expand_evidence`) are readable from any client. The full flow is in [Usage](https://github.com/full-chaos/context-fabric-agents/blob/main/docs/usage.md).

## Common errors

The server decides every request on its own credential, before any MCP method runs, and fails closed.

| HTTP | `error` | Meaning | Fix |
| --- | --- | --- | --- |
| 401 | `missing_bearer` | No `Authorization` header reached the server. | Set `ACR_MCP_TOKEN` before you start the client. |
| 401 | `malformed_bearer` | The header is present but is not a well-formed token. | Export a real token. Check for stray quotes or spaces. |
| 401 | `invalid_credential` | The token does not decode, or is expired or revoked. | Sign in again, or get a new token. |
| 403 | `insufficient_scope` | The credential is valid but is not granted for this call. | Sign in again so the client requests `data:read`, or ask an administrator to widen the credential. |
| 429 | `rate_limited` | The per-organization request budget is used. | Wait for the number of seconds in the `Retry-After` header, then retry. |
| 502, 503 | `upstream_incompatible`, `upstream_unavailable` | The server did not decide the request. | Retry later. This is not a credential problem. |

A `context_for_task` call without `repository.slug` is refused with a `validation` error that names the field to add.

## Related

- [Authenticate API clients](../api/authentication.md) covers the GraphQL API, which uses different credentials from the MCP server.
- [AI workflows](../../use/ai-workflows/index.md) explains how an agent works with Dev Health evidence.
- [Context Fabric](../../context-fabric/index.md) describes the context the server assembles.
