---
page_id: op-logs
summary: Collect structured, correlated, redacted logs with enough context to diagnose a request or job.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# Logs

Logs should identify service, environment, organization or workspace identifier, provider, operation, request or job identifier, status, duration, retry, and sanitized error.

Do not log:

- tokens, passwords, private keys, session cookies, authorization headers, or signed URLs;
- unredacted customer payloads or prompt content;
- full database connection strings;
- unnecessary personal data.

Use correlation identifiers to connect API, worker, queue, provider, and storage events.

## Ask Dev provider-client logging

Ask Dev's orchestrator and OpenAI-compatible provider adapter never log
conversation, evidence, or tool-result content themselves. The only path by
which that content could otherwise reach ordinary application logs is the
third-party HTTP/provider client libraries' own debug logging, which by
default inherits the process root log level.

Raising `LOG_LEVEL` to `DEBUG` (for example, temporarily, for local
diagnostics via `compose.yml`) does **not** enable that content logging for
Ask Dev. The `openai`, `httpx`, and `httpcore` loggers are pinned to
`WARNING` unconditionally, regardless of the configured root level, because
at `DEBUG` these libraries log the full outbound request body — including
the system policy prompt, prior conversation turns, the current question,
resolved scope, and tool-result payloads — and response headers/bodies.

What remains observable at every log level:

- request and run identifiers;
- provider and model fingerprints;
- latency, token usage, and estimated cost;
- a classified, sanitized error code and whether it is retryable (see
  `AgentProviderErrorCode` / `DevError.code`) — never a raw provider
  response body or exception text.

Supported diagnostic workflow: correlate a failing Ask Dev request by its
run ID and `safe_error_code` in `dev_runs` and exported telemetry, not by
raising `LOG_LEVEL` and inspecting `docker compose logs api`. If a live
provider integration needs deeper inspection, use the operator-owned
external model server's own logs (for example, LM Studio) — this contract
does not extend to those processes, only to FullChaos-operated ones.
