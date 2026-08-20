# BYO-LLM Credential Resolution

This page describes how the Dev Health platform resolves credentials for Bring Your Own LLM (BYO-LLM) configurations.

## Resolution Precedence

When a component requests an LLM provider, the system resolves the API key and base URL using a strict precedence order. The first source that provides a value wins.

```mermaid
flowchart TD
    Start([Resolve Credentials]) --> CheckPerCall{Per-Call / CLI Flags?}
    CheckPerCall -- Yes --> UsePerCall[Use api_key / base_url from arguments]
    CheckPerCall -- No --> CheckEnv{Process Environment?}
    
    CheckEnv -- Yes --> UseEnv[Use provider-specific env variables]
    CheckEnv -- No --> CheckOrg{Org Settings in Postgres?}
    
    CheckOrg -- Yes --> UseOrg[Decrypt and use org-scoped settings]
    CheckOrg -- No --> CheckRequired{API Key Required?}
    
    UsePerCall --> CheckRequired
    UseEnv --> CheckRequired
    UseOrg --> CheckRequired
    
    CheckRequired -- Yes, but missing --> RaiseAuthError[Raise LLMAuthError]
    CheckRequired -- No, or present --> ReturnCreds([Return LLMCredentials])
```

## Precedence Details

1. **Per-Call Arguments / CLI Flags**:
   Values passed directly to the function call (e.g., `--llm-api-key` or `--llm-base-url` from the CLI) take the highest priority.
2. **Process Environment Variables**:
   If no per-call arguments are present, the system checks the environment. Each provider has a list of environment variables it checks in order. For example, the `openai` provider checks `LLM_API_KEY` first, then `OPENAI_API_KEY`.
3. **Organization Settings**:
   If neither per-call arguments nor environment variables are set, the system queries the Postgres database for organization-scoped settings. These settings are stored under the `llm` category and decrypted using the `SettingsService`.

## Provider Environment Variable Mapping

The system maps providers to specific environment variables.

| Provider | API Key Environment Variables | Base URL Environment Variables |
|---|---|---|
| `openai` | `LLM_API_KEY`, `OPENAI_API_KEY` | `LLM_BASE_URL`, `OPENAI_BASE_URL` |
| `anthropic` | `LLM_API_KEY`, `ANTHROPIC_API_KEY` | `LLM_BASE_URL`, `ANTHROPIC_BASE_URL` |
| `gemini` | `LLM_API_KEY`, `GEMINI_API_KEY` | `LLM_BASE_URL`, `GEMINI_BASE_URL` |
| `qwen` | `LLM_API_KEY`, `QWEN_API_KEY`, `DASHSCOPE_API_KEY` | `LLM_BASE_URL`, `DASHSCOPE_BASE_URL` |
| `local` | `LLM_API_KEY`, `LOCAL_LLM_API_KEY` | `LLM_BASE_URL`, `LOCAL_LLM_BASE_URL` |
| `ollama` | `LLM_API_KEY`, `LOCAL_LLM_API_KEY` | `LLM_BASE_URL`, `OLLAMA_BASE_URL` |
| `lmstudio` | `LLM_API_KEY`, `LOCAL_LLM_API_KEY` | `LLM_BASE_URL`, `LMSTUDIO_BASE_URL` |

## Validation and Errors

If a provider requires an API key (such as `openai`, `anthropic`, `gemini`, or `qwen`) and none is found after checking all sources, the system raises an `LLMAuthError`. This error lists the missing configuration options to help operators resolve the issue.

## Admin status endpoint

Org admins can read `GET /api/v1/admin/llm-settings/status` to show the current BYO-LLM state without triggering resolver side effects. The endpoint is gated the same way as `GET /api/v1/admin/llm-settings` and returns:

```json
{
  "configured": true,
  "active": false,
  "degraded": true,
  "reason_code": "invalid_base_url",
  "last_fallback_at": "2026-07-06T12:00:00Z"
}
```

`reason_code` is one of:

| Reason code | Meaning |
| --- | --- |
| `not_configured` | No usable org provider has been saved. |
| `unknown_provider` | The saved provider is not one of the supported runtime providers. |
| `missing_credentials` | The provider is saved but lacks required credential material. |
| `invalid_base_url` | The current saved `base_url` fails SSRF validation, so runtime falls back to the platform default. |
| `active` | The saved org provider and credentials are currently usable. |

`degraded` is true only for the current saved `invalid_base_url` state. `last_fallback_at` is populated from recent deduplicated audit rows that match the current org, provider, and saved base URL hash; old rows for a since-fixed or different-org configuration do not make the endpoint read as degraded.

## Fallback audit dedupe and alerting

Runtime base URL fallbacks write at most one `audit_logs` row per `(org_id, provider, base_url_hash, reason_code)` during `BYO_LLM_BASE_URL_FALLBACK_DEDUPE_WINDOW`. Every fallback still emits the low-cardinality Prometheus counter `devhealth_byo_llm_base_url_fallback_total{provider,reason_code,audit_inserted}` and a structured warning. If recent fallback audit rows for the same org reach `BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD` during `BYO_LLM_BASE_URL_FALLBACK_ALERT_WINDOW`, the runtime also emits `devhealth_byo_llm_base_url_fallback_alert_total{provider,reason_code}` and a distinct warning for alert routing.
