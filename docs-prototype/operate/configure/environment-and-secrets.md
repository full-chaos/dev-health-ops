---
page_id: op-env
summary: Supply runtime configuration and secrets with explicit ownership, rotation, and restart behavior.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Environment and secrets

1. Generate the current configuration reference from runtime settings or source.
2. Separate ordinary configuration from secret values.
3. Store secrets in the approved secret manager and inject them at runtime.
4. Record required, optional, default, reload, and restart behavior.
5. Restrict read access and redact values from logs and diagnostics.
6. Test rotation for provider, database, queue, signing, and model credentials.

Never commit production secrets to the repository, images, Compose files, manifests, screenshots, or documentation examples.
