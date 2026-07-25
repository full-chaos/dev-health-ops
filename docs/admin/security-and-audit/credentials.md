---
page_id: admin-credential-resp
summary: Separate workspace credential responsibilities from operator secret storage and encryption.
content_type: concept
owner: platform-product
source_of_truth:
  - docs/llm/byo-llm-credentials.md
  - current provider and model credential flows
applicability: current
lifecycle: active
---

# Credential responsibilities

Workspace administrators choose supported connections and initiate approved authorization flows. Operators provide secure storage, injection, encryption, backup exclusion, and rotation mechanisms.

- Use least privilege and named ownership.
- Prefer installation or delegated authorization flows over shared long-lived tokens.
- Keep secret values out of screenshots, logs, report output, and issue comments.
- Record expiry and rotation triggers.
- Revoke abandoned credentials promptly.

A credential being accepted does not prove that it has the right namespace, repository, model, or record-family access.
