---
page_id: op-rb-auth
summary: Recover repeated provider authentication failures without exposing or repeatedly testing a compromised credential.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Provider authentication failure

1. Identify provider, host, connection or installation, affected workspace, and failure time.
2. Distinguish expiry, revocation, missing scope, callback mismatch, host mismatch, and provider outage.
3. Stop repeated retries when they create lockout or rate-limit risk.
4. Rotate or reauthorize through the supported path.
5. Verify one bounded request and synchronization.
6. Revoke the old credential and record any coverage gap.

Treat unexpected use or exposure as a credential incident, not routine troubleshooting.
