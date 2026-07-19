---
page_id: int-provider
summary: Implement a provider integration with explicit authentication, discovery, normalization, budgeting, and observability.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Build a provider integration

1. Define the supported systems, hosts, permissions, and data families.
2. Implement authentication and credential rotation without leaking secrets.
3. Discover source scope deterministically.
4. Normalize provider records into canonical contracts.
5. Preserve provider identifiers and timestamps for reconciliation.
6. Implement pagination, incremental watermarks, bounded backfill, rate budgets, retries, and idempotency.
7. Emit tenant-safe observability and coverage state.
8. Test live-like permission, rate-limit, replay, partial, and deletion behavior.

A provider-native field must not silently become a canonical product concept without an approved normalization rule.
