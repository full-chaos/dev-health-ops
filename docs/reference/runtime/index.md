---
page_id: ref-runtime
summary: Which runtime -- Go or Python -- actually produces, gates, and writes each job kind and bridge route today.
content_type: landing
owner: platform-operations
applicability: current
lifecycle: active
---

# Runtime

Use Runtime to look up which process actually executes a piece of work today, not which ticket says it should.

- [Python↔Go live-path ledger](python-go-live-path-ledger.md)

For the process/queue/lease architecture, read [Go worker runtime](../../contribute/architecture/go-worker-runtime.md) instead. This section answers a narrower, more perishable question: for this exact kind or route, who is the live writer right now.
