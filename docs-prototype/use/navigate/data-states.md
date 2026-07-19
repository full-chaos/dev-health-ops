---
page_id: use-data-states
summary: Distinguish measured values from unavailable, incomplete, stale, delayed, and unsupported states.
content_type: concept
owner: product-analytics
applicability: current
lifecycle: active
---

# Understand loading, empty, stale, and partial data

| State | Meaning |
| --- | --- |
| Loading | The request or client transition has not completed. |
| Measured value | The supported calculation returned a value for the current context. |
| Measured zero | The calculation returned zero; it is not shorthand for missing data. |
| Empty | No matching rows or result are available; the cause still needs diagnosis. |
| Unavailable | The product does not have a supported value for the context. |
| Incomplete or partial | Required source or processed input is missing. |
| Stale | A value exists but is older than the question requires. |
| Delayed | Ingestion, computation, or report work has not completed. |
| Unsupported | The route, feature, source, or combination is not currently supported. |

Use the visible status and timestamps where available. Start with [No or incomplete data](../troubleshooting/no-or-incomplete-data.md) rather than converting an absence into a conclusion.
