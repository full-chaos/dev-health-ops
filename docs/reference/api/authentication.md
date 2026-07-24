---
page_id: ref-api-auth
summary: Credential and organization-scope rules for supported API surfaces.
content_type: api-reference
owner: platform-api
applicability: current
lifecycle: active
---

# Authentication and authorization

| Surface | Credential boundary | Scope requirement |
| --- | --- | --- |
| GraphQL analytics | Deployment-supported application or API authentication | Authorized organization context in the request and GraphQL variables |
| Customer Push write/status/schema | Dedicated `fcpush_` bearer token | Token scopes; write token bound to one registered source |
| Customer Push source/token administration | Authenticated organization administrator | Intended organization and admin role |
| Provider webhooks | Provider-specific signature or secret | Configured endpoint and provider connection |

Authorization is evaluated independently from authentication. A valid credential can still be denied because of role, organization, source binding, scope, entitlement, or route support.

Never reuse one surface's credential on another surface. Redact bearer tokens, session cookies, private keys, authorization headers, and signed URLs.
