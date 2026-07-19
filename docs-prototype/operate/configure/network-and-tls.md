---
page_id: op-network
summary: Configure ingress, DNS, TLS, proxies, callbacks, and trusted client-address handling.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Network, TLS, proxy, and domains

1. Define internal service boundaries and the external ingress path.
2. Terminate TLS at the approved boundary and encrypt required internal links.
3. Configure DNS and provider callback URLs exactly.
4. Configure trusted proxies narrowly; ignore forwarded client headers from untrusted peers.
5. Restrict administrative, data-store, queue, and metrics endpoints.
6. Verify health, application routes, callbacks, and real client-address handling.

A broad trusted-proxy setting can allow header spoofing; an empty setting behind a load balancer can collapse rate-limit identity onto the proxy.
