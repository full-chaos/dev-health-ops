---
page_id: op-auth-capacity
summary: Size auth-service replicas, CPU, and database connections from measured per-pod request rates, with the headroom rule and the measurements that are still missing.
content_type: reference
owner: platform-operations
source_of_truth:
  - deploy/helm/dev-health/values.yaml
  - internal/auth/authruntime/runtime.go
  - internal/auth/authconfig/config.go
applicability: current
lifecycle: active
---

# Auth-service capacity

Size auth-service from the request rate of each path, not from one total. Login is bound by CPU (password hashing); token issue, refresh, and key publication are fast and bound by the in-flight cap and the database.
{: .fc-page-lede }

## What the numbers are

All numbers come from one measured run set: ops build `b4e8ff13`, a throwaway Compose project on the development host, one auth-service pod with 1 CPU and 512 MiB, load sent directly to the pod. They are not production measurements. Full tables are kept with the run record (`RESULTS.md` sha `6564b45b`, `RESULTS2.md` sha `08d77d1b`).

| Path | Measured per pod (1 CPU) | Knee |
|---|---|---|
| Key publication (JWKS) | p95 under 2 ms up to 1600 requests/s | about 3000 requests/s |
| Token issue | p95 under 10 ms up to 400 requests/s; 800 requests/s gives p95 161 ms at about 100% CPU | between 400 and 800 requests/s |
| Token refresh | p95 about 7 ms at 300 requests/s, CPU 47% | not measured |
| Login (valid or invalid password) | about 0.27 s per attempt, bcrypt cost 12 takes 258 ms | about 3.7 logins/s per CPU core |

Other measured values:

- In-flight cap: 64 requests per pod across all routes. A request over the cap is refused at once with 429.
- Database connections: token issue at 200 requests/s used at most 7. A pool cap of 4 cost nothing measurable at 200 requests/s. The default pool is 8 per replica (`AUTH_DATABASE_MAX_CONNS`, range 1 to 256).
- Memory: peak 27 MiB of 512 MiB.
- CPU: a 0.5 CPU limit held p95 for issue and key publication, with a p99 tail of 35 to 53 ms.
- Refresh with 6 times the access-token refresh rate (ADR-03 shorter lifetime): p95 6.8 ms, no errors, auth-service CPU from 4.7% to 18.9%.
- Default route limiter: 20 requests/s, burst 40, an exact token bucket.
- Refresh replay grace window: 30 s.

## Planning rules

Let `knee(path)` be the measured knee above, per CPU core, and `demand(path)` the expected requests per second.

1. Plan to 70% of the knee: `safe(path) = 0.7 x knee(path) x cores`. For login, `safe = 0.7 x 3.7 x cores`.
2. In-flight bound: `64 / p95 latency in seconds` is the most a pod can hold before it sheds with 429. Keep demand under half of it. A 10 s database hang gives 6.4 requests/s per pod, so a database stall sheds load even on key publication.
3. Pods: `ceil(max over paths of demand(path) / safe(path)) + 1`. The extra pod keeps the rule true after the loss of one pod.
4. Database budget: `pods x AUTH_DATABASE_MAX_CONNS` must fit inside the database connection budget described in [Capacity and sizing](capacity-and-sizing.md). Lower the pool before adding a pod when the budget is tight.
5. Refresh demand is `active sessions / access-token lifetime in seconds`. Shortening the lifetime raises refresh demand in the same ratio.

Worked example, 2 CPU cores per pod and 60 logins per minute at peak: `safe(login) = 0.7 x 3.7 x 2 = 5.2` logins/s per pod, demand is 1 login/s, so one pod carries it and the N+1 rule gives 2 pods. Failed logins cost the same as valid ones, so a burst of 20 failed logins keeps one 1 CPU pod busy for about 5 s; size the login path for attack bursts, not only for normal use.

## Dependency behavior to plan for

- Postgres unreachable: token issue and refresh answer 503, key publication keeps answering 200 (with 429 shedding once the in-flight cap fills), login answers 500.
- Valkey (rate-limit store) unreachable: login fails closed with 500; other paths are not affected.
- Degrade started 6 s (blackhole) or 11 s (delay) after a database fault began, as 429 load shedding. Every fault recovered within about 1 s of the dependency returning.

## Not measured

- The refresh knee above 300 requests/s. About 600 requests/s per pod is a linear extrapolation, not a measurement.
- A valid-login ladder. The address limit (20 per 15 minutes by design) stops it, so login capacity is derived from the per-attempt cost.
- Production hardware. Re-measure on the target node type before committing a production size.
- Login failure codes under a database fault, the key publication tail under a database stall, and the key-read path can change these numbers. Re-read this page when they change (CHAOS-11373, CHAOS-11374, CHAOS-11375).

## Chart values

The chart keeps auth-service off by default. Replica and resource defaults follow these rules in a later change; set `authService.replicas` and `authService.resources` per release from the rules above until then.
