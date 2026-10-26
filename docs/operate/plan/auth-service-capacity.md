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

Size auth-service from the request rate of each path, not from one total. Login is bound by CPU (password hashing). Token issue, refresh, and key publication are fast, but they share the same CPU and the same in-flight cap as login, so add the paths together.
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
- Rollout overlap (kind, chart defaults, 2 replicas, `helm upgrade` with a pod-template change, steady `/readyz` load plus a body-less request held on each old pod): runtime-login connections peaked at 4 under load and 6 to 7 during the rollout (two runs), with 4 pods alive, against a possible 24 (3 x 8) to 32 (4 x 8). Pod CPU sits at the 1000m limit under this load (950 to 1026m steady per pod) and is throttled (19.3 s while loading, 7.6 s during the roll), so the CPU ceiling of the overlap is pods alive x 1000m: 4 x 1000m while 4 pods were alive, measured 2216m total in the overlap window. Single-sample peaks of 1180 to 1260m per pod (3283m total) come from the sample interval (samples are about 0.6 to 0.9 s apart), not from use above the limit; the steady value is 950 to 1026m at the 1000m limit. Caveats: this is a probe-only measurement (a `/readyz` database probe per request); login and token issue traffic was not measured. The old pods were held only 1.0 s and 5.5 s, so the 20 s shutdown bound was never reached and a longer hold would keep connections longer. The full table and the command are on CHAOS-11390. Command: `scripts/capped.sh -t 1800 -m 16 -- bash measure_auth_rollout_overlap.sh <ops root>` (script sha256 `a648fdc3`, kept in the lane scratch directory; run log sha256 `8cc018ba`).
- Memory: peak 27 MiB of 512 MiB.
- CPU: a 0.5 CPU limit held p95 for issue and key publication, with a p99 tail of 35 to 53 ms.
- Refresh with 6 times the access-token refresh rate (ADR-03 shorter lifetime): p95 6.8 ms, no errors, auth-service CPU from 4.7% to 18.9%.
- Default route limiter: 20 requests/s, burst 40, an exact token bucket.
- Refresh replay grace window: 30 s.

## Planning rules

Let `knee(path)` be the measured knee above, per CPU core, and `demand(path)` the expected requests per second.

1. Plan to 70% of the knee: `safe(path) = 0.7 x knee(path) x cores`. For login, `safe = 0.7 x 3.7 x cores`.
2. In-flight bound: `64 / p95 latency in seconds` is the most a pod can hold before it sheds with 429. Keep demand under half of it. A 10 s database hang gives 6.4 requests/s per pod, so a database stall sheds load even on key publication.
3. Pods: add the load of all paths, because they share one CPU budget: `S = sum over paths of demand(path) / safe(path)` (the 1 CPU knees above are per core, so `safe` already includes the core count), then `pods = ceil(S) + 1`. The extra pod keeps `S / (pods - 1)` at or under 1 after the loss of one pod. The 64 in-flight slots are one pool per pod across all routes, not one per path. Mixed load with a pod removed was not measured (see Not measured).
4. Database budget: `pods x AUTH_DATABASE_MAX_CONNS` must fit inside the database connection budget described in [Capacity and sizing](capacity-and-sizing.md). Lower the pool before adding a pod when the budget is tight.
5. Refresh demand is `active sessions / access-token lifetime in seconds`. Shortening the lifetime raises refresh demand in the same ratio.

Worked example, 2 CPU cores per pod and 60 logins per minute at peak: `safe(login) = 0.7 x 3.7 x 2 = 5.2` logins/s per pod, demand is 1 login/s, so `S = 1 / 5.2 = 0.19` for login alone, one pod carries it, and the N+1 rule gives 2 pods. Add the token issue, refresh, and key publication demand to `S` before you read the result. Failed logins cost the same as valid ones, so a burst of 20 failed logins keeps one 1 CPU pod busy for about 5 s; size the login path for attack bursts, not only for normal use.

## Dependency behavior to plan for

- Postgres unreachable: token issue and the native refresh route (`/v1/token/refresh`) answer 503; the compatibility refresh route (`/api/v1/auth/refresh`) and login answer 500. Key publication keeps answering 200 from its cached key-state snapshot (with 429 shedding once the in-flight cap fills), but only while that snapshot is no more than five minutes old; after that it stops answering 200. The measured fault windows were shorter than that.
- Valkey (rate-limit store) unreachable: every route that uses the shared limiter fails closed with 500: login, compatibility refresh, credential routes, password reset, and social link. Routes that do not call the limiter (key publication measured unaffected) keep answering. Only the login failure was measured.
- Degrade started 6 s (blackhole) or 11 s (delay) after a database fault began, as 429 load shedding. Every fault recovered within about 1 s of the dependency returning.

## Not measured

- The refresh knee above 300 requests/s. About 600 requests/s per pod is a linear extrapolation, not a measurement.
- A valid-login ladder. The address limit (20 per 15 minutes by design) stops it, so login capacity is derived from the per-attempt cost.
- Mixed load. Login and token issue were not run together, and no pod was removed during a run. The sum rule above is arithmetic, not a measured result.
- Production hardware. Re-measure on the target node type before committing a production size.
- Login failure codes under a database fault, the key publication tail under a database stall, and the key-read path can change these numbers. Re-read this page when they change (CHAOS-11373, CHAOS-11374, CHAOS-11375).

## Chart values

The chart keeps auth-service off by default (`authService.phase: 0`). When it is on (phase 1 or more), the defaults follow the rules above:

| Value | Default | Why |
|---|---|---|
| `authService.replicas` | 2 | N+1: one pod can be lost or drained. |
| `authService.resources.requests.cpu` | 250m | Login is CPU bound; 500m is requested for two pods and 750m while a rollout surges a third. |
| `authService.resources.limits.cpu` | 1000m | The measured run used one CPU per pod. |
| `authService.resources.limits.memory` | 256Mi | Peak use was 27 MiB. |
| `authService.database.maxConns` | 8 | Pool per replica, a whole number from 1 to 32. Set it here, not through `extraEnv` or `extraArgs`; the chart passes it as the `--database-max-conns` flag. |
| `authService.database.connectionBudget` | 0 | The connections the auth database may hold: `max_connections` minus `superuser_reserved_connections` (PostgreSQL defaults: 100 - 3 = 97). 0 turns the check off. Above 0 the render fails when the static maximum below is more than the budget. |

With two or more replicas the chart also renders a PodDisruptionBudget with `minAvailable: 1`. The pods connect to the database directly, so the database must hold `replicas x maxConns`, plus one surge pod while rolling: 16 connections steady and 24 during a rollout at the defaults. These are planning figures, not a ceiling: a pod that is shutting down (up to 20 s, and it holds its connections until its in-flight requests finish) can overlap with the surge pod, so leave room above 24. The overlap was measured in two runs on kind (run 2 database connections only, run 3 connections and CPU): 6 to 7 connections at the high-water mark with four pods alive and old pods held 1 to 6 s (see Other measured values); the planning figures above stay, because a longer hold keeps more connections. Set `authService.database.connectionBudget` to make the chart check these figures; with the default 0 the chart does not check them, so compare them with the budget in [Capacity and sizing](capacity-and-sizing.md) yourself. The node capacity of a release is not known to the chart: confirm the cluster can schedule the CPU requests above before enabling the service.

## Database connection budget

The check counts every client that holds server connections on the auth database, as a static maximum:

`(authService.replicas + 1 surge pod) x authService.database.maxConns` plus every go-pgbouncer pool. PgBouncer sizes a pool per (user, database) pair, so the transaction pooler counts its own role at `transaction.poolSize` and each `goWorkers.pgbouncer.transaction.extraUsers` entry at its own `poolSize`, or at `transaction.poolSize` when the entry sets none, and ALL of those pools count again for the surge pod of a rollout: x (1 + surge pods), where the surge is `transaction.rollingUpdate.maxSurge` when set and otherwise 1 (the Kubernetes default at one replica). The queue-session and coordinator-session poolers count one pool each. Poolers count only while `goWorkers.enabled` and `goWorkers.pgbouncer.enabled` are on.

A `poolSize` on an `extraUsers` entry is enforced by the pooler: the chart renders a ConfigMap with a PgBouncer `[users]` file (`pool_size` per sized entry) that the transaction pooler loads after its generated config. The default KEDA read-only role has `poolSize: 4`.

| Client | Production values | Chart defaults | Source |
|---|---|---|---|
| Transaction pooler, own role | 9 | 16 | `goWorkers.pgbouncer.transaction.poolSize` |
| Transaction pooler, KEDA read-only role | 4 | 4 | the `transaction.extraUsers` entry's `poolSize` |
| Transaction pooler, surge pod of a rollout | 13 | 20 | the two rows above again, x `transaction.rollingUpdate.maxSurge` (default 1 pod) |
| Queue-session pooler | 36 | 50 | `goWorkers.pgbouncer.queueSession.poolSize` |
| Coordinator-session pooler | 10 | 14 | `goWorkers.pgbouncer.coordinatorSession.poolSize` |
| auth-service, 2 replicas + 1 surge pod | 24 | 24 | (2 + 1) x `authService.database.maxConns` 8 |
| Static maximum | 96 | 128 | |
| Usable with PostgreSQL defaults | 97 | 97 | `max_connections` 100 minus 3 reserved |

The production values (deploy repo, CHAOS-11546, option (a)) keep the transaction pool at 16 and set `transaction.rollingUpdate: {maxSurge: 0, maxUnavailable: 1}` with `connectionBudget: 97`: (16 + 4 KEDA) x (1 + 0 surge) + 36 queue-session + 10 coordinator-session + 24 direct = 90, inside 97 with a margin of 7. A transaction pool of 9 was ruled out: the 7 day peak of domain-pool demand is 19 client connections (CHAOS-11548). The chart defaults do not fit, on purpose: the transaction (16) and queue-session and coordinator-session defaults (50 and 14) are sized for the deployment manifest, which assumes every worker group at `max_replicas` plus a full rolling restart. With `connectionBudget: 97` the chart defaults fail the render with the named message, and the chart default budget (0) leaves every render unchanged. The cost of `maxSurge: 0` is that with one replica every roll of the transaction pooler is an outage of its start time (measured: 331 client errors over 18 s against 95 over 10 s with the surge; a worker that started in the gap exited and restarted for 13 to 15 s, CHAOS-11541), so it is not the chart default. The values ship after the CHAOS-11541 (retry) and CHAOS-11558 (schema refuses `maxSurge: 0` without `maxUnavailable` of 1 or more) gates.

**The transaction pool of 9 has no spare over one observation.** The production values file observed 9 domain and 3 KEDA backends in use on the two transaction pools (once, before CHAOS-11490; no connection metric exists yet, CHAOS-11548). A pool of 9 equals that figure. Kind measurement (CHAOS-11542, one run per cell, one query at a time per session): at the observed concurrency (9 domain, 3 KEDA clients) pools of 16, 11 and 9 behave the same (p99 client wait 2.0 to 2.4 ms, no waiting client); at 12 concurrent domain clients the pool of 9 queues (p99 wait about 102 ms, throughput down 25 %, no errors). Transaction mode queues clients, it does not refuse them.

This is an upper bound, not a measured count: a pool opens server connections only while it has work. The production values file observes 9 and 3 backends in use on the two transaction pools.

Named assumption, not a counted term: the session poolers add +0 while they roll (CHAOS-11521 kind measurement). Their old pod keeps its clients until it is stopped (24 to 31 s, the 30 s grace period), so the surge pod opens no server connection: the measured total stayed at the baseline in every session-pooler case. The transaction pooler is different: its surge pod serves new connections at once (+18 measured at pool 16 + KEDA 4), so the check counts it. If a session pooler ever keeps fewer clients on the old pod (for example, a shorter grace period), measure again before relying on +0: at the production values one extra session pod could add up to 36 (queue-session) or 10 (coordinator-session) connections.
