# CHAOS-3142 end-to-end proof: final report

Moved out of [`README.md`](./README.md), which is the operational runbook. This
is an incident/verification record: what a specific investigation proved, what
it did not, and why. It is kept for provenance, not as a procedure to follow.
For the cutover procedure see [`CUTOVER-RUNBOOK.md`](./CUTOVER-RUNBOOK.md); for
deployment and coexistence see [`README.md`](./README.md).

This is the durable record of what CHAOS-3142 actually proved, against both
a real shared local stack and an isolated throwaway one, and exactly where
it stopped. Written here rather than in a session-scoped note because this
is the artifact that outlives the session.

### Proven, against a real shared local stack

- Coordinator-role provisioning on a pre-existing, pre-CHAOS-3033-split
  Postgres cluster: `go-river-provision` created the `devhealth_coordinator`
  login where none existed before.
- The exact grant/readiness asymmetry this document names above, hit for
  real: `coordinatorPosture()` requires `fixed_schedule_occurrences`
  (`0065_add_fixed_schedule_occurrences.py`), which didn't exist on this
  database (`alembic_version` was `0064`, two migrations behind head); the
  `to_regclass`-guarded grant was silently skipped;
  `coordinator_postgres` readiness failed with no stated reason until a
  **targeted** `alembic upgrade 0065` (never `head` — see above) created the
  table and a re-run of `go-worker-migrate` picked up the previously-skipped
  grant.
- `go-reconciler` reaching `/readyz` → `{"status":"ok"}` with `RestartCount
  0` against the real stack, and staying that way — `CheckCoordinatorAuthorization`
  re-queries live on every poll, so no restart was needed once the grant
  landed.
- Non-zero, real metric series from the real reconciler:
  `worker_outbox_reconciler_up 1`, `sync_dispatch_observer_up 1`, and their
  paired `..._last_success_age_seconds` gauges reporting sub-second real
  values — the "present but zero" failure mode this document elsewhere
  warns against did not occur here.

### Proven, against an isolated throwaway project

Using a separate compose project (`-p <name>`, its own network/volumes,
never the shared stack), with a hand-seeded additive
`Integration`/`IntegrationSource`/`IntegrationDataset`/`SyncRun`/
`SyncRunUnit`/`SyncRunReferenceDiscovery`/`integration_credentials` row set
for a synthetic org (github/repo-metadata), and the durable route
(`worker_job_routes.transport`) and Python producer switch
(`WORKER_GITHUB_REPO_METADATA_ENABLED`) both flipped ONLY inside that
isolated project — never on shared infrastructure:

- Producer gate (`dispatch_sync_run`) → `worker_job_outbox` row
  (`status=delivered`) → reconciler relay → River job → Go handler pickup
  and execution, all confirmed end to end.
- The Go handler reached a real `github.com` HTTP round-trip (a fake PAT, so
  a `retryable`, not `permanent`, failure — real network I/O happened, this
  wasn't a local rejection) and `worker_budget_wait_seconds{provider="github",
  cost_class="light"}` carried `count=6`, `sum=0.000241334` — a real,
  non-zero series, not the present-and-zero shape this document elsewhere
  calls out as the actual failure mode to guard against.

### Not proven

A ClickHouse `repos` row, on either stack. `worker_sync_lease_expired_total`
also never carried a non-zero series in any run: this is expected and
**not a regression signal** — that metric only increments on a claim that
itself recovered an *expired* lease
(`providerunit.Handler.observeLeaseRecovery` checks `claim.Recovered`, a
no-op for an ordinary first-attempt claim), and nothing in this proof
deliberately expired and re-claimed a lease.

**Correction (this section originally overstated the credential blocker —
see below):** the isolated project's synthetic org needed a fake PAT
because its Postgres volume was fresh and empty; that limitation does not
carry over to a stack with real data, and an earlier version of this report
wrongly transplanted it onto the real shared stack without checking. On the
real stack the blocker is routing, not credentials — see the next section.

### Not attempted against the real shared stack, and why

**Blocked on a deliberate routing decision, not on credential
availability.** A working, decryptable, correctly-shaped github credential
already exists on the real shared stack — this was checked directly rather
than assumed:

- `integrations`: 16 rows with `provider='github'`, 7 with a `credential_id`
  set.
- `integration_credentials` for `provider='github'`: 3 active rows, all
  `last_test_success = true`, most recently tested 2026-07-05.
- All three decrypt successfully today, using the app's own `decrypt_value`
  with the **current** `SETTINGS_ENCRYPTION_KEY` (run inside the
  `dev-health-api-1` container) — which also confirms that key is the one
  they were encrypted with. Two decrypt to a JSON object with keys
  `app_id`, `base_url`, `installation_id`, `org`, `private_key`, `token` —
  a real `token`, in the JSON-object shape `complete_route.go` requires,
  not a bare string (see "Provider credentials" below). The third is
  app-only: `app_id`, `installation_id`, `private_key`.

So credential availability and shape are not what stands between this stack
and a real ClickHouse `repos` row. What does, and was deliberately not
touched, is the two-key routing interlock, both halves of which were
considered and explicitly left alone:

1. **They are not independent, and the order matters.** Flipping the
   durable route (`worker_job_routes.transport` for `sync.provider_unit`,
   currently `celery`, would need `river_canary`) alone, with the Python
   producer switch (`WORKER_GITHUB_REPO_METADATA_ENABLED`) still off, is not
   a smaller, safer version of flipping both — it actively raises.
   `dispatch_sync_run` (`src/dev_health_ops/workers/sync_units.py`) consults
   `ProviderUnitRouteSwitches.is_route_ready(provider, dataset)` (matrix-only,
   unconditional) BEFORE consulting the switch. If the matrix says a pair is
   route-ready and the durable route already points at River, but the
   switch is off, that combination is treated as an explicit ownership
   fault — `dispatch_sync_run` raises
   `WorkerJobRouteError("sync provider canary capability is unavailable")`
   rather than degrading to Celery, by design ("never a reason to silently
   fall back to legacy Celery dispatch for a pair the matrix says is done").
   So the durable-route flip cannot be evaluated, or left in place, on its
   own — getting past it requires the Python switch too, immediately, for
   every org.
2. **The Python switch requires a live container recreate, not a
   reversible row.** Neither route switch is wired into `api`/`worker`/
   `beat`/`worker-heavy`/`worker-ingest` in the repo-root `compose.yml` at
   all (confirmed: `docker inspect` on the running containers, and a grep of
   the compose file, both show zero occurrences outside the `go-worker`
   service block). Setting it means recreating `dev-health-worker-1` — the
   real Celery worker actively processing real organizations' real sync
   traffic — with new environment. That is a materially different, and
   materially bigger, ask than one reversible `UPDATE` on a single
   `worker_job_routes` row, and it was not authorized. The user re-decided
   with the corrected facts above (credentials exist; the blocker is purely
   the routing interlock) and still chose to stop here — this is a
   deliberate scope decision, not a missing capability.

### Reachability: what completing the chain would actually take

With both interlock halves flipped, the chain **can** complete to a real
ClickHouse `repos` row using one of the three existing credentials above —
no GitHub PAT needs to be obtained or seeded. Whoever picks this up needs
only:

1. `UPDATE worker_job_routes SET transport='river_canary', generation=generation+1, updated_at=now() WHERE job_kind='sync.provider_unit'` — matches the already-checked-in policy in `contracts/jobs/v1/migration-state.json` (`"route": "river_canary"`, from CHAOS-3123), not a new state.
2. `WORKER_GITHUB_REPO_METADATA_ENABLED=true` on `dev-health-worker-1` (and anywhere else `dispatch_sync_run` runs), which requires recreating that container with the new environment — wire the variable into the repo-root `compose.yml`'s Python service blocks first, the same way it's already wired for `go-worker`.
3. `go-worker` running and ready (see the crash-loop section above for what that needs).

Revert both (1) and (2) back to `celery`/`false` afterward — this is a
canary capability being exercised on demand, not a standing cutover.

### Credential shape, restated for this report

See "Provider credentials the Go executor can decrypt" above for the full
detail. The two gotchas worth restating here because they are exactly what
blocked the isolated-project proof until found: the decrypted plaintext
must be a JSON object (`{"token": "..."}`), not a bare token string; and the
claim query resolves `credential_id` via `COALESCE(sync_runs.credential_id,
integrations.credential_id)`, so a credential row that exists but isn't
linked from `integrations.credential_id` is invisible to a claim even
though direct `(org_id, provider)` lookup would find it.
