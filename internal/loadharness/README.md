# Auth load, abuse and fault runs

`cmd/loadtest` measures, `cmd/faultproxy` injects network faults, and the
profiles in `testdata/abuse/` say what each run must see. Nothing here is
shipped.

## Where a run may happen

- Fault runs: a kind cluster (`isolated-env-kind`) or a throwaway compose
  project with its own `-p` name, own network and ports, torn down after.
  Never the shared dev stack, never prod.
- Abuse-only runs (no faults): the shared dev stack at modest rates. The login
  limiters count per client address for 15 minutes, so a burst run locks that
  address out of login for the window.
- The host guard of both tools refuses any host that is not local, `auth-service`
  or named with `--allow-host`.
- Credentials come from env var names only (never argv, never a file in the
  repo): `AUTH_LOADTEST_ISSUER_CREDENTIAL`, `AUTH_LOADTEST_PRINCIPAL_ID`,
  `AUTH_LOADTEST_REFRESH_CREDENTIAL`, and `AUTH_LOADTEST_REFRESH_POOL` (comma
  list). Check a variable by name only: `[ -n "${NAME:-}" ] && echo set`.

## Profiles and the status each one expects

Expected statuses come from `internal/auth/authapi` (login, refresh, issue,
limits). A run that sees anything else fails with `ASSERTION FAILED` and exit 1.

| Profile | Request | Must see | Hard asserts |
|---|---|---|---|
| `login-badcreds.json` | wrong password, new account each request | 401, then 429 at the address limit | never 200, never 5xx |
| `burst-limit.json` | wrong password, one account | 401 and 429 (account limit 10, address limit 20 per 15 min) | never 200, never 5xx |
| `oversize-body.json` | login with a 2 MiB body | 413 | never 200, never 5xx |
| `forged-credentials.json` | garbage, JWT-shaped and wrong-class bearer on `GET /v1/sessions/current` | 401 | never 200, never 5xx |
| `refresh-replay.json` | the same refresh credential again and again | one 200, then 401 | never 5xx |
| `refresh-storm.json` | refresh from a credential pool | 200 or 401 | never 5xx |
| `fault-mix.json` | JWKS, issue, refresh and bad-credential login together | see the fault table | never 200 for bad credentials, never 500 on issue or refresh |

`refresh-replay`: read the exact count with
`jq '.paths[] | select(.name=="refresh-replay") | .statuses' report.json`; the
`200` count must be 1.

## Faults

The proxy sits between a service and one dependency. Run it next to the
service, point the service at the proxy address, and start `loadtest` and
`faultproxy` back to back so the fault window offsets line up (skew up to one
second; keep recovery bounds at 5 s or more).

```sh
faultproxy --listen 127.0.0.1:15432 --upstream postgres:5432 --allow-host postgres \
  --schedule 20s=blackhole,50s=pass --events fault-events.jsonl
loadtest --config internal/loadharness/testdata/abuse/fault-mix.json --profile normal \
  --duration 90s --fault-name postgres-blackhole --fault-on 20s --fault-off 50s \
  --fault-max-err 0.05 --fault-sustain 3 --recover-within 30s --out report.json
```

Modes: `pass`, `delay:<latency>`, `slow:<bytes per second>`, `drop` (reset),
`cut` (close live connections, refuse new), `blackhole` (read and discard). A
mode can also change at runtime through the loopback control endpoint:

```sh
faultproxy --listen 127.0.0.1:16379 --upstream valkey:6379 --allow-host valkey --control 127.0.0.1:19000
```

then `curl -X POST '127.0.0.1:19000/mode?name=cut'` and
`curl 127.0.0.1:19000/stats`.

| Fault | How | Expected from the code (record what is seen) |
|---|---|---|
| Postgres down or slow | proxy between auth-service and the database (cut, blackhole, delay) | issue and refresh answer 503 (store unavailable); JWKS is served from the loaded key and should keep answering 200 |
| Valkey (limiter store) down | proxy on the limiter store | a limiter store error answers the generic 500 and stops, never a pass (go-api parity): login asserts "never 200", the 500 is recorded as the observed fail-closed answer |
| Key store | the key is a file read at start (`AUTH_SIGNING_KEY_FILE`, volume `auth_signing_keys`): restart auth-service in the throwaway project with the key unreadable | the service should refuse to start and `/readyz` should fail while running instances keep serving; record both |
| Outbox | the publisher is off today (`devhealth_auth_outbox_publisher_enabled` = 0): no sink to fault | record "no sink: n/a" unless the test stack enables the publisher |
| Network | proxy delay or drop on the load-test-to-service hop and on each service-to-dependency hop | latency and error rate per window |

## ADR-03 access TTL (10 min against 60 min)

A 10-minute access token means six times the refresh rate. Run `refresh-storm`
at six times the normal refresh rate and compare with the objective:

```sh
loadtest --config internal/loadharness/testdata/abuse/refresh-storm.json --rate 120 \
  --duration 120s --concurrency 64 --slo refresh:p95=200ms,err=0.01 --out ttl-report.json
```

Verdict line for the record: confirms the 10-minute TTL if the SLO holds, and
falsifies it with the first window where it does not.

## Record for the decision log

One entry per run: fault or abuse profile, venue (kind or compose project
name), offered rate and duration, `time_to_degrade_ms`, `time_to_recover_ms`,
the status histogram per path, `unexpected_5xx`, `first_seen_ms` of the first
429, the assertion lines, and for the TTL run the verdict line. The report
holds names, counts and timings only; keep it that way.
