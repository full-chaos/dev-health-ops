# ci/bigboy — tracked bigboy venue tooling

Tracked home of the bigboy-cut.sh script family and the bigboy compose file
(CHAOS-6964, CHAOS-6977). The host's own operational copies live in `_records/` on the bigboy
host and are kept byte-identical to this directory by hand at every change (there is no automated
sync yet).

## Operating the stack: ONE compose file (CHAOS-9962)

The bigboy stack is one compose file, `ci/bigboy/compose.bigboy.yml`, with profiles for the optional services, started with
plain `docker compose` (the earlier `stack.sh` over twelve overlay files is retired; the file's header says what each of the
twelve was). The project directory is the bigboy root, the env files are `ops/.env` and the pins file, the project is `dev-health`:

```
cd "$BIGBOY_ROOT"      # /home/ubuntu/devhealth, the running tree (the checkout is ops/)
docker compose -p dev-health --project-directory "$BIGBOY_ROOT" \
  --env-file ops/.env --env-file _records/images.env \
  -f ops/ci/bigboy/compose.bigboy.yml \
  up -d --no-deps --no-build query-api go-api web
```

(`--profile` only for the optional services, e.g. `--profile smoke run --rm --no-deps -T web-smoke`; none for the live stack.)

| What | Command (after the lines above) |
|---|---|
| render, read-only | `ci/bigboy/compose-config-redacted.sh <the arguments above>` (NAME=<length> only; never a bare `config` into a pipe) |
| status, logs | `ps`, `logs --tail=200 <service>` |
| start or recreate a service | `up -d --no-deps --no-build <service>...` (never the whole stack) |
| one-off container | `run --rm --no-deps -T <service> [cmd...]` |
| the optional services | add `--profile smoke` (web-smoke), `--profile venue` (venue-prove, venue-tools), `--profile venue-hook` (venue-hook) or `--profile venue-river` (venue-river) |

Do not export `COMPOSE_FILE`, `COMPOSE_PROFILES` or any other `COMPOSE_*` variable: the cut scripts unset them so nothing adds a file
or a profile. `docker compose` interpolates every service of the file, also those of a profile that is not enabled, so a variable a
one-off needs is read with an empty default (`${PROVE_ORG:-}`) and the script that starts the one-off checks it
(web-path-smoke.sh, bigboy-graphql-prove.sh); only the variables the live stack needs are required (`:?`): the three image pins
and `DHO_API_CH_USERS_XML` (the host file mounted into ClickHouse's users.d), all four named in `_records/images.env`, which the
command passes. `BIGBOY_OPERATOR_IMAGE` alone has a default, the placeholder tag `:not-resolved`, so a plain `ps` or `config`
renders without it and a worker cannot start from it (the pull fails); the cut exports the real digest and repin writes it into
`_records/images.env`.

**Pins.** The digests of the four images (`BIGBOY_DHO_IMAGE`, `BIGBOY_API_TOOLS_IMAGE`, `BIGBOY_WEB_IMAGE`, `BIGBOY_OPERATOR_IMAGE`) live
in `_records/images.env`, the one file a cut rewrites (`bigboy-repin*.sh`; they also write the operator pin, so a plain
`docker compose ps` needs nothing exported). The tracked `ci/bigboy/images.env` is the last pin a PR recorded; copy it to
`_records/` to start a bigboy root. `BIGBOY_OPERATOR_IMAGE` is absent from the tracked copy: the cut exports it, and repin writes it into `_records/images.env`.

**First use on a root that still has the old pin file** (read-only until the last line; GWC owns the switch window): the new file renders
the live `stack` byte-identically (so no container is recreated by the switch itself), but the pins now come from an env file. Make
`_records/images.env` from the digests the old `_records/compose.bigboy.images.yml` pins, with grep only (no value is a secret):

```
cd "$BIGBOY_ROOT"
for pair in BIGBOY_DHO_IMAGE:dev-health-go-dho BIGBOY_API_TOOLS_IMAGE:dev-health-go-api-tools BIGBOY_WEB_IMAGE:dev-health-web BIGBOY_OPERATOR_IMAGE:dev-health-go-operator; do
  var=${pair%%:*}; img=${pair##*:}
  ref=$(grep -ohE "ghcr.io/full-chaos/$img@sha256:[0-9a-f]{64}" _records/compose.bigboy.images.yml | head -1) || true
  [ -n "$ref" ] && echo "$var=$ref"
done > _records/images.env
printf 'DHO_API_CH_USERS_XML=%s\n' "$BIGBOY_ROOT/.go-api-dev/dho_api_ch.xml" >> _records/images.env
```

then compare the render (names only: `ci/bigboy/compose-config-redacted.sh` with the arguments above) before running any `up`.

What `stack.sh` did and is gone: it refused any verb but a short list (now: type the command above), it refused to start a worker
without `BIGBOY_OPERATOR_IMAGE` (now compose refuses: the variable is required), and it appended one line per mutating verb to
`_records/stack-runs.log` (not kept: the cut's own `STEP` lines and `$REC/*.out` are the record).

## The acr venue stack (CHAOS-10004)

The acr pre-prod venue (acr-api, acr-projector, acr-mcp, its postgres and falkordb and the one-offs that
prepare the database) is in the same file, in the same project, started with the same command and a profile:

```
cd "$BIGBOY_ROOT"
docker compose -p dev-health --project-directory "$BIGBOY_ROOT" --env-file ops/.env --env-file _records/images.env \
  -f ops/ci/bigboy/compose.bigboy.yml --profile acr up -d --no-deps --no-build <service>
```

- `acr-db-init` and `acr-db-acl` run `acr-db-init.sh` of the acr commit in `ci/bigboy/acr-db-init.pin` (ops `ci/bigboy/images.env` `BIGBOY_ACR_COMMIT`, the acr of the acr-api pin). Before any compose command that names them, and after every acr repin, run `bash ops/ci/bigboy/acr-db-init-fetch.sh`: it fetches the file at that commit, refuses a sha256 other than the pin's, and writes it to the path `ACR_DB_INIT_SCRIPT` of the pins file (`/var/lib/oci-cache/mcp-venue/acr-db-init/acr-db-init.sh`). A host's `_records/images.env` must hold the `ACR_DB_INIT_SCRIPT` and `BIGBOY_ACR_COMMIT` lines, or compose fails and names the variable. An acr repin updates the pin file (both lines) and `BIGBOY_ACR_COMMIT` together; a gate test compares the two commits.
- `--profile acr` holds every acr service. `--profile acr-venue` holds only what must exist before acr-api: `acr-postgres`, `acr-db-init`
  (roles), `acr-migrate` and `acr-db-acl` (grants). Pass both when you want to name no service.
  Services do not clash with the dev-health ones: the database and the graph are `acr-postgres` and `acr-falkordb`, answering to the
  names `postgres` and `falkordb` on the acr network only.
- Data does not move. The bind mounts are the old paths, `/var/lib/oci-cache/mcp-venue/postgres-data` and `.../falkordb-data`; the network is `dev-health-acr`. `container_name` is the old project's
  (`dev-health-acr-mcp-<service>-1`). acr-api and acr-projector reach `dho-query-internal` and `dho-clickhouse-readers` as before; no acr
  service is on the `dev-health` network. The data URLs use the alias `query-api`, never an address.
- Secrets are files under `/var/lib/oci-cache/mcp-venue/secrets/`, mounted by compose as `/run/secrets/<name>`; the file holds the paths
  (overridable by the `ACR_*_FILE` variables) and no value. Non-secret settings (`ACR_DB_NAME`, the role names, the projector org) default to
  what the old `.env` held.
- Staging acr host: `acr.commanderkeen.dev` reaches acr-api through the edge traefik (`:3000`), by the docker labels on `acr-api` (router `acr-staging`). acr-api publishes no port (only the MCP server and the graph do). The two meet on their own network `dev-health-acr-edge`; the edge traefik joins it (`docker network connect dev-health-acr-edge dev-health-traefik-1`, and the same network in the host compose file of traefik), so traefik never joins the `acr` network and acr-api never joins the ops network.
- FalkorDB on the host (CHAOS-10942): `acr-falkordb` publishes `127.0.0.1:16379` (loopback only, never every address). It requires a password: the default user (used by acr-api and acr-projector through `ACR_CONTEXT_FABRIC_FALKOR_PASSWORD_FILE`) and a read-only user `venue_ro` (`GRAPH.RO_QUERY`, `GRAPH.LIST`, `PING`, `HELLO`, `AUTH` only; read-only by command allow-list, key pattern `~*`: `GRAPH.RO_QUERY` declares RW key flags, so a read-only key rule `%R~*` gets NOPERM on every `GRAPH.RO_QUERY` read) come from an ACL file mounted as a secret. The `venue_ro` ACL line is `user venue_ro on #<sha256 hash> ~* &* -@all +ping +hello +auth +graph.ro_query +graph.list +client|setname +client|setinfo`. Host files in `/var/lib/oci-cache/mcp-venue/secrets/` (mode 0600, owner 1001, never committed; paths overridable by `ACR_FALKORDB_PASSWORD_FILE` and `ACR_FALKORDB_ACL_FILE`): `falkordb_password` (default user, mounted into acr-falkordb, acr-api, acr-projector), `falkordb_venue_ro_password` (the venue reader; mounted nowhere), `falkordb_acl` (sha256 hashes only). Every `redis-cli` against the graph needs `REDISCLI_AUTH` read from the secret file (the switch procedure below shows the form; the reader adds `--user venue_ro` and its own password file). The files share uid 1001 with every process of the stack's operators: the password stops accidental writes, it is not an OS boundary. A change of the port, the ACL or the password recreates `acr-falkordb`, `acr-api` and `acr-projector` (and nothing else): `... --profile acr up -d --no-deps --no-build acr-falkordb acr-api acr-projector`.
- Images: all pinned by registry digest in the pins file (`BIGBOY_ACR_*_IMAGE`, each REQUIRED: with one missing every compose command fails and names it, so a host's `_records/images.env` must hold the acr lines before its checkout holds this file). acr-api (which also
  runs the projector and the migrations) and acr-mcp are the images of the acr release build, `ghcr.io/full-chaos/dev-health-acr/acr-api` and `.../acr-mcp`. A build
  on this host needs `--build-arg VERSION=0.1.0 --build-arg COMMIT=<full sha>`; without both args acr-api refuses the sidecar with a 426.
- acr-api talks to the real ops, as the venue does after its move off the stand-in: `ACR_DEV_HEALTH_ENTITLEMENT_URL` defaults to `http://go-api:8091` (the go-api internal listener, by alias, on `dho-query-internal`) and the bearer is a workload token (CHAOS-12976, the prod shape): `ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=workload`, and acr-api exchanges its Compose bootstrap credential at auth-service (`ACR_AUTH_WORKLOAD_*`, the section "auth venue" below). There is no service token in the venue. BYO LLM config (CHAOS-12997): acr reads an org's BYO config from ops with the same workload token (scope `llm_config:read`); the `ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE` setting was removed from acr and the compose file no longer passes it. The entitlement stand-in is removed (CHAOS-12976): prod has none (deploy `values.acr-prod.yaml`: "No stand-in on prod"), and it checked a static bearer. On the roll, its container `dev-health-acr-mcp-acr-entitlement-standin-1` and the volume `dev-health-acr-entitlement-tmp` are left as orphans: remove them by exact name (`docker rm -f dev-health-acr-mcp-acr-entitlement-standin-1`, `docker volume rm dev-health-acr-entitlement-tmp`).
- The one-time move from the old project `dev-health-acr-mcp` to this file is a recreate of the acr stack (new project label, same container
  names and data). Run it only in a window MCP opens. The new file fixes the old project's container names, so the old containers must be
  REMOVED, not only stopped (a stopped container keeps its name and the new `up` fails with `Conflict. The container name ... is already in use`;
  executed in a scratch project). The steps, in this order:
  1. Before: render the acr services from the old `compose.yml` and from this file and diff them (names, images, environment, volumes, networks):
     only the image reference, the project-prefixed secret/volume/network names and the services' network name may differ.
  2. Save the graph first (MCP): `docker exec -e REDISCLI_AUTH="$(cat /var/lib/oci-cache/mcp-venue/secrets/falkordb_password)" dev-health-acr-mcp-falkordb-1 redis-cli BGSAVE`, wait until the same command with `LASTSAVE` changes, then copy the dump to a dated backup
     path, `cp /var/lib/oci-cache/mcp-venue/falkordb-data/dump.rdb /var/lib/oci-cache/mcp-venue/falkordb-data.backup-$(date -u +%Y%m%dT%H%MZ).rdb` (outside the bind-mounted directory).
     Note the repository count for step 5: `docker exec -e REDISCLI_AUTH="$(cat /var/lib/oci-cache/mcp-venue/secrets/falkordb_password)" dev-health-acr-mcp-falkordb-1 redis-cli GRAPH.QUERY <graph> "MATCH (r:Repository) RETURN count(r)"`, the `<graph>` being
     the one the same command with `GRAPH.LIST` shows besides `_acr_health` (34 on 2026-10-17).
  3. Remove the old project's containers and its own network, keeping the data: `cd /home/ubuntu/devhealth/dev-health-acr-mcp && docker compose -p dev-health-acr-mcp -f compose.yml down`
     (NEVER `-v`, and never `--rmi`). The bind mounts `/var/lib/oci-cache/mcp-venue/postgres-data` and `.../falkordb-data` are host paths and stay; the networks
     `dho-query-internal` and `dho-clickhouse-readers` are external and stay.
  4. Set the projector switch in bigboy's env BEFORE the `up`: `ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true` must be a line of the host's `_records/images.env` (the tracked
     `ci/bigboy/images.env` has it; the compose file defaults it to false, as the old file did, and the projector writes nothing without it). Then start ONLY the acr services (eight since CHAOS-12976 removed the entitlement stand-in),
     by name (a bare `--profile acr up -d` also starts or recreates every default-profile dev-health service, 42 containers instead of 9 at the move, checked with a dry-run):
     `cd "$BIGBOY_ROOT" && docker compose -p dev-health --project-directory "$BIGBOY_ROOT" --env-file ops/.env --env-file _records/images.env -f ops/ci/bigboy/compose.bigboy.yml --profile acr up -d --no-build acr-postgres acr-db-init acr-migrate acr-db-acl acr-falkordb acr-api acr-projector acr-mcp`
     (their `depends_on` stays inside the acr profiles, so a dry-run of exactly this command creates the acr containers and nothing else). Then `ps`, and confirm the images are the pinned digests: `docker inspect --format '{{.Image}}' dev-health-acr-mcp-acr-api-1 dev-health-acr-mcp-acr-mcp-1`
     must print the ids in `BIGBOY_ACR_API_IMAGE` and `BIGBOY_ACR_MCP_IMAGE` of the host's `_records/images.env` (fb4c745e: `01cab48f...` and `38aeb351...`).
  5. Readback: the venue answers, and the graph still holds the repository count noted in step 2 (34): run the same
     `docker exec -e REDISCLI_AUTH="$(cat /var/lib/oci-cache/mcp-venue/secrets/falkordb_password)" dev-health-acr-mcp-falkordb-1 redis-cli GRAPH.QUERY <graph> "MATCH (r:Repository) RETURN count(r)"` again and compare. Also check that the host's
     `_records/images.env` carries `ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true` (the file defaults it to false; the projector writes nothing without it).
  6. After the readback, the old named volume `dev-health-acr-mcp_acr_entitlement_tmp` is left behind on purpose (it only held the stand-in's generated
     nginx.conf): remove it with `docker volume rm dev-health-acr-mcp_acr_entitlement_tmp` when the
     readback is good, or leave it.

## auth venue (CHAOS-12976)

The bigboy venue has the prod target shape of the acr-api -> go-api credential: auth-service issues acr-api a workload token, and go-api
verifies it. Profile `auth` of `compose.bigboy.yml` holds the services of ops `compose.yml`'s `auth` profile, the acr-api bootstrap pair of acr
`deploy/compose/acr-auth-workload.compose.yml`, and `auth-issuer` (the https origin of the issuer). Test values only: the signing key is made in
the venue, the TLS pair is a host test pair, the database logins have compose.yml's local passwords. No prod secret is used.

- Issuer: `https://auth.bigboy.internal:8443`, one literal in auth-service (`AUTH_TOKEN_ISSUER_URL`), go-api (`AUTH_ACCESS_TOKEN_ISSUER`) and
  acr-api (`ACR_AUTH_WORKLOAD_ISSUER`). go-api fetches the discovery document and the JWKS from it over https only; `auth-issuer` (alias
  `auth.bigboy.internal` on `dev-health`) forwards exactly those two documents to auth-service, as prod's issuer host does.
- go-api trusts the issuer certificate through `SSL_CERT_DIR=/run/auth-issuer-ca` (the `ca/` directory of the TLS dir, read-only). Not
  `SSL_CERT_FILE`: it would replace the image's CA file for every outbound call. go-api still accepts `legacy_hs256` tokens
  next to the eddsa class (eddsa section below).
- Networks: auth-service and the one-shots are on `auth_plane` (postgres joins it); acr-api reaches auth-service on `acr_auth_workload`
  (only those two); acr-api never joins `auth_plane`. auth-service is also on `dev-health`, so go-api reaches it at `AUTH_PROXY_URL=http://auth-service:8095`.
- eddsa (CHAOS-12978): auth-service mints the eddsa access token (`AUTH_ACCESS_TOKEN_CLASS=eddsa`, `AUTH_JWT_AUDIENCE=dev-health-api`), signed with the one
  `AUTH_SIGNING_KEY_FILE` key. go-api, query-api and web verify it against the same issuer (`AUTH_ACCESS_TOKEN_ISSUER`; web also `AUTH_ACCESS_TOKEN_CLASS=eddsa`).
  query-api trusts the issuer CA through `SSL_CERT_DIR`, web through `NODE_EXTRA_CA_CERTS=/run/auth-issuer-ca/issuer.pem`. Sessions move to auth-service:
  `AUTH_PROXY_SESSIONS=true` on go-api and query-api, and `AUTH_SESSION_READER_ROLES=devhealth_api,devhealth_query_api` on auth-migrate and auth-grants.
  The session successor key is made in the venue by `auth-session-keys-init` (0600 file, volume `auth_session_keys`, `AUTH_SESSION_SUCCESSOR_KEY_FILE`); the
  JWT key of auth-service is go-api's `JWT_SECRET_KEY` from `ops/.env` (one source). go-api asks for sessions with its workload token
  (`AUTH_SESSION_ISSUE_CREDENTIAL=workload`, bootstrap file `/run/workload/api.bootstrap` from `go-api-workload-bootstrap`, uid 65532, read-only mount).
- The venue acr-api runs as uid 1001, so `acr-workload-volume` gives `/run/workload` to 1001 (mode 700) and `acr-workload-bootstrap` writes
  the 0600 credential as 1001. acr-api mounts the volume read-only.

Bring-up, in this order (every command names its services; `-p dev-health ... --profile auth` as in the first section):

1. Pins: `_records/images.env` holds `BIGBOY_AUTH_SERVICE_IMAGE`, `BIGBOY_AUTH_MIGRATE_IMAGE` (required) and an acr-api pin of acr d168fa95
   or later (the workload credential, acr CHAOS-11203).
2. TLS test pair, once, as ubuntu (1001), under `BIGBOY_AUTH_ISSUER_TLS_DIR` (default `/var/lib/oci-cache/mcp-venue/auth-issuer-tls`):
   `mkdir -p -m 0700 "$D/key" && mkdir -p -m 0755 "$D/ca" && openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 365 -subj "/CN=auth.bigboy.internal" -addext "subjectAltName=DNS:auth.bigboy.internal" -keyout "$D/key/issuer.key" -out "$D/ca/issuer.pem" && chmod 0600 "$D/key/issuer.key" && chmod 0644 "$D/ca/issuer.pem"`
   (`D` = the TLS dir). Both binds have `create_host_path: false`: a missing file fails the start instead of mounting a new directory.
3. postgres joins `auth_plane`: it is recreated, in its own step (a data store restart).
4. `--profile auth up -d --no-deps --no-build auth-keys-init auth-session-keys-init auth-provision auth-owner-grants auth-migrate auth-grants auth-fence-init auth-workload-seed`
   (one at a time, each must exit 0), then `--profile auth up -d --no-deps --no-build auth-service auth-issuer`.
5. Once: `--profile auth run --rm acr-workload-bootstrap` (runs `acr-workload-volume` first). It prints the credential id and its
   `expires_at`; re-create before that time (a second `run` refuses while a credential is live: revoke it first with
   `auth-migrate workloads bootstrap-revoke -credential <id> -reason owner_request -apply`, or rotate with `bootstrap-rotate`). It is never a `depends_on`.
6. The subject: nothing to write. go-api reads the principal of the workload acr-api from the workload registry (the row `auth.workloads`
   `name = 'acr-api'`); the venue grants its role that read (`AUTH_WORKLOAD_READER_ROLES=devhealth_api` on auth-migrate and auth-grants).
   No live acr-api row, or an unreadable registry, and go-api refuses every workload token on the internal acr routes.
7. Once: `--profile auth run --rm go-api-workload-bootstrap` (runs `go-api-workload-volume` first), then read the principal of the workload `api` as in step 6
   (`WHERE name = 'api'`) and write it to `ops/.env` as `AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=prn_...`. Recreate auth-service (`--profile auth up -d --no-deps --no-build auth-service`).
8. Recreate go-api (`up -d --no-deps --no-build go-api`; it needs the api bootstrap file from step 7 to start), query-api and web, then acr-api (`--profile acr up -d --no-deps --no-build acr-api`).
9. Platform roles (CHAOS-13282): auth-service has `AUTH_PLATFORM_ROLES_OWNED=true` in `compose.bigboy.yml` (the prod form: the PDP reads the role from `auth.platform_role_assignments`, not `users.is_superuser`), so a plain `up` keeps it.
10. Users owned (CHAOS-13356): every service runs with users owned, as prod's phase 4: `AUTH_USERS_OWNED=true` on auth-service, go-api, the auth one-shots and `auth-users-backfill`. go-api forwards sessions, local login and credentials (`AUTH_PROXY_SESSIONS`, `AUTH_PROXY_LOCAL_LOGIN`, `AUTH_PROXY_CREDENTIALS`; it refuses the owned switch without the three); auth-service reads the login lockout key from `/run/auth/session/login-lockout-key` (made by `auth-session-keys-init`, 0600) and sends the verify and reset mail over SMTP to the venue's own `mailpit` service (`EMAIL_PROVIDER=smtp`, `SMTP_HOST=mailpit`, `SMTP_PORT=1025`, no TLS; read the mail at `mail.localhost`). `AUTH_MAIL_ALLOW_CONSOLE=true` stays only so the venue's http `APP_BASE_URL` is accepted. Recreate auth-session-keys-init, then auth-service, then go-api. The seed tool is `dho auth users-backfill -insert-only` (the one-off `auth-users-backfill`, users and platform roles owned): it creates the auth users that are missing and never changes one or grants a role.
11. The drift-S control (moved to the platform roles by CHAOS-13356): `BIGBOY_DRIFT_S_ID=<uuid of a user principal with no platform role> ci/bigboy/bigboy-platform-admin-fixture.sh` runs the seed tool, then proves the control: it reads the active platform roles from `auth.platform_role_assignments` (the platform role store's active rule) and is green when drift-S holds none and another principal holds `platform_admin`. The fixture plants the drift directly in that table (an active `platform_admin` row for drift-S, in one transaction that sets the audit commit guard's `dev_health.audit_txid`; no go-api route, no audit row), wants the control red, clears it (deletes the drift-S rows), reads back 0 rows and wants the control green again; the trap clears a plant on every exit. `... bigboy-platform-admin-fixture.sh check` runs the control alone (exit 20 when red). It is idempotent; a drift-S row left from an earlier run stops it (exit 11) until it is deleted.

## Go-only stack (CHAOS-8361)

The bigboy stack runs no Python api, as prod does. The base `compose.yml` of the stack is a host file
(not in this repository), so the tracked files do the work:

- `compose.bigboy.yml` moves the base file's `api` service to the profile `retired-python-api`,
  which nothing enables; states web's `depends_on` again without `api`; runs `migrate` from the dho
  image (`dho migrate upgrade --river`); and puts `venue-prove` on the stack's networks (it shared the
  api container's network namespace). No service names the Python api image.
- The cut's chain no longer holds the host side file `compose/compose.metrics-api.local.yml` (the Python
  `metrics-api` service), and the cut's `up` list is `query-api go-api web`.
- `compose.bigboy.yml` runs the web-path smoke as `dho smoke web-path`; its image is the dho image
  by digest, set in `compose.bigboy.yml` with the other dho pins.
- `bigboy-graphql-prove.sh` proves through the routed `/graphql` in Go-edge mode only.
- The router file names no Python backend: `/docs`, `/redoc`, `/openapi.json` and `/metrics` reach the
  default backend (the Go api's 404), as in prod. Regenerate `.traefik-dynamic/planes.yml` with the cut.
- The cut no longer runs the two-plane legs of the record directory (the proof-token bootstrap, the
  `pass-bigboy*.sh` scripts, `run-rest-bigboy.sh`): each ran inside the Python api container and
  compared the Go plane with the Python plane. The tracked `pass-bigboy-admin2` and `pass-bigboy-auth`
  scripts were such passes and are deleted. The cut's pass is: route coverage, the routing ledger
  steps, the log and worker checks, river apply, the hook check, the grants checks and the web-path
  smoke (R460).
- The envelope key files are unchanged: query-api and the venue one-offs read `.go-api-dev/`
  (`envelope-jwks.json`, `envelope-private-key.pem`). The base file declares no key init service, so
  `--no-deps` on the cut's `up` skips none.

ORDER for the first cut with these files (the same order the billing-edge retirement used): (1) put
the tracked `images.env` in place of the pin file `_records/images.env`,
keeping the digests the host copy has; and repin the dho digests to a build that has the `dho smoke web-path` verb (the smoke works only with both); (2) with the OLD chain still exported, name and remove the two
Python containers with compose verbs (`docker compose ... ps api metrics-api`, then
`docker compose ... rm -sf api metrics-api`; one approved line, run on the lead's GO); (3) regenerate
`.traefik-dynamic/planes.yml`; (4) run the cut.

## Plane-split router (CHAOS-6987, D2724)

Bigboy has no k8s Ingress, so unlike prod it had no mechanism to route go-served REST paths
(`ingress.goApiPaths`/`ingress.queryApiPaths` in deploy's `values.prod.yaml`) to `go-api`/`query-api`
directly -- every request went through the Python `api` service, which 500s unconditionally on any
route whose Python body has been deleted (CHAOS-6241 class). This surfaced as a real P1: chris's
real-org dashboard showing "no data" through the `commanderkeen.dev` Cloudflare tunnel into bigboy.

Fix: `cigate plane-split-router` (Go, `internal/cigates/routing`; it was `generate-plane-split-router.py`
until CHAOS-10346, and its output is byte-identical) derives a traefik file-provider dynamic config (routers +
services, one per plane) straight from `ingress.goApiPaths`/`ingress.queryApiPaths` -- never
hand-maintained. `compose.bigboy.yml` adds traefik's `--providers.file.directory` flag +
volume mount for that generated file, and points `web`'s `BACKEND_URL` at the router
(`http://traefik:3000`) instead of straight at `api:8000`, so both browser-origin traffic
(`commanderkeen.dev`) and web's own server-side calls get the same split.

Regenerate after every `ingress.goApiPaths`/`ingress.queryApiPaths` change:

```
go run ./cmd/cigate plane-split-router <deploy-repo>/values.prod.yaml --format dynamic \
  > .traefik-dynamic/planes.yml
```

then recreate ONLY `traefik` (file-provider watch picks it up live, no restart strictly required,
but a clean recreate after a real deploy-repo change is the safe default).

### One path, one backend

The generator REFUSES (exit 4, nothing emitted) a values file in which a path the Python api is
given and a path a Go plane is given can match the same request. On bigboy the Go plane would win
by priority while prod's ingress controller picks by its own rule order, so bigboy would prove a
route prod may not have. The check is on the paths the rules match, not on the text of an entry:

- It reads every Python source: `ops.ingress.pythonAllowList`, each host's own `pythonAllowList`
  list (prod's in-cluster host carries one; only prod routes it, and a flip that forgets it is
  half a flip), and the local-only paths this router adds (`BIGBOY_LOCAL_PYTHON_PATHS`, empty since
  CHAOS-8361: bigboy has no Python api).
- A `Prefix` is read at its widest: every path that starts with its text. That is what
  ingress-nginx renders for a Prefix on a host in regex mode, and it contains this router's own
  reading (the path, or anything under it).
- A Go plane wildcard (`{param}`, `[^/]+`) never crosses a `/`, so the answer is exact.
- An allow-list entry may name its backend: `service: query-api`. The router then sends that
  path to query-api and leaves it out of the Python rule, and the entry counts as a Go plane path
  in this check, on whichever list it is: the same path still on Python on another list is half
  a change and is refused. Such an entry must be ANCHORED (`ImplementationSpecific`,
  `/literal$`, no `.` or `..` segment): the one shape that is one path on every host. An
  `Exact` or `Prefix` entry is refused: the chart reads those by the host they are on.
- A second refusal, `one path, a backend that differs by host`. This router has one host and
  prod has several, so it can prove the backend of such a path only when every host that
  serves the api gives it that backend. Each host of `ops.ingress.hosts` must be one of two
  kinds: a host that carries the entry on the allow-list it uses (the shared one if it opts in
  with `pythonAllowList: true`, or its own) and has no path of its own beside `/`; or a web
  host (all its paths go to `web`). Any other host is refused. A list that no host uses is not
  read: the chart renders nothing from it.
- This generator does not read the chart's rules a second time for such an entry: a shape it
  would have to read the way the chart does is refused. The shape in use passes: a public api
  host on the shared list, an in-cluster api host with its own list, a web host.
- One more refusal, `one path, two Ingress objects`: an entry that names query-api while
  `ingress.goApiPaths` or `ingress.queryApiPaths` also claims the path. Prod renders the
  allow-list in one Ingress object and the path tables in others, and its ingress admission
  denies two live objects for one host and path.
- An allow-list entry of a shape the ops chart does not accept (an unanchored
  `ImplementationSpecific`, an unknown `pathType`) is refused too, not read as a literal.

A Go plane path with a character that needs a regex escape (a dot, for one) is refused as well
(exit 5): its rule cannot be written in the router file, the file would not load, and traefik would
keep the old router with no failing step.

The refusal names the list, the entry and the Go plane path. A values change that moves a path
to a Go plane takes it off every Python list in the same commit. The router file names a Python
backend only when an allow-list entry of the values names the Python api; the prod values have no
such entry.

### D2733 regression + fix: `go-api-paths`/`query-api-paths` must match `Host(traefik)` ONLY

The first version of this router matched `Host(commanderkeen.dev) || Host(www.commanderkeen.dev)
|| Host(traefik)` on ALL three routers, on the theory that browser-origin and web's own
server-side traffic needed the identical split. That let a real browser's OWN client-side request
for any REST path in these lists (e.g. clicking an "Explain" button, opening a panel) bypass `web`
entirely -- and with it, `proxy.ts`, the ONLY place that turns the NextAuth session cookie into an
`Authorization: Bearer` header (`apiClient`'s `getServerAuthHeaders` deliberately returns `{}` in
the browser: "the backend remains the authentication boundary"). The request landed on
go-api/query-api with no auth carrier at all -- a real, reproducible P1 (CHAOS-6987 follow-up,
D2733): confirmed live, a bare unauthenticated request straight to traefik with
`Host: commanderkeen.dev` 401'd `/api/v1/work-units`, `/api/v1/investment` and `/api/v1/home`
*identically* -- proving it was never route-specific, only a question of whether that page's data
happened to be fetched server-side (`apiClient` DOES attach auth there) or client-side (it never
does, by design).

Fixed: `HOST_RULE = "Host(\`traefik\`)"` for `go-api-paths`/`query-api-paths` too, matching
`api-internal-catchall`'s already-correct pattern. A real browser request on `commanderkeen.dev`
now always lands on `web`'s own pre-existing docker-label router first (unaffected, since it has
no competing `Path` restriction), which runs `proxy.ts` and re-issues the request server-side via
`BACKEND_URL=http://traefik:3000` -- arriving back here as `Host: traefik`, now correctly
authenticated. Server-side calls (already `Host: traefik`, already carrying a real Authorization
header) are unaffected. Verified post-fix: the same bare-request probe now gets a 303 to
`/auth/signin` (proxy.ts's own designed unauthenticated redirect), and a real Authorization header
sent directly with `Host: traefik` still reaches `plane=go` correctly.

## query-api env: flags + secret refs (CHAOS-6967(b), CHAOS-6987/D2728)

`cigate query-api-flags` (was `generate-query-api-enabled-flags.py`) derives BOTH classes of env var query-api needs from
`ops.queryApi.extraEnv` in deploy's `values.prod.yaml`:
- `GO_API_*_ENABLED` literal flags (unchanged since CHAOS-6967(b)).
- `valueFrom.secretKeyRef`-backed names (`GO_API_EDGE_JWT_SECRET`, `LLM_PROVIDER`, `OPENAI_API_KEY`,
  `LLM_MODEL`, `SETTINGS_ENCRYPTION_KEY`), emitted as a compose `${KEY}` substitution using the same
  key name `ops/.env` already uses for every other service.

D2728 found the exact same silent-drift class that CHAOS-6967(b) named for the flags ALSO applied
to the secret refs: `GO_API_EDGE_JWT_SECRET` (and, once the generator was extended to check for it,
`LLM_PROVIDER`/`OPENAI_API_KEY`/`LLM_MODEL`/`SETTINGS_ENCRYPTION_KEY` too) were simply never set on
query-api at all -- a real user's JWT was rejected outright with no way to serve real dashboard data.

`bigboy-cut.sh`'s `query-api-enabled-flags-current` STEP now does two checks, both required for
`DEPLOY_SHA=<pinned deploy commit>` (see "Pinned deploy values" below):
1. Every NAME the generator emits (flags + secret refs) is present in the checked-in
   `_records/images.env` -- fails loud (STEP rc=1) naming exactly which name(s) are
   missing, rather than a silent 404/401 surfacing hours later through a real user.
2. Every secret-ref NAME resolves to a non-blank value through the LIVE, `--env-file`-substituted
   compose config -- fails loud (separate STEP `query-api-secret-refs-nonblank`, rc=1) if any
   resolves blank, which is exactly what a missing `--env-file ops/.env` produces.

## Suspected gap 4: ruled a mis-aimed probe (D2732), not a defect

A direct POST of a real end-user JWT straight to `query-api:8090/query` 401s. That is
`internal_auth.go`'s `authenticateInternalRequest` working as designed: `/query` accepts only the
internal-listener identity headers or a Bearer **envelope** token (Ed25519 JWKS at
`GO_API_ENVELOPE_JWKS_PATH`) -- never a raw end-user HS256 JWT. `GO_API_EDGE_JWT_SECRET` (D2728)
governs the browser-reachable `/api/v1/*` REST routes only, not `/query`. Confirmed live:
query-api's own log shows `envelope.rejected reason=bad_signature kid=""` for that probe, i.e. the
designed rejection, not a hang or a silent failure. The `*_database_configured`/`*_clickhouse_configured`
startup-log fields that looked suspicious are a parity question against prod's own query-api env
block (`values.prod.yaml`), not evidence of a defect on their own.

The real GraphQL path for a real user is through the edge (Python `api` `/graphql`, or web), which
either forwards an enabled operation with an envelope-signed dispatch or serves it itself.
Verified live: the bigboy routeswitch ledger (`dho goapi routing status`) shows 47/55 catalog
operations enabled (mode=canary, rollout=100; 8 MISSING -- the saved-reports family plus
testopsRisk); one real edge-forwarded op (`featureFlags`, the exact registered document text, a
real user JWT through Python api's `/graphql`) returned 200, `X-Dev-Health-Plane: go`, and real
rows for the real org; and the edge's own fallback behavior on a query-api rejection
(`go_api_dispatcher.py`) is confirmed LOUD by source -- `_forward_to_go` has no silent-fallback
branch, any non-200 goes through `_go_failed` (logged, counted, a typed GraphQL error), never a
quiet retry on Python.

## Web-path smoke (CHAOS-6987/R460)

`web-path-smoke.sh` (+ the `dho smoke web-path` verb in `internal/websmoke`, `compose.bigboy.yml`) is the one proof that
closes the loop for a REAL browser session, not a hand-minted token: it logs in through web's own
`/api/v1/auth/login` route as the bigboy admin, then issues the EXACT operations web's Cockpit
(`/api/v1/home`, `/api/v1/investment`, `/api/v1/opportunities` REST threads) and Diagnose
(`complexityTimeseries` GraphQL, `COMPLEXITY_TIMESERIES_QUERY` verbatim from
`web/src/lib/graphql/queries.ts`) pages send, through the plane-split router -- asserting 200 +
`X-Dev-Health-Plane: go` + non-empty rows for each, and writing a receipt (`_records/bigboy-<sha8>/
web-path-smoke-receipt.json` from `bigboy-cut.sh`, structural facts only -- status/plane/row
counts, never a token or a body value).

CHAOS-7190: it also sends the three seeded read documents (`HOME_QUERY`, `RECOMMENDATIONS_QUERY`,
`WORK_ITEM_TEAM_ATTRIBUTIONS_QUERY`, verbatim from the same file) and asserts 200 + `X-Dev-Health-Plane:
go` + no GraphQL errors + the expected `data.<op>` shape; a Python-plane answer fails as `<op>_plane_python`.
The web source mounted at `./web/src` must be the deployed web build (it must define those constants).

Credentials: `DHO_SMOKE_ADMIN_EMAIL` + `DHO_SMOKE_ADMIN_PASSWORD_FILE` (0600, the `*_FILE`
convention, CHAOS-6972) in `ops/.env`. **This STEP fails loud (rc=1, named reason) while either is
absent or resolves blank** -- presence/blank is checked through `compose-config-redacted.sh`
exclusively (NAME=length only), never a raw read of `ops/.env`. It does not run against the real
org until chris adds both entries.

## D2734: `web` needs `AUTH_URL` too -- same parity class as D2728, on `web` instead of `query-api`

A real user's logout redirected to `http://[::]:3000/` (Node's own listen bind) instead of
anywhere useful. `trustHost: true` is already set in `web/src/lib/auth.ts`, but Auth.js's default
callback-url cookie is computed from `AUTH_URL`/`NEXTAUTH_URL` when set, not purely from the
request's Host header -- and neither was set on bigboy's `web` container at all. Prod's own web
block (`deploy/values.prod.yaml:1015-1016`) sets `AUTH_URL: https://www.fullchaos.dev` explicitly,
with its own doc comment: "without it web derives its origin from the request URL... every
[Origin-checked] approval is refused" -- the exact same drift-without-a-generator-check failure
CHAOS-6987 already exists to close, just on a different service. Fixed: `AUTH_URL:
https://www.commanderkeen.dev` added to `web`'s environment in `compose.bigboy.yml`.
Verified live: `GET /api/auth/session` through the router now sets
`__Secure-authjs.callback-url=https%3A%2F%2Fwww.commanderkeen.dev`, not the bind address.

Prod's web `extraEnv` has five other entries (`ACR_API_ORIGIN`, `ACR_WEB_ASSERTION_*`) -- all
in-cluster-DNS/private-key-mount ACR wiring, which bigboy does not run (acr stays off bigboy per
standing rule) and would need its own volume/secret plumbing regardless. `AUTH_URL` is the only
bigboy-relevant entry in that list, so it is hand-maintained here with a citing comment rather than
adding a second full YAML-parsing generator for a single name; re-derive by hand from
`values.prod.yaml`'s web block if this list ever grows a second bigboy-relevant entry.

## Drift checks added after D2735/D2736 (CHAOS-6987 follow-through)

Every check below fails loud with a named finding; none can pass on an empty or unread input.

- **Names-only container env reader** (`container-env-names.sh`, R462/R463): the only sanctioned way
  to read a container's environment on bigboy. The name is cut inside docker's own Go template, so a
  value never leaves the docker CLI; any line that is not an env-var name is withheld and exits 3.
- **Router host scope** (`internal/cigates/routing/planes_test.go`; the checked-in example also in
  `internal/cigates/bigboy/compose_router_test.go`): every rule the generator
  can emit (file-provider and label form) and the checked-in example must be exactly
  `Host(`traefik`)` or `Host(`traefik`) && PathRegexp(...)`. A public hostname, a second Host
  matcher, or an `||` that lets a rule match without the Host clause fails (the D2735 shape).
- **Live router current** (`bigboy-cut.sh` STEP `router-current`, needs `DEPLOY_SHA`): the live
  `.traefik-dynamic/planes.yml` must be byte-identical to the generator's output for the pinned
  deploy sha (see "Pinned deploy values" below).
- **Web env parity** (`cigate web-env-parity`, STEP `web-env-parity`): every prod `ops.web.env` /
  `ops.web.extraEnv` NAME must be classified (overlay vs venue-local base) -- a new prod name fails,
  named; overlay names (`BACKEND_URL`, `AUTH_URL`) must be in `compose.bigboy.yml` with the
  expected public values; the running web container (names via `container-env-names.sh`) must carry
  every prod name. `AUTH_URL` stays hand-maintained per the lead's ruling; this check is what makes
  the hand list loud.
- **Routing-ledger parity**: removed (CHAOS-8543; PRs #3751, #3769). query-api serves a catalog
  operation that has no routing row, so there is no row to enable and no ledger to compare with
  prod. The cut has no such STEP (it had `routing-enable` / `routing-parity`), and
  `bigboy-graphql-prove.sh` has no parity step. `routing-ops.txt` stays: the web-path smoke reads
  its `KNOWN-MISSING` markers.
- **Web-path smoke, real browser session** (`dho smoke web-path`, Go, run from the dho image the stack pins): logs in through Auth.js
  (csrf + Credentials callback) with the PUBLIC Host header on traefik, so every call takes the
  browser's path through `web`'s proxy.ts. Checks: unauthenticated public-host request lands on web
  (303, no plane header); callback-url cookie and sign-out redirect carry the public origin; backend
  `/health`; Cockpit threads; `filters/options`; `investment/explain`; `work-units`
  (`include_textual=true`); `drilldown/prs` feeding a real `flame` entity; GraphQL
  `complexityTimeseries`, `workGraphFlow` (no empty `nodeType`) and `testopsRisk`; the
  `/testops/risk` page. GraphQL documents sent are the registered wire-form documents compiled into the dho build (no printer). Web's own `export const ..._QUERY` text is read from `web/src` and formatted as urql formats it (`__typename` appended to every non-root selection set, `@_` directives dropped) must carry exactly the GraphQL tokens of a registered document of the build (the current one, or a legacy one: a two-step pin, this ops with the older web; whitespace and commas aside), and that registered document must hash to a digest the edge catalog holds for that operation (current or legacy) at the deployed sha, so a change in web's document fails as `document_digest_mismatch`; a web document that equals a legacy text passes and the check's receipt names it in `legacy_documents` (the smoke still sends the current document; CHAOS-9146); each REST path must still appear in the web source file that calls it. The catalog comes from `DHO_SMOKE_CATALOG_FILE`, or `DHO_SMOKE_OPS_SHA` for a standalone run. Exit 3 = passed except
  named KNOWN-MISSING checks (read from `routing-ops.txt`).

### Running the web-path smoke

- **Inside a cut:** `bigboy-cut.sh` fetches the edge catalog at the cut's sha into
  `_records/bigboy-<sha8>.catalog.json` and exports it as `DHO_SMOKE_CATALOG_FILE`. Nothing else to set.
- **Standalone (no cut):** set `DHO_SMOKE_OPS_SHA` to the ops sha bigboy is running (the last cut's
  full sha). The script fetches `contracts/graphql/v1/go_api_operations.json` at that sha
  with `gh api`. Without `DHO_SMOKE_CATALOG_FILE` or `DHO_SMOKE_OPS_SHA` it fails with exit 1 and names
  both variables. Example: `DHO_SMOKE_OPS_SHA=<full sha> bash ci/bigboy/web-path-smoke.sh`.
- Both need `DHO_SMOKE_ADMIN_EMAIL` and `DHO_SMOKE_ADMIN_PASSWORD_FILE` in `ops/.env`.
- Exit codes: 0 pass; 1 fail (named); 2 refused target (`DHO_SMOKE_BASE_URL` outside the allowlist);
  3 passed except named KNOWN-MISSING checks.

## GraphQL prove harness (CHAOS-6993, partial)

`bigboy-graphql-prove.sh <full ops sha> [--go-edge]` + `compose.bigboy.yml` run the prove leg of
prod's STEP 216 (`pod-r216.sh`) on the compose stack, from `venue-prove`. The edge is the ROUTED
`/graphql` (`http://traefik:3000/graphql`, which the plane-split router sends to query-api:
CHAOS-6263, the prover's Go-edge mode, `dho goapi prove -go-edge`). There is no Python edge
(CHAOS-8361); `--go-edge` is still accepted and means the same. Every proof is the candidate alone,
and the prover refuses by name if a Python plane answers the routed `/graphql`. The steps:

1. refuse unless `venue-prove`'s image is `go-api-tools:sha-<sha>` (prover build skew);
2. derive the local org read-only (the single org of the local admin account; never printed);
3. `dho goapi prove` for the local org -- read-only against org data, writes proof receipts only;
   the proof credential is the local admin's user access token (a platform admin), in a 0600/0400
   file the operator stages and names by `BIGBOY_PROVE_ACCESS_TOKEN_FILE` (the script refuses a
   file that is unset, missing, a symlink or of another mode, and never reads the value); it is
   mounted read-only into the container and read there (`-proof-access-token-file`); the edge token
   is still minted in process from `JWT_SECRET_KEY` (compose substitution from `ops/.env`);

Full output names the org and stays under the devhealth-root `_records/bigboy-<sha8>/graphql-prove-<ts>/`.
`BIGBOY_ROOT` (default `/home/ubuntu/devhealth`) is the running tree the harness works in, the same
parameter `bigboy-cut.sh` reads.

First live run (4f014a9d, before CHAOS-8702): prove attempted=250 executed=183 PROVEN_GO_ONLY=178. Since CHAOS-8702 there is no
routing row to read or repoint for a catalog operation: query-api serves every registered operation, and the prover measures each
one through the routed `/graphql` against the running build. A catalog operation that was once unprovable here for want of a
routing row (testopsRisk) is measured like any other; the old `shadow` / `/query/proof` procedure for it is gone with the rows.

## Pinned deploy values (R467)

Deploy values can run AHEAD of the running build: a deploy PR that routes new paths to Go can merge
before the ops change that serves them is in any image (rev 195's SSO group: deploy main routed 5
SSO paths while bigboy still ran 4f014a9d). So the values source is an explicit input per cut:

- `DEPLOY_SHA` (in the round's `round.env` and recorded in `_records/bigboy-<sha8>.deploy-sha.txt`),
  read from `DEPLOY_REPO` (default the devhealth-root `deploy` checkout) with `git show` -- never a
  working tree, never "deploy main".
- STEP `deploy-pin`: `cigate plane-split-router --deploy-repo --deploy-sha --expect-ops-sha $NEW
  --values-out ...` REFUSES (rc=3) when that deploy commit's `vendor/dev-health-ops` pin is not the
  build the cut runs. Every values-driven STEP (router-current, query-api flags, web-env parity,
  route-coverage) reads the file it wrote. Without `DEPLOY_SHA` those STEPs report rc=2 SKIPPED.
  The cut builds `cigate` once for these STEPs (`go build -trimpath -o _records/bigboy-<sha8>.bin/cigate
  ./cmd/cigate` in `BIGBOY_OPS_REPO`); a failed build is `deploy-pin rc=1` with the build log in
  `_records/bigboy-<sha8>.cigate-build.err`. With `DEPLOY_SHA` set and no `go` on `PATH`, the cut stops before any
  STEP with one line: `FAIL: bigboy-cut: go not found on PATH; needed to build cigate for the DEPLOY_SHA STEPs`.
- STEP `route-coverage` (`cigate route-coverage`): one `ROUTEPROBE` request per routed path to each
  Go plane; net/http's method-aware mux answers 405 (or 401 behind auth) for a registered path and
  404 for none, before any handler runs. A 404 fails the STEP by name.

Limits, stated plainly: the vendor-pin check cannot tell a deploy commit whose values are ahead of
its own vendor pin (deploy main at R467 vendored 4f014a9d too), and the ROUTEPROBE check cannot tell
a registered stub from a real handler (bigboy's 4f014a9d go-api answers the OIDC paths with 405
because a placeholder is registered). The control for R467 is choosing `DEPLOY_SHA` = the deploy
commit that was rolled (or proven) with this build -- today **8028ff17** for ops 4f014a9d.

Regenerate the live router from a pinned sha:
`go run ./cmd/cigate plane-split-router --deploy-repo /home/ubuntu/devhealth/deploy --deploy-sha 8028ff17
--expect-ops-sha 4f014a9d9babfff17b6ca71a33778f2f69d30737 --format dynamic > .traefik-dynamic/planes.yml`
(write to a temp file and rename; traefik hot-reloads).

## Billing host on go-api; the Python billing-edge is retired (CHAOS-7055)

Prod serves the Stripe webhook host from go-api's billing-edge listener (:8010) and has no Python
`billing-edge`. `compose.bigboy.yml` does the same here: go-api gains
`--api-billing-edge-addr=:8010` and a traefik router for `Host(billing.localhost)` (the rule and entrypoint the
Python service carried) to port 8010; `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` and `LICENSE_PRIVATE_KEY`
reach go-api by name from `ops/.env` (never written in the file; unset, the listener's `/health` names the
missing one with a 503); the Python `billing-edge` service moves to the profile `retired-billing-edge`, which
nothing enables. The file is the one compose file the cut runs, so go-api is never recreated without them.

Applying it recreates go-api, so it goes in with a cut, never on its own while go-api is in use, and the
checkout the cut's scripts read (line 39's list) is pulled to the merge right before that cut, not earlier. ORDER:
(1) name the Python container and its state (`docker compose ps billing-edge`), (2) remove it with compose verbs
(`docker compose ... rm -sf billing-edge`; it is base-file drift, the cut has no rm step; one approved line, run
on the lead's GO), (3) run the cut. The go-api router is `billing-go` so the old container's router `billing` is
never redefined while both exist. All three values are present in `ops/.env` on this host (checked by name and length only), so the listener starts
configured; whether go-api starts with them EMPTY was not executed (compose.go.workers.yml's go-api comment says its
`/health` then answers 503 naming the missing one). Read the rendered chain only through
`compose-config-redacted.sh`. Proof: the billing host answers from go-api :8010 through traefik, and
`docker compose ps` no longer lists `billing-edge`.
