{{/*
Expand the name of the chart.
*/}}
{{- define "dev-health.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "dev-health.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dev-health.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dev-health.labels" -}}
helm.sh/chart: {{ include "dev-health.chart" . }}
{{ include "dev-health.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dev-health
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dev-health.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dev-health.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "dev-health.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dev-health.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace
*/}}
{{- define "dev-health.namespace" -}}
{{- default .Release.Namespace .Values.global.namespaceOverride }}
{{- end }}

{{/*
Backend image
*/}}
{{- define "dev-health.image" -}}
{{- if contains "@" .Values.image.repository -}}
{{- .Values.image.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end -}}
{{- end }}

{{/*
Web image
*/}}
{{- define "dev-health.webImage" -}}
{{- if contains "@" .Values.webImage.repository -}}
{{- .Values.webImage.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.webImage.repository (default .Chart.AppVersion .Values.webImage.tag) }}
{{- end -}}
{{- end }}

{{/*
query-api image
*/}}
{{- define "dev-health.queryApiImage" -}}
{{- if contains "@" .Values.queryApi.image.repository -}}
{{- .Values.queryApi.image.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.queryApi.image.repository (default .Chart.AppVersion .Values.queryApi.image.tag) }}
{{- end -}}
{{- end }}

{{/*
dho (operator binary) image: the Go api Deployment runs `dho api` from it
*/}}
{{- define "dev-health.goApiImage" -}}
{{- if contains "@" .Values.goApi.image.repository -}}
{{- .Values.goApi.image.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.goApi.image.repository (default .Chart.AppVersion .Values.goApi.image.tag) }}
{{- end -}}
{{- end }}

{{/*
Secret name — either the one we create or an external one
*/}}
{{- define "dev-health.secretName" -}}
{{- if .Values.secrets.create }}
{{- include "dev-health.fullname" . }}-secrets
{{- else }}
{{- required "secrets.externalSecretName is required when secrets.create is false" .Values.secrets.externalSecretName }}
{{- end }}
{{- end }}

{{/*
ConfigMap name
*/}}
{{- define "dev-health.configMapName" -}}
{{- include "dev-health.fullname" . }}-config
{{- end }}

{{/*
Redis/Valkey URL — auto-computed when valkey.enabled, otherwise empty (the
caller falls back to secrets.data.REDIS_URL / secrets.data.VALKEY_URI, an
explicit external Redis/Valkey URL). ONE URL serves both the Python API
(REDIS_URL) and the Go workers (VALKEY_URI), on DB 1: the Go client refuses
any other DB (internal/storage/valkey/factory.go), and the CHAOS-4226 cache
epoch the Go finalize bumps must land in the keyspace the API's cache reads.
*/}}
{{- define "dev-health.redisURL" -}}
{{- if .Values.valkey.enabled }}
{{- printf "redis://%s-valkey:6379/1" (include "dev-health.fullname" .) }}
{{- end }}
{{- end }}

{{/*
ClickHouse URI — auto-computed when clickhouse.enabled
*/}}
{{- define "dev-health.clickhouseURI" -}}
{{- if .Values.clickhouse.enabled }}
{{- printf "clickhouse://%s:%s@%s-clickhouse:8123/%s" .Values.clickhouse.credentials.user .Values.clickhouse.credentials.password (include "dev-health.fullname" .) .Values.clickhouse.credentials.database }}
{{- end }}
{{- end }}

{{/*
The Go runtimes' effective CLICKHOUSE_URI, or empty when neither source exists.

dev-health.clickhouseURI above renders the HTTP port (8123) because Python's
clickhouse-connect speaks HTTP. The Go client speaks the native wire protocol
and eagerly Ping()s at construction, so it needs the native port -- the same
variable name resolving to a different port per runtime.

Resolution order: an explicit goWorkers.clickhouseURI wins, otherwise the
bundled ClickHouse is addressed natively (dev-health.clickhouseNativeURI
below). With an EXTERNAL ClickHouse and no goWorkers.clickhouseURI set, this
renders empty and the shared Secret's HTTP URI is inherited -- which will fail
readiness, so the value is required in that configuration and the deployment
contract test asserts it.

Callers use this ONLY to decide WHETHER a CLICKHOUSE_URI entry is rendered.
HOW the value reaches the container differs by source and is decided in
go-workers.yaml: an operator-supplied goWorkers.clickhouseURI is inline config
(same posture as goWorkers.pgbouncer.postgres.host -- the chart neither minted
it nor can tell whether it carries a credential), while the derived bundled URI
is a chart-managed credential and travels by secretKeyRef.
*/}}
{{- define "dev-health.goWorkerClickhouseURI" -}}
{{- if .Values.goWorkers.clickhouseURI }}
{{- .Values.goWorkers.clickhouseURI }}
{{- else }}
{{- include "dev-health.clickhouseNativeURI" . }}
{{- end }}
{{- end }}

{{/*
The native-protocol (9000) URI for the bundled ClickHouse. It embeds
clickhouse.credentials.password, so it is rendered into the shared Secret
(dev-health.secretData, key CLICKHOUSE_NATIVE_URI) and reaches a Pod only by
secretKeyRef -- never as an inline `value:` in a Pod spec, which
`kubectl get <workload> -o yaml` prints to anyone holding read access on the
workload.
*/}}
{{- define "dev-health.clickhouseNativeURI" -}}
{{- if .Values.clickhouse.enabled }}
{{- printf "clickhouse://%s:%s@%s-clickhouse:9000/%s" .Values.clickhouse.credentials.user .Values.clickhouse.credentials.password (include "dev-health.fullname" .) .Values.clickhouse.credentials.database }}
{{- end }}
{{- end }}

{{/*
PostgreSQL URI — auto-computed when postgresql.enabled
*/}}
{{- define "dev-health.postgresURI" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "postgresql+asyncpg://%s:%s@%s-postgresql:5432/%s" .Values.postgresql.credentials.user .Values.postgresql.credentials.password (include "dev-health.fullname" .) .Values.postgresql.credentials.database }}
{{- end }}
{{- end }}

{{/* Go PgBouncer topology helpers. The poolers have stable in-cluster Service
names; role credentials and runtime DSNs remain Secret values. */}}
{{- define "dev-health.goPgbouncerSecretName" -}}
{{- if .Values.goWorkers.pgbouncer.secret.create }}
{{- printf "%s-go-pgbouncer" (include "dev-health.fullname" .) }}
{{- else }}
{{- required "goWorkers.pgbouncer.secret.externalSecretName is required when goWorkers.pgbouncer.secret.create=false" .Values.goWorkers.pgbouncer.secret.externalSecretName }}
{{- end }}
{{- end }}

{{- define "dev-health.goPgbouncerPostgresHost" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql" (include "dev-health.fullname" .) }}
{{- else }}
{{- required "goWorkers.pgbouncer.postgres.host is required for external PostgreSQL" .Values.goWorkers.pgbouncer.postgres.host }}
{{- end }}
{{- end }}

{{- define "dev-health.goPgbouncerPostgresPort" -}}
{{- if .Values.postgresql.enabled }}5432{{- else }}{{ required "goWorkers.pgbouncer.postgres.port is required for external PostgreSQL" .Values.goWorkers.pgbouncer.postgres.port }}{{- end }}
{{- end }}

{{- define "dev-health.goPgbouncerPostgresDatabase" -}}
{{- if .Values.postgresql.enabled }}
{{- required "postgresql.credentials.database must not be empty when postgresql.enabled=true" .Values.postgresql.credentials.database }}
{{- else }}
{{- required "goWorkers.pgbouncer.postgres.database is required for external PostgreSQL" .Values.goWorkers.pgbouncer.postgres.database }}
{{- end }}
{{- end }}

{{/*
query-api base URL — the in-cluster Service the Python edge's dispatcher
(GO_API_QUERY_API_URL) forwards to. Auto-computed so the chart renders a
reachable endpoint whenever queryApi is enabled; override
config.GO_API_QUERY_API_URL to point at a differently-named release or an
out-of-cluster origin instead.
*/}}
{{- define "dev-health.queryApiURL" -}}
{{- printf "http://%s-query-api:%v" (include "dev-health.fullname" .) .Values.queryApi.port }}
{{- end }}

{{/*
Envelope JWKS Secret for query-api. This chart does NOT create it: the keypair
is minted out of band (scripts/mint-envelope-keys.sh) and only the public half
belongs in the cluster. Empty derives the conventional name, which is what an
existing deployment already uses.
*/}}
{{- define "dev-health.queryApiJwksSecretName" -}}
{{- if .Values.queryApi.envelope.jwksSecretName }}
{{- .Values.queryApi.envelope.jwksSecretName }}
{{- else }}
{{- printf "%s-query-api-jwks" (include "dev-health.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Image pull secrets
*/}}
{{- define "dev-health.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Shared non-secret config data — used by the main ConfigMap and the migration
hook ConfigMap (the latter exists because pre-install hooks run before the
chart's regular resources are created).
*/}}
{{- define "dev-health.configData" -}}
{{- /* Auto-compute the in-cluster Redis URL only when valkey is enabled AND
   the caller has not already pinned an explicit external one via the
   REDIS_URL Secret key. */}}
{{- $redisAuto := and .Values.valkey.enabled (not (index .Values.secrets.data "REDIS_URL")) }}
{{- /* Keys whose empty placeholder is replaced by a computed value below. */}}
{{- $derivedKeys := list "GO_API_QUERY_API_URL" }}
{{- range $key, $value := .Values.config }}
{{- if or $value (not (has $key $derivedKeys)) }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if $redisAuto }}
REDIS_URL: {{ include "dev-health.redisURL" . | quote }}
{{- end }}
{{- /* The Go workers read VALKEY_URI (internal/platform/config). Derive it
   from the same URL unless the caller pinned an explicit external one via
   the VALKEY_URI Secret key. */}}
{{- $valkeyAuto := and .Values.valkey.enabled (not (index .Values.secrets.data "VALKEY_URI")) }}
{{- if $valkeyAuto }}
VALKEY_URI: {{ include "dev-health.redisURL" . | quote }}
{{- end }}
{{- /* Only when a query-api workload actually exists to forward to. With
   queryApi disabled and no operator override the key stays ABSENT rather than
   empty: go_api_cli._query_api_url and the dispatcher both read it with `or`,
   so absent and empty behave alike, and absent says "no Go plane here". */}}
{{- if and .Values.queryApi.enabled (not (index .Values.config "GO_API_QUERY_API_URL")) }}
GO_API_QUERY_API_URL: {{ include "dev-health.queryApiURL" . | quote }}
{{- end }}
{{- if not (hasKey .Values.config "AUTO_RUN_MIGRATIONS") }}
{{- /* CHAOS-2304: when the migration hook owns schema changes, app pods must
   never ambient-migrate. Set config.AUTO_RUN_MIGRATIONS to override. */}}
AUTO_RUN_MIGRATIONS: {{ ternary "false" "true" .Values.migrations.hook.enabled | quote }}
{{- end }}
{{- end }}

{{/*
Shared secret stringData — used by the main Secret and the migration hook
Secret.
*/}}
{{- define "dev-health.secretData" -}}
{{- range $key, $value := .Values.secrets.data }}
{{- if $value }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if and .Values.clickhouse.enabled (not (index .Values.secrets.data "CLICKHOUSE_URI")) }}
CLICKHOUSE_URI: {{ include "dev-health.clickhouseURI" . | quote }}
{{- end }}
{{- /* The native-protocol (:9000) companion the Go runtimes need. It lives
   here, not inline in a Pod spec, because it embeds
   clickhouse.credentials.password -- see dev-health.clickhouseNativeURI. */}}
{{- if and .Values.clickhouse.enabled (not (index .Values.secrets.data "CLICKHOUSE_NATIVE_URI")) }}
CLICKHOUSE_NATIVE_URI: {{ include "dev-health.clickhouseNativeURI" . | quote }}
{{- end }}
{{- if and .Values.postgresql.enabled (not (index .Values.secrets.data "DATABASE_URI")) }}
DATABASE_URI: {{ include "dev-health.postgresURI" . | quote }}
{{- end }}
{{- end }}

{{/*
Migration-only secret data. Keep the elevated migration DSN out of the shared
application Secret while retaining selected Postgres/ClickHouse compatibility
values for existing installations.
*/}}
{{- define "dev-health.migrationSecretData" -}}
{{- /*
CHAOS-4428: when the weight-10 river-migrate hook owns the River step, this
Secret -- which the weight-0 migrate Job envFroms -- does not carry
MIGRATION_DATABASE_URI: the same DSN reaches the Job under POSTGRES_URI,
which dho's migration resolver takes when MIGRATION_DATABASE_URI is not
configured. The Job then runs `dho migrate upgrade` without --river, and the
River step waits for the weight-5 hook that creates the roles it requires.
*/}}
{{- $riverHookOwnsRiver := .Values.migrations.hook.riverMigrate.enabled }}
{{- range $key, $value := .Values.migrations.hook.secretData }}
{{- if and $value (not (and $riverHookOwnsRiver (eq $key "MIGRATION_DATABASE_URI"))) }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- /*
The migrate Job runs dho, which speaks ClickHouse's native protocol (9000):
an explicit migrations.hook.secretData.CLICKHOUSE_URI wins (it must be
native), else the Go runtimes' URI (goWorkers.clickhouseURI, else the bundled
ClickHouse addressed natively), else the shared secrets.data.CLICKHOUSE_URI,
which the Python api reads over HTTP -- with an external ClickHouse, set
goWorkers.clickhouseURI, as the Go workers already require.
*/}}
{{- if not (index .Values.migrations.hook.secretData "CLICKHOUSE_URI") }}
{{- $nativeClickhouseURI := include "dev-health.goWorkerClickhouseURI" . }}
{{- if $nativeClickhouseURI }}
CLICKHOUSE_URI: {{ $nativeClickhouseURI | quote }}
{{- else if (index .Values.secrets.data "CLICKHOUSE_URI") }}
CLICKHOUSE_URI: {{ index .Values.secrets.data "CLICKHOUSE_URI" | quote }}
{{- end }}
{{- end }}
{{- $hasDedicatedMigrationURI := index .Values.migrations.hook.secretData "MIGRATION_DATABASE_URI" }}
{{- $hasHookPostgresURI := index .Values.migrations.hook.secretData "POSTGRES_URI" }}
{{- $hasHookDatabaseURI := index .Values.migrations.hook.secretData "DATABASE_URI" }}
{{- $hasHookDatabase := or $hasDedicatedMigrationURI $hasHookPostgresURI $hasHookDatabaseURI }}
{{- if and $riverHookOwnsRiver $hasDedicatedMigrationURI (not $hasHookPostgresURI) (not $hasHookDatabaseURI) }}
POSTGRES_URI: {{ $hasDedicatedMigrationURI | quote }}
{{- else if and (not $hasHookDatabase) (index .Values.secrets.data "POSTGRES_URI") }}
POSTGRES_URI: {{ index .Values.secrets.data "POSTGRES_URI" | quote }}
{{- else if and (not $hasHookDatabase) (index .Values.secrets.data "DATABASE_URI") }}
DATABASE_URI: {{ index .Values.secrets.data "DATABASE_URI" | quote }}
{{- else if and (not $hasHookDatabase) .Values.postgresql.enabled }}
POSTGRES_URI: {{ include "dev-health.postgresURI" . | quote }}
{{- end }}
{{- end }}

{{/*
The elevated DSN the weight-10 River hook runs against, resolved the same way
the migration Secret resolves Alembic's: the dedicated URI, then the migration
Secret's own compatibility aliases, then the application Secret's, then the
bundled StatefulSet. Kept separate from migrationSecretData because the two
consumers must see this value under DIFFERENT variable names -- which is the
whole point of the split (see that helper's comment).
*/}}
{{- define "dev-health.riverMigrationDSN" -}}
{{- $hook := .Values.migrations.hook.secretData }}
{{- $dsn := or (index $hook "MIGRATION_DATABASE_URI") (index $hook "POSTGRES_URI") (index $hook "DATABASE_URI") (index .Values.secrets.data "POSTGRES_URI") (index .Values.secrets.data "DATABASE_URI") }}
{{- if $dsn }}{{ $dsn }}{{- else if .Values.postgresql.enabled }}{{ include "dev-health.postgresURI" . }}{{- end }}
{{- end }}

{{/*
Component labels helper — call with (dict "component" "api" "context" $)
*/}}
{{- define "dev-health.componentLabels" -}}
{{ include "dev-health.labels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Component selector labels — call with (dict "component" "api" "context" $)
*/}}
{{- define "dev-health.componentSelectorLabels" -}}
{{ include "dev-health.selectorLabels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Lockstep image check — call with (dict "context" $ "image" <resolved hook
image> "labelWithValue" <pre-formatted "<flag description> (<image>)" text>
"reason" <trailing sentence naming why the two must match>).

Shared by route-activate-hooks.yaml (its own operator image) and
river-hooks.yaml (the River hook image): both run a Go operator binary
against the database the migrate Job just migrated, so that operator's
image must be pinned to the SAME commit as image.repository/image.tag
(dev-health.image) -- production revision 14 went down on exactly this
mismatch (a floating/stale operator image against a freshly migrated
database, failing runtime_role_unauthorized with no diagnostics).

Only comparable when BOTH sides carry this repo's `sha-<12 hex>`
immutable-tag convention: a sha256 digest does not encode which commit it
was built from, so a digest-only pin on either side is never compared here
-- that would be fabricating a mapping this chart cannot verify. Keeping a
digest pin in lockstep with the api image is the operator's own
responsibility. This is a NAMED, KNOWN limit shared by both callers (see
each caller's own comment and the CHAOS-6324 follow-up ticket for the
digest -> commit resolution this cannot currently do); it is not
"unlimited trust" -- it is the same limit the pin-format check right
above each caller's own call site already accepts (a digest is a valid
pin on its own, just not one this specific comparison can use).
*/}}
{{- define "dev-health.lockstepImageCheck" -}}
{{- $apiImageTag := regexFind ":sha-[0-9a-f]{12}$" (include "dev-health.image" .context) -}}
{{- $hookImageTag := regexFind ":sha-[0-9a-f]{12}$" .image -}}
{{- if and $apiImageTag $hookImageTag (ne $apiImageTag $hookImageTag) -}}
{{- fail (printf "%s and image.repository/image.tag (%s) are pinned to different commits (%s vs %s) -- %s" .labelWithValue (include "dev-health.image" .context) $hookImageTag $apiImageTag .reason) -}}
{{- end -}}
{{- end }}

{{/*
Pinned operator (dho) image check — call with (dict "context" $ "image" <resolved
image> "pullPolicy" <resolved pull policy, already defaulted to "IfNotPresent" by
the CALLER, never to .Values.image.pullPolicy -- see below> "flagLabel" <the
precedence description, e.g. "migrations.hook.provisionRoles.image, else
migrations.hook.riverMigrate.image, else migrations.hook.routeActivate.image">
"verb" <the dho verb this image must run, e.g. "`dho migrate roles`">).

CHAOS-6958: river-hooks.yaml's provision-roles hook, its River hook,
migrate-job.yaml's migrate Job and route-activate-hooks.yaml each grew their OWN
copy of "is this image pinned" — three gaps found by the CHAOS-6951 r1 round, all
pre-existing (reproduced against the already-merged River hook), now fixed ONCE
here instead of at each call site:

  1. IDENTITY, not just shape (D2684: this needed an argued exception, not a
     silent denylist -- the argument and the EXECUTED evidence for it are here,
     and are also posted to the CHAOS-6958 Linear comment thread). A
     digest-pinned or sha-<12 hex>-tagged image used to be accepted whatever
     repository it named -- a pinned Python api image
     (ghcr.io/full-chaos/dev-hops-api:sha-<12 hex>) with a matching image.tag
     rendered clean and then failed at runtime with an unrecognized-argument
     error.

     D2684 asked for a cross-check against "the chart's configured operator
     repository, default or override" (a repository-part match, not a name
     allowlist), with a denylist as at most a second layer, UNLESS that
     cross-check is tautological here -- argued and shown, not asserted:

     TRIED that cross-check (every OTHER hook's image already falls back to
     migrations.hook.routeActivate.image when unset -- river-hooks.yaml:102,
     :291, migrate-job.yaml:71 -- so it is the only candidate for "the chart's
     one configured operator repository"), and it FAILED two EXISTING tests,
     not a hypothetical:
       - test_provisioning_image_prefers_its_own_then_the_river_then_the_route_image
         and test_river_migrate_takes_its_own_pinned_image
         (test_helm_migration_hook_chain.py) both pin a hook's OWN image
         WINNING over migrations.hook.routeActivate.image's, from a DIFFERENT
         repository (dev-health-go-dho vs. dev-health-go-operator) and a
         DIFFERENT commit -- by design, per this file's own comment at
         river-hooks.yaml:93-96 ("provisionRoles.image, else riverMigrate.image,
         else routeActivate.image -- the operator image the rest of the chain
         ALREADY requires") and query-api-deployment.yaml:11-13
         ("dev-health-go-dho or dev-health-go-operator, matched on the last
         path element"): this chart treats the two repository names as
         interchangeable dho images, each hook independently pinned to
         whatever commit THAT step needs, not required to share a repository
         or a commit with any other hook.
     A repository-part cross-check is therefore not tautological here -- it is
     WRONG: it would refuse a legitimately independent per-hook pin the chart
     already supports and tests. Reverted; the executed failing-test evidence
     above is the argued exception D2684 allows for.

     What is left, and its residual: a DENYLIST of the retired/wrong images
     this repo actually publishes under a name a dho verb could otherwise reach
     by mistake (dev-hops-api, the Python edge; the retired
     dev-health-query-api and dev-health-go-worker images) -- the same shape of
     check migrate-job.yaml already ran for dev-hops-api alone, now shared and
     covering the other two retired names too. It closes the r1 round's actual
     reproduction (a Python image pinned in place of a dho one). It does NOT
     close a hook pinned to some OTHER, unlisted wrong image that happens to
     share the pin-shape rule -- Helm has no oracle for "the right image"
     beyond what the operator configures and this chart's own KNOWN retired
     names, and there is no single "chart's configured operator repository"
     this chart's own design lets every hook be cross-checked against.

     A positive name ALLOWLIST (the stricter check query-api-deployment.yaml and
     go-workers.yaml use) is also deliberately NOT used here:
     test_route_activate_operator_image_override_is_honoured
     (test_helm_route_activate_hooks.py) pins a CUSTOM repository name
     (ghcr.io/example/custom-operator) as a supported, deliberate override,
     which an allowlist would regress the same way the cross-check did.
  2. A REAL digest, not a substring. `contains "@sha256:"` treated
     `repo@sha256:not-a-digest` as pinned; a real puller rejects the malformed
     reference. The digest must now be a well-formed 64 lowercase-hex-digit
     sha256.
  3. Pull policy. This helper does not choose a pull policy -- it only checks the
     one the caller already resolved. The caller must default it to
     "IfNotPresent", NEVER to .Values.image.pullPolicy (the APPLICATION image's
     policy): a local dev value of image.pullPolicy=Never used to leak onto a
     PINNED REGISTRY operator image, which then could not be pulled at all.
     route-activate-hooks.yaml already got this right (its own r2 P2 codex-review
     fix); provision-roles, river-migrate and migrate did not.

The lockstep check (this image pinned to the SAME commit as
image.repository/image.tag) still runs at the end, unchanged.
*/}}
{{- define "dev-health.pinnedOperatorImageCheck" -}}
{{- $image := .image -}}
{{- if regexMatch "(^|/)dev-hops-api([:@]|$)" $image -}}
{{- fail (printf "%s (%s) is not a pinned dho image: it is the Python api image, which has no dho entrypoint. It runs %s, which needs the dho (operator) image." .flagLabel $image .verb) -}}
{{- end -}}
{{- if regexMatch "(^|/)dev-health-query-api([:@]|$)" $image -}}
{{- fail (printf "%s (%s) is not a pinned dho image: it is the retired dev-health-query-api image, which has no dho entrypoint. It runs %s, which needs the dho (operator) image." .flagLabel $image .verb) -}}
{{- end -}}
{{- if regexMatch "(^|/)dev-health-go-worker([:@]|$)" $image -}}
{{- fail (printf "%s (%s) is not a pinned dho image: it is the retired dev-health-go-worker image, which has no dho entrypoint. It runs %s, which needs the dho (operator) image." .flagLabel $image .verb) -}}
{{- end -}}
{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{64}$" $image -}}
{{- $immutableTag := regexMatch "^.+:sha-[0-9a-f]{12}$" $image -}}
{{- $sideloadedLocal := and (regexMatch "^.+:local$" $image) (or (eq .pullPolicy "Never") (eq .pullPolicy "IfNotPresent")) -}}
{{- if not (or $digestPinned $immutableTag $sideloadedLocal) -}}
{{- fail (printf "%s (%s) is not a pinned dho image. It runs %s: set it to repo@sha256:<64 lowercase-hex-digit digest> or repo:sha-<12 hex>; repo:local is accepted only with pullPolicy Never or IfNotPresent." .flagLabel $image .verb) -}}
{{- end -}}
{{- include "dev-health.lockstepImageCheck" (dict "context" .context "image" $image "labelWithValue" (printf "%s (%s)" .flagLabel $image) "reason" (printf "%s must run the same build as the application it is targeting." .verb)) -}}
{{- end }}
