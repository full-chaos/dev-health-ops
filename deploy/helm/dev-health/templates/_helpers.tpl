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
Workload ServiceAccount name: <fullname>-<component>. Call with (dict "ctx" $ "component" "web").
*/}}
{{- define "dev-health.workloadServiceAccountName" -}}
{{- printf "%s-%s" (include "dev-health.fullname" .ctx) .component }}
{{- end }}

{{/*
The pod-spec lines that bind a workload to its own ServiceAccount.
Call with (dict "ctx" $ "component" "web" "cfg" .Values.web.serviceAccount).
*/}}
{{- define "dev-health.workloadServiceAccount" -}}
serviceAccountName: {{ include "dev-health.workloadServiceAccountName" . }}
automountServiceAccountToken: {{ .cfg.automountToken | default false }}
{{- end }}

{{/*
Namespace
*/}}
{{- define "dev-health.namespace" -}}
{{- default .Release.Namespace .Values.global.namespaceOverride }}
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
Auth Control Plane images (docker/go-worker.Dockerfile targets auth-service and auth-migrate)
*/}}
{{- define "dev-health.authServiceImage" -}}
{{- if contains "@" .Values.authService.image.repository -}}
{{- .Values.authService.image.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.authService.image.repository (default .Chart.AppVersion .Values.authService.image.tag) }}
{{- end -}}
{{- end }}

{{- define "dev-health.authMigrateImage" -}}
{{- if contains "@" .Values.authService.migrate.image.repository -}}
{{- .Values.authService.migrate.image.repository }}
{{- else -}}
{{- printf "%s:%s" .Values.authService.migrate.image.repository (default .Chart.AppVersion .Values.authService.migrate.image.tag) }}
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
CHAOS-11217: the Secret that holds AUTH_SESSION_ISSUE_TOKEN, the credential go-api
presents to auth-service's session issue route. goApi.sessionIssue.secretName names a
dedicated, operator-created Secret; empty keeps the shared Secret. Only go-api reads it.
*/}}
{{- define "dev-health.sessionIssueSecretName" -}}
{{- $issue := default dict .Values.goApi.sessionIssue }}
{{- if $issue.secretName }}
{{- $issue.secretName }}
{{- else }}
{{- include "dev-health.secretName" . }}
{{- end }}
{{- end }}

{{/*
CHAOS-10446: the Secret that holds JWT_SECRET_KEY, the HS256 access-token key. It is
its own Secret so the shared Secret the worker groups envFrom never carries it.
hs256Secret.externalSecretName names an operator-managed one; otherwise the chart
creates <fullname>-hs256 (secrets.create). Only go-api reads it through the chart.
*/}}
{{- define "dev-health.hs256SecretName" -}}
{{- if .Values.hs256Secret.externalSecretName }}
{{- if eq (lower (trim .Values.hs256Secret.externalSecretName)) (lower (trim (include "dev-health.secretName" .))) }}
{{- fail "hs256Secret.externalSecretName must differ from the shared Secret name (CHAOS-10446): the worker groups envFrom the shared Secret, so the HS256 key would reach them" }}
{{- end }}
{{- .Values.hs256Secret.externalSecretName }}
{{- else if .Values.secrets.create }}
{{- include "dev-health.fullname" . }}-hs256
{{- else }}
{{- fail "hs256Secret.externalSecretName is required when secrets.create is false (CHAOS-10446): JWT_SECRET_KEY lives in its own Secret, not in secrets.externalSecretName" }}
{{- end }}
{{- end }}

{{/*
CHAOS-10446: one guard for every web selector. args: dict "ctx" <root> "name" <Secret name> "key" <Secret key> "what" <where> ["sharedOk" true].
Fails when the name (trim + case-fold) is the HS256 key's Secret or the shared Secret, or the key is JWT_SECRET_KEY.
sharedOk (single keys only, web.extraEnv secretKeyRef): the shared Secret is allowed for keys other than JWT_SECRET_KEY.
*/}}
{{- define "dev-health.webSecretGuard" -}}
{{- $ctx := .ctx -}}
{{- $n := lower (trim (.name | default "")) -}}
{{- $hs := lower (trim ($ctx.Values.hs256Secret.externalSecretName | default (ternary (printf "%s-hs256" (include "dev-health.fullname" $ctx)) "" $ctx.Values.secrets.create))) -}}
{{- $sh := lower (trim (ternary (printf "%s-secrets" (include "dev-health.fullname" $ctx)) ($ctx.Values.secrets.externalSecretName | default "") $ctx.Values.secrets.create)) -}}
{{- $sharedOk := .sharedOk | default false -}}
{{- if $sharedOk -}}
{{- if or (and $n $hs (eq $n $hs)) (eq (trim (.key | default "")) "JWT_SECRET_KEY") -}}
{{- fail (printf "%s must not name the HS256 key's Secret or the key JWT_SECRET_KEY (CHAOS-10446): web would hold a token-minting key; other keys of the shared Secret are allowed" .what) -}}
{{- end -}}
{{- else if or (and $n $hs (eq $n $hs)) (and $n $sh (eq $n $sh)) (eq (trim (.key | default "")) "JWT_SECRET_KEY") -}}
{{- fail (printf "%s must not name the HS256 key's Secret, the shared Secret, or the key JWT_SECRET_KEY (CHAOS-10446): web would hold a token-minting key" .what) -}}
{{- end -}}
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
{{- if and $value (ne $key "JWT_SECRET_KEY") }}
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
image must be pinned to the SAME commit as the Go serving images the roll
puts in front of that database -- production revision 14 went down on exactly
this mismatch (a floating/stale operator image against a freshly migrated
database, failing runtime_role_unauthorized with no diagnostics).

CHAOS-8309: the comparison targets are the Go serving images -- queryApi.image
(when queryApi.enabled) and goApi.image (when goApi.enabled). When both are
enabled the hook tag must equal BOTH. Before this change the one target was
the Python api image: that is the wrong reference for a Go-only concern, and the
route-activate already compares against queryApi.image for the same
reason. A component that is not enabled has no image
being rolled, so it is not a comparison target.

CHAOS-7520: the Python image clause is gone (the Python api image has no
chart object left).

Only comparable when BOTH sides carry this repo's `sha-<12 hex>`
immutable-tag convention: a sha256 digest does not encode which commit it
was built from, so a digest-only pin on either side is never compared here
-- that would be fabricating a mapping this chart cannot verify. Keeping a
digest pin in lockstep with the Go serving images is the operator's own
responsibility. This is a NAMED, KNOWN limit shared by both callers (see
each caller's own comment and the CHAOS-6324 follow-up ticket for the
digest -> commit resolution this cannot currently do); it is not
"unlimited trust" -- it is the same limit the pin-format check right
above each caller's own call site already accepts (a digest is a valid
pin on its own, just not one this specific comparison can use).
*/}}
{{- define "dev-health.lockstepImageCheck" -}}
{{- $hookImageTag := regexFind ":sha-[0-9a-f]{12}$" .image -}}
{{- if $hookImageTag -}}
{{- $label := .labelWithValue -}}
{{- $reason := .reason -}}
{{- if .context.Values.queryApi.enabled -}}
{{- $queryApiImage := include "dev-health.queryApiImage" .context -}}
{{- $queryApiTag := regexFind ":sha-[0-9a-f]{12}$" $queryApiImage -}}
{{- if and $queryApiTag (ne $queryApiTag $hookImageTag) -}}
{{- fail (printf "%s and queryApi.image (%s) are pinned to different commits (%s vs %s) -- %s" $label $queryApiImage $hookImageTag $queryApiTag $reason) -}}
{{- end -}}
{{- end -}}
{{- if .context.Values.goApi.enabled -}}
{{- $goApiImage := include "dev-health.goApiImage" .context -}}
{{- $goApiTag := regexFind ":sha-[0-9a-f]{12}$" $goApiImage -}}
{{- if and $goApiTag (ne $goApiTag $hookImageTag) -}}
{{- fail (printf "%s and goApi.image (%s) are pinned to different commits (%s vs %s) -- %s" $label $goApiImage $hookImageTag $goApiTag $reason) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Pinned operator (dho) image check — call with (dict "context" $ "image" <resolved
image> "pullPolicy" <resolved pull policy, already defaulted to "IfNotPresent" by
the CALLER, never the application image's pull policy -- see below> "flagLabel" <the
precedence description, e.g. "migrations.hook.provisionRoles.image, else
migrations.hook.riverMigrate.image, else migrations.hook.routeActivate.image">
"verb" <the dho verb this image must run, e.g. "`dho migrate roles`">).

CHAOS-6958: river-hooks.yaml's provision-roles hook, its River hook,
migrate-job.yaml's migrate Job and route-activate-hooks.yaml each grew their OWN
copy of "is this image pinned" — three gaps found by the CHAOS-6951 r1 round, all
pre-existing (reproduced against the already-merged River hook), now fixed ONCE
here instead of at each call site:

  1. IDENTITY, not just shape (D2684, then D2693 -- both decisions and their
     EXECUTED evidence are posted in full to the CHAOS-6958 Linear comment
     thread). A digest-pinned or sha-<12 hex>-tagged image used to be accepted
     whatever repository it named -- a pinned Python api image
     (ghcr.io/full-chaos/dev-hops-api:sha-<12 hex>) with a matching image.tag
     rendered clean and then failed at runtime with an unrecognized-argument
     error.

     D2684 asked for a cross-check against migrations.hook.routeActivate.image
     as "the chart's configured operator repository". TRIED it (every OTHER
     hook already falls back to that value when unset, so it is the only
     candidate) and it FAILED two EXISTING tests:
     chainProvisioningImagePreference and chainRiverTakesItsOwnPinnedImage
     (internal/cigates/chartgates/operator_images_test.go) both pin a hook's OWN image WINNING
     over routeActivate's, from a DIFFERENT repository
     (dev-health-go-dho vs. dev-health-go-operator) -- by design: each hook is
     independently pinned to whatever commit THAT step needs, never required
     to share a repository or a commit with any other hook
     (river-hooks.yaml:93-96).

     D2693 (final): routeActivate.image was the wrong anchor -- it is another
     hook's own choice, not a chart-level default. The correct anchor is the
     image NAME itself (the last path element, registry/namespace/tag/digest
     all stripped): it must be dev-health-go-operator or dev-health-go-dho,
     the two repository names this project ships with a dho entrypoint,
     "mirror-friendly" (any registry or namespace still matches, so re-pointing
     the SAME name at a private mirror is unaffected) -- the exact regex
     query-api-deployment.yaml and go-workers.yaml already use for the
     identical question. This does NOT touch the two tests above: neither
     changes the image NAME, only the repository/registry and the digest, so
     both still pass under a name-only check.

     A positive name allowlist WOULD, however, regress
     routeOperatorImageOverrideHonoured
     (internal/cigates/chartgates/route_pgbouncer_test.go), which pins a CUSTOM name
     (ghcr.io/example/custom-operator) and expects it to render. D2693's own
     ruling on exactly this shape of test: "if it pins an arbitrary NAME as
     'the operator' and expects that to pass, it asserts a contract the chart
     never promised" -- posted to Linear with that test's exact assertion for
     the lead's ruling before this file was changed to accommodate or break it;
     see the Linear thread for the disposition that followed.

     What is left, and its residual: the name allowlist above, PLUS a DENYLIST
     of every non-dho image this project publishes or has published under a
     name a dho verb could otherwise reach by mistake (dev-hops-api and six
     retired Go image names -- the same shape of check migrate-job.yaml
     already ran for dev-hops-api alone, now shared and, per the r1 round's OWN
     follow-up finding, widened from 3 names to the full catalogue). The
     allowlist alone already closes the r1 round's reproduction (a bare
     `postgres` third-party image rendered clean); the denylist's remaining
     value is a SPECIFIC, named reason for the images this project actually
     ships or has retired, rather than the allowlist's generic message.
  2. A REAL digest, not a substring. `contains "@sha256:"` treated
     `repo@sha256:not-a-digest` as pinned; a real puller rejects the malformed
     reference. The digest must now be a well-formed 64 lowercase-hex-digit
     sha256.
  3. Pull policy. This helper does not choose a pull policy -- it only checks the
     one the caller already resolved. The caller must default it to
     "IfNotPresent", NEVER to the application image's pull policy (the APPLICATION image's
     policy): a local dev value of image.pullPolicy=Never used to leak onto a
     PINNED REGISTRY operator image, which then could not be pulled at all.
     route-activate-hooks.yaml already got this right (its own r2 P2 codex-review
     fix); provision-roles, river-migrate and migrate did not.

The lockstep check (this image pinned to the SAME commit as
the Go serving images) still runs at the end, unchanged.
*/}}
{{/*
CHAOS-6958 r1 round (executed finding): the denylist above was incomplete. A
real, currently-PUBLISHED, non-retired first-party image
(ghcr.io/full-chaos/dev-health-go-migrate, entrypoint
/usr/local/bin/dev-health-worker-migrate -- docker/go-worker.Dockerfile:173-175,
built and pushed by .github/workflows/docker-images.yml:1545) satisfied the
pin-shape rule and would have rendered clean, then failed at runtime exactly
like the Python image did; so would dev-health-go-api-tools (the plain
`runtime` target, no dho entrypoint at all -- Dockerfile:96-114) and three
retired names this chart ALREADY refuses at other call sites --
dev-health-go-stream-runner (go-workers.yaml:44-45), dev-health-go-reconciler
and dev-health-go-scheduler (go-workers.yaml:61) -- which this helper had not
picked up. Every name below is a real repository this project has published or
retired; dev-health-go-contractcheck is NOT here because its entrypoint IS
/usr/local/bin/dho (Dockerfile:121-124), the same as dev-health-go-dho and
dev-health-go-operator.
*/}}
{{- define "dev-health.pinnedOperatorImageCheck" -}}
{{- $image := .image -}}
{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{64}$" $image -}}
{{- /*
D2696 (lead): identity (the denylist below AND the name allowlist that
follows it) applies to DIGEST-pinned references only -- the P1 this closes
was reproduced digest-shaped (a bare `postgres@sha256:...`, and
`dev-health-go-migrate@sha256:...`). A sha-<12 hex>-TAGGED or `:local`
reference stays governed by the pin-shape and lockstep checks alone, same as
before this ticket: this is what keeps
test_provisioning_accepts_a_lockstep_match_and_a_sideloaded_local_image's
`dho:local` sideloaded fixture green untouched (a bare "dho" repo name, no
digest, used purely for local/kind development) without adding it to any
allowlist.
*/}}
{{- if $digestPinned -}}
{{- range $name, $label := (dict
    "dev-hops-api" "the Python api image"
    "dev-health-query-api" "the retired dev-health-query-api image"
    "dev-health-go-worker" "the retired dev-health-go-worker image"
    "dev-health-go-migrate" "the dev-health-go-migrate image (entrypoint dev-health-worker-migrate, not dho)"
    "dev-health-go-api-tools" "the dev-health-go-api-tools image (a debug/runtime image with no dho entrypoint)"
    "dev-health-go-stream-runner" "the retired dev-health-go-stream-runner image"
    "dev-health-go-reconciler" "the retired dev-health-go-reconciler image"
    "dev-health-go-scheduler" "the retired dev-health-go-scheduler image") -}}
{{- if regexMatch (printf "(^|/)%s([:@]|$)" $name) $image -}}
{{- fail (printf "%s (%s) is not a pinned dho image: it is %s, which has no dho entrypoint. It runs %s, which needs the dho (operator) image." $.flagLabel $image $label $.verb) -}}
{{- end -}}
{{- end -}}
{{- /*
D2693 (lead): the identity anchor is the image NAME (last path element,
registry/namespace/tag/digest all stripped) -- it must be one of the images
this project actually ships with a dho entrypoint: dev-health-go-operator or
dev-health-go-dho (D2696: exactly the names a hook TEMPLATE actually
references; dev-health-go-contractcheck, though it also carries a dho
entrypoint, joins only if a hook template ever references it -- none does
today). "Mirror-friendly": any registry or namespace still matches, so
re-pointing the SAME name at a private mirror, under a different digest, is
unaffected -- only the name itself is checked, the same regex
query-api-deployment.yaml and go-workers.yaml already use for the identical
question. This is checked in addition to, not instead of, the denylist above
(which gives a specific, named reason for the images this project has
actually published or retired under the wrong name; this catches every OTHER
wrong image, first-party or third-party, that the denylist does not
enumerate -- reproduced: a bare `postgres` image used to render clean here).
*/}}
{{- if not (or (regexMatch "(^|/)dev-health-go-operator([:@]|$)" $image) (regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)) -}}
{{- fail (printf "%s (%s) is not a pinned dho image: its name is neither dev-health-go-operator nor dev-health-go-dho (matched on the last path element; any registry or namespace is accepted). It runs %s, which needs the dho (operator) image." .flagLabel $image .verb) -}}
{{- end -}}
{{- end -}}
{{- $immutableTag := regexMatch "^.+:sha-[0-9a-f]{12}$" $image -}}
{{- $sideloadedLocal := and (regexMatch "^.+:local$" $image) (or (eq .pullPolicy "Never") (eq .pullPolicy "IfNotPresent")) -}}
{{- if not (or $digestPinned $immutableTag $sideloadedLocal) -}}
{{- fail (printf "%s (%s) is not a pinned dho image. It runs %s: set it to repo@sha256:<64 lowercase-hex-digit digest> or repo:sha-<12 hex>; repo:local is accepted only with pullPolicy Never or IfNotPresent." .flagLabel $image .verb) -}}
{{- end -}}
{{- include "dev-health.lockstepImageCheck" (dict "context" .context "image" $image "labelWithValue" (printf "%s (%s)" .flagLabel $image) "reason" (printf "%s must run the same build as the application it is targeting." .verb)) -}}
{{- end }}

{{/*
The ordering contract every workload carries (CHAOS-7421): "2", or the operator's
goWorkers.operationalOrderingContract when it is "2". Contract 1 is unsupported
(D3635), so any other value fails the render instead of reaching a workload (the Go
processes refuse it at start, and Python, which still reads "1" as legacy, would
serve it).
*/}}
{{- define "dev-health.operationalOrderingContract" -}}
{{- $value := .Values.goWorkers.operationalOrderingContract | default "2" | toString -}}
{{- if ne $value "2" -}}
{{- fail (printf "goWorkers.operationalOrderingContract=%q: only contract 2 is supported (contract 1 is unsupported, CHAOS-7421); leave it unset or set it to 2" $value) -}}
{{- end -}}
{{- $value -}}
{{- end -}}

{{/*
authService.phase, the one auth cutover key (values.yaml lists the phases). It refuses the keys it replaces, refuses
any value that is not a whole number from 0 to 5, refuses phase 5 (D6358: orgs owned waits for its own public-table writer
leaf), and renders the phase. Every ladder read goes through dev-health.authOn, so every render runs these checks.
*/}}
{{- define "dev-health.authPhase" -}}
{{- $v := .Values -}}
{{- if hasKey $v.goApi "authProxy" }}{{- fail "goApi.authProxy is replaced by authService.phase (0 go-api only, 1 auth-service dark, 2 sessions, 3 login and social sign-in, 4 users owned, 5 orgs owned): remove goApi.authProxy and set authService.phase" }}{{- end -}}
{{- if hasKey $v.authService "enabled" }}{{- fail "authService.enabled is replaced by authService.phase: enabled=true is phase 1 and enabled=false is phase 0; remove authService.enabled and set authService.phase" }}{{- end -}}
{{- if hasKey $v.authService "usersOwned" }}{{- fail "authService.usersOwned is replaced by authService.phase: users owned is phase 4; remove authService.usersOwned" }}{{- end -}}
{{- if hasKey $v.authService "orgsOwned" }}{{- fail "authService.orgsOwned is replaced by authService.phase: orgs owned is phase 5; remove authService.orgsOwned" }}{{- end -}}
{{- $p := $v.authService.phase -}}
{{- if not (and (has (kindOf $p) (list "int" "int64" "float64")) (eq (float64 $p) (floor (float64 $p))) (ge (float64 $p) 0.0) (le (float64 $p) 5.0)) }}{{- fail (printf "authService.phase must be a whole number from 0 to 5, got %s %v" (kindOf $p) $p) }}{{- end -}}
{{- if ge (int $p) 5 }}{{- fail (printf "authService.phase %d: phase 5 (orgs owned) needs its own public-table writer leaf (D6358): auth-service does not yet write the existing public organizations and memberships tables, so phase 4 is the highest phase this chart renders" (int $p)) }}{{- end -}}
{{- int $p -}}
{{- end -}}

{{/*
Every env name the chart renders for the auth-service container (Secret references and phase-derived settings), as a
JSON list. authService.extraEnv refuses each of them in any case: a later entry with the same name would shadow the
rendered one, a Secret reference included, and put a literal in the manifest. auth_phase_test.go walks the rendered
names and proves each is refused.
*/}}
{{- define "dev-health.authServiceEnvNames" -}}
{{- $names := list "AUTH_DATABASE_URI" "AUTH_JWT_SECRET_KEY" "AUTH_SESSION_SUCCESSOR_KEY" "AUTH_LOGIN_LOCKOUT_KEY" "AUTH_SOCIAL_LINK_ROUTES" "AUTH_ACCESS_TOKEN_CLASS" "AUTH_TOKEN_ISSUER_URL" "AUTH_JWT_AUDIENCE" "AUTH_USERS_OWNED" "AUTH_ORGS_OWNED" "AUTH_VALKEY_URI" "AUTH_SERVICE_TRUSTED_PROXIES" "OTEL_ENABLED" "OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_SAMPLE_RATE" "OTEL_SERVICE_NAME" "EMAIL_PROVIDER" "EMAIL_FROM_ADDRESS" "APP_BASE_URL" "AUTH_MAIL_ALLOW_CONSOLE" -}}
{{- /* EMAIL_API_KEY is appended, not listed, so the worker allow-list keeps the only listed spelling (a chartgates planted defect edits it). */ -}}
{{- $names = (append (append $names "EMAIL_API_KEY") "RESEND_API_KEY") -}}
{{- range $provider := list "GITHUB" "GITLAB" "GOOGLE" }}{{- range $suffix := list "CLIENT_ID" "CLIENT_SECRET" }}{{- $names = append $names (printf "SOCIAL_%s_%s" $provider $suffix) }}{{- end }}{{- end -}}
{{- toJson $names -}}
{{- end -}}

{{/*
Every env name the chart renders for the go-api container, as a JSON list. goApi.extraEnv refuses each of them in
any case, for the same reason as dev-health.authServiceEnvNames. auth_phase_test.go walks the rendered names. The DSN name is appended, not listed, so the worker DSN list keeps the only listed spelling (a chartgates planted defect edits it).
*/}}
{{- define "dev-health.goApiEnvNames" -}}
{{- $names := list "JWT_SECRET_KEY" "AUTH_DATABASE_SCHEMA" "HIDE_MIGRATED_CHILD_CONFIGS" "SYNC_WATERMARK_OVERLAP" "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS" "AUTH_PROXY_URL" "AUTH_PROXY_SESSIONS" "AUTH_PROXY_LOCAL_LOGIN" "AUTH_PROXY_CREDENTIALS" "AUTH_PROXY_SOCIAL" "AUTH_PROXY_OIDC" "AUTH_PROXY_SAML" "AUTH_SESSION_ISSUE_TOKEN" "AUTH_USERS_OWNED" "AUTH_ACCESS_TOKEN_ISSUER" "TRUSTED_PROXIES_NETWORK_FENCED" "DEV_HEALTH_LEGACY_INGEST_ENABLED" "DEV_HEALTH_LEGACY_INGEST_SUNSET" -}}
{{- $names = (append $names "API_DATABASE_URI") }}{{- toJson $names -}}
{{- end -}}

{{/*
"true" when an extraEnv entry carries a literal value under a secret-like name: the name ends, in any case, in
_SECRET, _PASSWORD, _TOKEN, _KEY, _DSN or _CREDENTIALS. Such a name may come only by valueFrom.secretKeyRef.
Call with the extraEnv entry.
*/}}
{{- define "dev-health.secretLikeLiteralEnv" -}}
{{- if and (kindIs "map" .) (hasKey . "value") (regexMatch "(?i)_(SECRET|PASSWORD|TOKEN|KEY|DSN|CREDENTIALS)$" (trim (toString .name))) -}}true{{- end -}}
{{- end -}}

{{/*
"true" when the value is a valid Kubernetes Secret key: ^[-._a-zA-Z0-9]+$, at most 253 characters, and not ".",
".." or a name that starts with "..". Call with the value.
*/}}
{{- define "dev-health.validSecretKey" -}}
{{- $k := toString . -}}
{{- if and (kindIs "string" .) (regexMatch "^[-._a-zA-Z0-9]+$" $k) (le (len $k) 253) (not (has $k (list "." ".."))) (not (hasPrefix ".." $k)) -}}true{{- end -}}
{{- end -}}

{{/*
Every auth check that must hold in every render, whatever the phase and whatever goApi.enabled, queryApi.enabled
and the other enable flags say. templates/auth-service.yaml includes it once, outside every gate.
- Class rule: every Secret key the auth values read is a valid Kubernetes Secret key (dev-health.validSecretKey):
  authService.database.secretKey, authService.migrate.secretKey, authService.signingKey.secretKey,
  authService.valkey.secretKey, authService.sessionKeys.successor.secretKey,
  authService.sessionKeys.loginLockout.secretKey, goApi.sessionIssue.secretKey and each set
  authService.keySet.keys[].secretKey.
- Phase 2 preconditions (both session readers, the successor key): query-api, the migrate Job and the River hooks
  carry AUTH_PROXY_SESSIONS from phase 2 even when go-api is off.
- Phase 3: the login lockout key is named.
- The access-token class, SSO and social link route keys.
*/}}
{{- define "dev-health.authValidate" -}}
{{- $a := .Values.authService -}}
{{- $session := default dict $a.sessionKeys -}}
{{- $keys := list (list "authService.database.secretKey" (default dict $a.database).secretKey) (list "authService.migrate.secretKey" (default dict $a.migrate).secretKey) (list "authService.signingKey.secretKey" (default dict $a.signingKey).secretKey) (list "authService.valkey.secretKey" (default dict $a.valkey).secretKey) (list "authService.sessionKeys.successor.secretKey" (default dict $session.successor).secretKey) (list "authService.sessionKeys.loginLockout.secretKey" (default dict $session.loginLockout).secretKey) (list "authService.mail.keys.provider" (default dict (default dict $a.mail).keys).provider) (list "authService.mail.keys.fromAddress" (default dict (default dict $a.mail).keys).fromAddress) (list "authService.mail.keys.apiKey" (default dict (default dict $a.mail).keys).apiKey) (list "authService.mail.keys.resendApiKey" (default dict (default dict $a.mail).keys).resendApiKey) (list "authService.mail.keys.appBaseURL" (default dict (default dict $a.mail).keys).appBaseURL) (list "goApi.sessionIssue.secretKey" (default dict .Values.goApi.sessionIssue).secretKey) -}}
{{- range $keys }}
{{- if not (include "dev-health.validSecretKey" (index . 1)) }}{{- fail (printf "%s must be a Secret key: letters, digits, '-', '_' and '.', at most 253 characters, not '.', '..' or a name that starts with '..'" (index . 0)) }}{{- end }}
{{- end }}
{{- range $i, $k := ((default dict $a.keySet).keys | default list) }}
{{- if and (kindIs "map" $k) $k.secretKey (not (include "dev-health.validSecretKey" $k.secretKey)) }}{{- fail (printf "authService.keySet.keys[%d].secretKey must be a Secret key: letters, digits, '-', '_' and '.', at most 253 characters, not '.', '..' or a name that starts with '..'" $i) }}{{- end }}
{{- end }}
{{- if eq (include "dev-health.authOn" (dict "flag" "sessions" "context" .)) "true" }}
{{- /* Phase 2 preconditions: query-api, the migrate Job and the River hooks carry AUTH_PROXY_SESSIONS from phase 2 even when go-api is off, so they are checked here, once. */}}
{{- $rd := .Values.authService.migrate.sessionReaders }}
{{- $missing := list }}
{{- if not (trim (toString $rd.goApi)) }}{{- $missing = append $missing "authService.migrate.sessionReaders.goApi (go-api's API_DATABASE_ROLE)" }}{{- end }}
{{- if not (trim (toString $rd.queryApi)) }}{{- $missing = append $missing "authService.migrate.sessionReaders.queryApi (query-api's registry login)" }}{{- end }}
{{- if $missing }}
{{- fail (printf "authService.phase >= 2 (sessions) needs the session-reader roles of both services: unset %s" (join ", " $missing)) }}
{{- end }}
{{- $miss := list }}
{{- if not (trim (toString .Values.authService.sessionKeys.successor.secretName)) }}{{- $miss = append $miss "authService.sessionKeys.successor.secretName" }}{{- end }}
{{- if not (include "dev-health.validSecretKey" .Values.authService.sessionKeys.successor.secretKey) }}{{- $miss = append $miss "authService.sessionKeys.successor.secretKey (a Secret key: letters, digits, '-', '_', '.')" }}{{- end }}
{{- if $miss }}
{{- fail (printf "authService.phase >= 2 (sessions) needs the auth-service session keys: unset %s (the access-token key is go-api's JWT_SECRET_KEY Secret)" (join ", " $miss)) }}
{{- end }}
{{- end }}
{{- if eq (include "dev-health.authOn" (dict "flag" "localLogin" "context" .)) "true" }}
{{- if not (include "dev-health.validSecretKey" .Values.authService.sessionKeys.loginLockout.secretKey) }}{{- fail "authService.phase >= 3 (login) needs authService.sessionKeys.loginLockout.secretKey to be a Secret key: letters, digits, '-', '_', '.'" }}{{- end }}
{{- end }}
{{- $_ := include "dev-health.authEdDSAIssuer" . }}
{{- $_ := include "dev-health.authSSOOn" (dict "flag" "oidc" "context" .) }}
{{- $_ := include "dev-health.authSSOOn" (dict "flag" "saml" "context" .) }}
{{- $linkRoutes := eq (include "dev-health.authBool" (dict "value" .Values.authService.socialLinkRoutes "key" "authService.socialLinkRoutes")) "true" }}
{{- if and $linkRoutes (not (include "dev-health.authOn" (dict "flag" "sessions" "context" .))) }}{{- fail "authService.socialLinkRoutes needs authService.phase >= 2: the link routes need the session secrets" }}{{- end }}
{{- end -}}

{{/*
"true" when authService.phase turns the ladder flag on, else "". Call with (dict "flag" <name> "context" $).
The ladder (each flag is on from its phase up): service 1, sessions 2, localLogin 3, social 3, credentials 4,
usersOwned 4, orgsOwned 5. credentials moves with usersOwned: go-api refuses AUTH_USERS_OWNED unless the credential routes
go to auth-service. auth-service mounts them from phase 2 (a 503 stub until users are owned).
*/}}
{{- define "dev-health.authOn" -}}
{{- $ladder := dict "service" 1 "sessions" 2 "localLogin" 3 "social" 3 "credentials" 4 "usersOwned" 4 "orgsOwned" 5 -}}
{{- if not (hasKey $ladder .flag) }}{{- fail (printf "dev-health.authOn: %s is not a ladder flag" .flag) }}{{- end -}}
{{- if ge (int (include "dev-health.authPhase" .context)) (int (get $ladder .flag)) -}}true{{- end -}}
{{- end -}}

{{/*
OIDC and SAML through auth-service (authService.sso.oidc / .saml): "true" or "". true is refused: the chart does not
render auth-service's SSO settings, so go-api would forward to routes auth-service does not mount.
Call with (dict "flag" "oidc"|"saml" "context" $).
*/}}
{{- define "dev-health.authSSOOn" -}}
{{- $key := printf "authService.sso.%s" .flag -}}
{{- $sso := default dict .context.Values.authService.sso -}}
{{- if eq (include "dev-health.authBool" (dict "value" (index $sso .flag) "key" $key)) "true" }}{{- fail (printf "%s=true is refused: the chart does not render auth-service's SSO settings (state key, base URL, encryption key) yet, so its %s routes would not mount" $key (upper .flag)) }}{{- end -}}
{{- end -}}

{{/*
"" when one authService.networkPolicy.egress.mail.to entry is acceptable, else the reason. Call with
(dict "peer" <entry> "ports" <authService.networkPolicy.egress.mail.ports>). A peer selects pods, namespaces or one
address block; an empty selector admits everything of its kind and an empty entry admits every destination. An
ipBlock is one CIDR checked by dev-health.otlpPeerCidrProblem (IPv4 /8 or longer, IPv6 /32 or longer, nothing that
overlaps the IPv4-mapped or NAT64 ranges, in any spelling). The one exception (D6427, a provider with no fixed addresses): exactly 0.0.0.0/0 or ::/0,
only with ports [TCP 443] alone and an except list holding every private, shared, link-local and loopback network
below, and for IPv6 also the mapped and NAT64 ranges that would reach internal IPv4 addresses.
*/}}
{{- define "dev-health.authMailPeerProblem" -}}
{{- $peer := .peer -}}
{{- if not (kindIs "map" $peer) -}}is not an object
{{- else if and (not (and (kindIs "map" $peer.podSelector) (gt (len $peer.podSelector) 0))) (not (and (kindIs "map" $peer.namespaceSelector) (gt (len $peer.namespaceSelector) 0))) (not (kindIs "map" $peer.ipBlock)) -}}has no non-empty podSelector, namespaceSelector or ipBlock
{{- else if kindIs "map" $peer.ipBlock -}}
{{- $cidr := toString $peer.ipBlock.cidr -}}
{{- if not (regexMatch "^[0-9a-fA-F:.]+/[0-9]{1,3}$" $cidr) -}}ipBlock.cidr must be one address/prefix
{{- else -}}
{{- $parts := splitList "/" $cidr -}}
{{- $ip := lower (index $parts 0) -}}
{{- $prefix := int (index $parts 1) -}}
{{- $v6 := contains ":" $ip -}}
{{- if eq $prefix 0 -}}
{{- $required := ternary (list "fc00::/7" "fe80::/10" "::1/128" "::ffff:0:0/96" "64:ff9b::/96") (list "10.0.0.0/8" "172.16.0.0/12" "192.168.0.0/16" "100.64.0.0/10" "169.254.0.0/16" "127.0.0.0/8") $v6 -}}
{{- $except := list -}}
{{- range ($peer.ipBlock.except | default list) }}{{- $except = append $except (lower (toString .)) }}{{- end -}}
{{- $missing := list -}}
{{- range $required }}{{- if not (has . $except) }}{{- $missing = append $missing . }}{{- end }}{{- end -}}
{{- $ports := .ports | default list -}}
{{- $port := dict -}}
{{- if eq (len $ports) 1 }}{{- $port = index $ports 0 }}{{- end -}}
{{- if not (or (and $v6 (eq $ip "::")) (and (not $v6) (eq $ip "0.0.0.0"))) -}}ipBlock.cidr with prefix 0 must be exactly 0.0.0.0/0 or ::/0
{{- else if not (and (eq (len $ports) 1) (kindIs "map" $port) (eq (upper (toString (default "TCP" $port.protocol))) "TCP") (hasKey $port "port") (eq (int $port.port) 443) (not (hasKey $port "endPort"))) -}}an ipBlock with prefix 0 is accepted only with authService.networkPolicy.egress.mail.ports of TCP 443 alone
{{- else if $missing -}}ipBlock.cidr {{ $cidr }} needs except entries {{ join ", " $missing }}
{{- end -}}
{{- else -}}
{{- include "dev-health.otlpPeerCidrProblem" $cidr -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The access-token class (authService.accessToken). Checks it and renders the eddsa issuer URL, or "" for any other
class: one value for auth-service (AUTH_TOKEN_ISSUER_URL) and go-api (AUTH_ACCESS_TOKEN_ISSUER). The pattern is
auth-service's issuerURLPattern (internal/auth/authconfig), so a value the service would refuse fails the render.
*/}}
{{- define "dev-health.authEdDSAIssuer" -}}
{{- $t := default dict .Values.authService.accessToken -}}
{{- if not (and (or (kindIs "invalid" $t.class) (kindIs "string" $t.class)) (or (kindIs "invalid" $t.issuerURL) (kindIs "string" $t.issuerURL))) }}{{- fail "authService.accessToken.class and authService.accessToken.issuerURL must be strings" }}{{- end -}}
{{- $class := $t.class | default "" -}}
{{- $issuer := $t.issuerURL | default "" -}}
{{- if $class -}}
{{- if not (has $class (list "hs256_compat" "eddsa")) }}{{- fail "authService.accessToken.class must be empty, hs256_compat or eddsa" }}{{- end -}}
{{- if not (include "dev-health.authOn" (dict "flag" "sessions" "context" .)) }}{{- fail "authService.accessToken.class needs authService.phase >= 2: auth-service mints access tokens only on its session routes" }}{{- end -}}
{{- end -}}
{{- if eq $class "eddsa" -}}
{{- if not (regexMatch "^https://[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?(/[A-Za-z0-9_~-][A-Za-z0-9._~-]{0,63}){0,8}$" $issuer) }}{{- fail "authService.accessToken.class=eddsa needs authService.accessToken.issuerURL: an https URL with a lowercase host, no trailing slash, userinfo, query or fragment" }}{{- end -}}
{{- $issuer -}}
{{- else if $issuer -}}
{{- fail "authService.accessToken.issuerURL is set but authService.accessToken.class is not eddsa: the issuer is rendered only for eddsa tokens" }}
{{- end -}}
{{- end -}}

{{/*
CHAOS-10231: "true" when go-api is enabled and go-api forwards /api/v1/auth/* routes to auth-service (phase >= 2:
every other aggregate needs sessions), else "".
*/}}
{{- define "dev-health.authProxyOn" -}}
{{- if and .Values.goApi.enabled (include "dev-health.authOn" (dict "flag" "sessions" "context" .)) -}}true{{- end -}}
{{- end -}}

{{/*
The auth schema's name: ONE source for the auth migrator, the auth service and
the ops migrator (the ops migrator exempts exactly this schema from its
foreign-database check, so all three must read the same value).
*/}}
{{- /*
The auth-migrate grant-narrowing acknowledgement (authService.migrate.acknowledgeGrantNarrowing):
empty renders nothing; otherwise the value must be the 16 lowercase hex characters of the
digest a refused run prints, or the render fails. The result is the one flag, shared by the
schema Job and the grants Job.
*/ -}}
{{- define "dev-health.authAckGrantNarrowing" -}}
{{- $ack := trim (toString (default "" .Values.authService.migrate.acknowledgeGrantNarrowing)) -}}
{{- if $ack -}}
{{- if not (regexMatch "^[0-9a-f]{16}$" $ack) }}{{- fail "authService.migrate.acknowledgeGrantNarrowing must be the 16 lowercase hex characters of the digest a refused auth-migrate run prints" }}{{- end -}}
-ack-grant-narrowing={{ $ack }}
{{- end -}}
{{- end }}

{{/*
-defer-narrowing for both auth-migrate Jobs, or "". A Job that passes the acknowledgement while auth-service does not
own users (authOn "usersOwned" off; today phase 3 or lower) also defers: that is the flip-back release, whose old owned auth-service pods still serve while its pre-upgrade hooks
run, so the users-owned grants it would revoke are kept (CHAOS-11581). authService.migrate.narrowAfterFlipBack, set in
a LATER release after no pod of the old owned ReplicaSet is left, renders the acknowledgement without it, so that
release narrows. The flag's refusals are dev-health.authNarrowAfterFlipBackGuard.
*/}}
{{- define "dev-health.authDeferGrantNarrowing" -}}
{{- $m := .Values.authService.migrate -}}
{{- $narrow := eq (include "dev-health.authBool" (dict "value" $m.narrowAfterFlipBack "key" "authService.migrate.narrowAfterFlipBack")) "true" -}}
{{- $ack := include "dev-health.authAckGrantNarrowing" . -}}
{{- if and $ack (not $narrow) (ne (include "dev-health.authOn" (dict "flag" "usersOwned" "context" .)) "true") -}}
-defer-narrowing
{{- end -}}
{{- end }}

{{/*
The authService.migrate.narrowAfterFlipBack refusals, run on every render by auth-narrow-after-flipback-guard.yaml
(not only when an auth-migrate Job renders): the flag needs the acknowledgement, checked by the same helper as the Jobs
(dev-health.authAckGrantNarrowing), and is refused at authService.phase 4
or higher and while auth-service owns users.
*/}}
{{- define "dev-health.authNarrowAfterFlipBackGuard" -}}
{{- $m := .Values.authService.migrate -}}
{{- if eq (include "dev-health.authBool" (dict "value" $m.narrowAfterFlipBack "key" "authService.migrate.narrowAfterFlipBack")) "true" -}}
{{- if not (include "dev-health.authAckGrantNarrowing" .) }}{{- fail "authService.migrate.narrowAfterFlipBack=true needs authService.migrate.acknowledgeGrantNarrowing: the digest a refused auth-migrate run prints for the deferred users-owned grants" }}{{- end -}}
{{- if or (ge (int (include "dev-health.authPhase" .)) 4) (eq (include "dev-health.authOn" (dict "flag" "usersOwned" "context" .)) "true") }}{{- fail "authService.migrate.narrowAfterFlipBack is refused at authService.phase 4 or higher and while auth-service owns users" }}{{- end -}}
{{- end -}}
{{- end }}

{{/*
A strict boolean switch of authService: renders "true" or "false", and FAILS the render for anything that is
not a YAML boolean (a string such as "false" is non-empty and would read as true; null is no value). Every read
of authService.backfill.enabled, authService.backfill.dryRun, authService.socialLinkRoutes and authService.sso.*
goes through it. Call with (dict "value" <value> "key" "<dotted key>").
*/}}
{{- define "dev-health.authBool" -}}
{{- if not (kindIs "bool" .value) }}{{- fail (printf "%s must be the boolean true or false, got %s %v" .key (kindOf .value) .value) }}{{- end }}
{{- .value }}
{{- end }}

{{- define "dev-health.authDatabaseSchema" -}}
{{- default "auth" .Values.authService.database.schema -}}
{{- end }}

{{/*
Database DSN names a Go worker group never takes from the shared Secret or the
shared ConfigMap. goWorkers.secretKeys refuses them and each worker group blanks
them, from this one list (internal/platform/dsnclass classifies the same names).
*/}}
{{- define "dev-health.goWorkerDSNNames" -}}
{{- toJson (list "DATABASE_URI" "DATABASE_URL" "POSTGRES_URL" "API_DATABASE_URI" "GO_API_REGISTRY_POSTGRES_URI" "MIGRATION_DATABASE_URI" "AUTH_DATABASE_URI" "AUTH_MIGRATION_DATABASE_URI" "COORDINATOR_DATABASE_URI" "WORKER_DATABASE_URI") -}}
{{- end }}

{{/*
The closed list of keys of the shared Secret a Go worker group may take
(goWorkers.secretKeys). A name outside it refuses the render, so an unlisted
database DSN name cannot reach a worker pod either.
*/}}
{{- define "dev-health.goWorkerSecretKeyNames" -}}
{{- toJson (list "APP_BASE_URL" "CLICKHOUSE_URI" "EMAIL_API_KEY" "EMAIL_FROM_ADDRESS" "EMAIL_PROVIDER" "EXPECTED_WORKER_GROUPS" "INVESTMENT_LLM_BATCH_MODE" "INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS" "LICENSE_PRIVATE_KEY" "LINEAR_API_KEY" "LLM_MODEL" "LLM_PROVIDER" "OPENAI_API_KEY" "PAGER_DUTY_CLIENT_ID" "PAGER_DUTY_REDIRECT_URI" "PAGER_DUTY_SECRET" "SETTINGS_ENCRYPTION_KEY" "STRIPE_SECRET_KEY" "STRIPE_WEBHOOK_SECRET" "WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH") -}}
{{- end }}

{{/*
The query-api pods leave the release-wide NetworkPolicy and get their own ingress and egress policies while a listener
that trusts internal identity headers (internal or MCP) is on: the release-wide in-namespace rule admits every port.
Renders "true" or nothing.
*/}}
{{- define "dev-health.queryApiOwnNetworkPolicy" -}}
{{- if and .Values.networkPolicy.enabled .Values.queryApi.enabled (or .Values.queryApi.internal.enabled .Values.queryApi.mcp.enabled) }}true{{- end }}
{{- end }}

{{/*
Egress rules shared by the release-wide, go-api and query-api egress policies: DNS (UDP and TCP), the release
namespace, HTTPS, then networkPolicy.extraEgress. The auth pods never use these rules.
*/}}
{{- define "dev-health.networkPolicyEgress" -}}
{{- range $i, $rule := .Values.networkPolicy.extraEgress }}
{{- if not (kindIs "map" $rule) }}
{{- fail (printf "networkPolicy.extraEgress[%d] must be a NetworkPolicy egress rule: an object with to and/or ports" $i) }}
{{- end }}
{{- range $key := keys $rule | sortAlpha }}
{{- if not (has $key (list "to" "ports")) }}
{{- fail (printf "networkPolicy.extraEgress[%d] takes only to and ports, not %q: Kubernetes drops an unknown key, so the rule would be wider than written" $i $key) }}
{{- end }}
{{- end }}
{{- range $key := list "to" "ports" }}
{{- if and (hasKey $rule $key) (not (kindIs "slice" (index $rule $key))) (not (kindIs "invalid" (index $rule $key))) }}
{{- fail (printf "networkPolicy.extraEgress[%d].%s must be a list" $i $key) }}
{{- end }}
{{- end }}
{{- $to := $rule.to | default list }}
{{- $ports := $rule.ports | default list }}
{{- if and (empty $to) (empty $ports) }}
{{- fail (printf "networkPolicy.extraEgress[%d] has neither a non-empty to nor non-empty ports: Kubernetes reads that rule as every destination on every port" $i) }}
{{- end }}
{{- range $j, $port := $ports }}
{{- if not (kindIs "map" $port) }}
{{- fail (printf "networkPolicy.extraEgress[%d].ports[%d] must be an object with port (and protocol, endPort)" $i $j) }}
{{- end }}
{{- range $key := keys $port | sortAlpha }}
{{- if not (has $key (list "protocol" "port" "endPort")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].ports[%d] takes only protocol, port and endPort, not %q" $i $j $key) }}
{{- end }}
{{- end }}
{{- $number := has (kindOf $port.port) (list "float64" "int64" "int") }}
{{- if not (or (and $number (eq (float64 $port.port) (floor (float64 $port.port))) (ge (int $port.port) 1) (le (int $port.port) 65535)) (and (kindIs "string" $port.port) (ne $port.port ""))) }}
{{- fail (printf "networkPolicy.extraEgress[%d].ports[%d] needs a port (1-65535 or a port name): Kubernetes reads an entry with no port as every port of its protocol" $i $j) }}
{{- end }}
{{- if and (hasKey $port "protocol") (not (has (toString $port.protocol) (list "TCP" "UDP" "SCTP"))) }}
{{- fail (printf "networkPolicy.extraEgress[%d].ports[%d].protocol %v is not TCP, UDP or SCTP" $i $j $port.protocol) }}
{{- end }}
{{- if hasKey $port "endPort" }}
{{- if not (and $number (has (kindOf $port.endPort) (list "float64" "int64" "int")) (eq (float64 $port.endPort) (floor (float64 $port.endPort))) (ge (int $port.endPort) (int $port.port)) (le (int $port.endPort) 65535)) }}
{{- fail (printf "networkPolicy.extraEgress[%d].ports[%d].endPort needs a numeric port and must be a number from port to 65535" $i $j) }}
{{- end }}
{{- end }}
{{- end }}
{{- range $j, $peer := $to }}
{{- if not (kindIs "map" $peer) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d] must be an object with podSelector, namespaceSelector or ipBlock" $i $j) }}
{{- end }}
{{- if empty (keys $peer) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d] is empty: name a podSelector, namespaceSelector or ipBlock" $i $j) }}
{{- end }}
{{- range $key := keys $peer | sortAlpha }}
{{- if not (has $key (list "podSelector" "namespaceSelector" "ipBlock")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d] takes only podSelector, namespaceSelector and ipBlock, not %q" $i $j $key) }}
{{- end }}
{{- end }}
{{- range $selector := list "podSelector" "namespaceSelector" }}
{{- if hasKey $peer $selector }}
{{- if not (kindIs "map" (index $peer $selector)) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].%s must be an object with matchLabels and/or matchExpressions" $i $j $selector) }}
{{- end }}
{{- range $key := keys (index $peer $selector) | sortAlpha }}
{{- if not (has $key (list "matchLabels" "matchExpressions")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].%s takes only matchLabels and matchExpressions, not %q: Kubernetes drops an unknown key, so the selector would be wider than written" $i $j $selector $key) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- if hasKey $peer "ipBlock" }}
{{- if not (and (kindIs "map" $peer.ipBlock) (kindIs "string" $peer.ipBlock.cidr) (ne $peer.ipBlock.cidr "")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].ipBlock needs a cidr" $i $j) }}
{{- end }}
{{- range $key := keys $peer.ipBlock | sortAlpha }}
{{- if not (has $key (list "cidr" "except")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].ipBlock takes only cidr and except, not %q: Kubernetes drops an unknown key, so the block would be wider than written" $i $j $key) }}
{{- end }}
{{- end }}
{{- if hasKey $peer.ipBlock "except" }}
{{- if not (kindIs "slice" $peer.ipBlock.except) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].ipBlock.except must be a list of CIDR strings" $i $j) }}
{{- end }}
{{- range $peer.ipBlock.except }}
{{- if not (and (kindIs "string" .) (ne . "")) }}
{{- fail (printf "networkPolicy.extraEgress[%d].to[%d].ipBlock.except must be a list of CIDR strings" $i $j) }}
{{- end }}
{{- end }}
{{- end }}
{{- if and (empty $ports) (regexMatch "/0+$" $peer.ipBlock.cidr) (empty $peer.ipBlock.except) }}
{{- fail (printf "networkPolicy.extraEgress[%d] sends to ipBlock %s with no except and no ports: that is every destination on every port; name the ports" $i $peer.ipBlock.cidr) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
# DNS resolution
- to:
    - namespaceSelector: {}
  ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
# Internal communication
- to:
    - podSelector: {}
# External HTTPS (GitHub, GitLab, Jira, Stripe APIs)
- to:
    - ipBlock:
        cidr: 0.0.0.0/0
  ports:
    - protocol: TCP
      port: 443
{{- range .Values.networkPolicy.extraEgress }}
- {{ toYaml . | nindent 2 | trim }}
{{- end }}
{{- end }}

{{/*
The integer value of a dotted IPv4 address (validated by the caller).
*/}}
{{- define "dev-health.ipv4Int" -}}
{{- $o := splitList "." (toString .) -}}
{{- add (add (mul (int (index $o 0)) 16777216) (mul (int (index $o 1)) 65536)) (add (mul (int (index $o 2)) 256) (int (index $o 3))) -}}
{{- end -}}

{{/*
One authService.trustedProxies entry is what internal/auth/clientaddr ParseTrust accepts (a bare IP address, or a strict CIDR
network: canonical prefix digits, no host bits, no zone), so a rendered value never stops auth-service at start. Prints "true" when
the entry is valid, nothing otherwise. The chart refuses /0 and IPv4-mapped forms on top of this. An exclusion entry
("!<address>") is this check on its address, plus the IPv4-only rules in auth-service.yaml.
*/}}
{{- define "dev-health.trustedProxyEntry" -}}
{{- $parts := splitList "/" (toString .) -}}
{{- $addr := index $parts 0 -}}
{{- $hasPrefix := eq (len $parts) 2 -}}
{{- $prefix := "" -}}
{{- if $hasPrefix }}{{- $prefix = index $parts 1 }}{{- end -}}
{{- $ok := false -}}
{{- if and (le (len $parts) 2) (or (not $hasPrefix) (regexMatch "^(0|[1-9][0-9]{0,2})$" $prefix)) -}}
{{- $octet := "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])" -}}
{{- if regexMatch (printf "^%s(\\.%s){3}$" $octet $octet) $addr -}}
{{- $ok = true -}}
{{- if $hasPrefix -}}
{{- $p := int $prefix -}}
{{- if gt $p 32 }}{{- $ok = false }}{{- else -}}
{{- $o := splitList "." $addr -}}
{{- $v := add (add (mul (int (index $o 0)) 16777216) (mul (int (index $o 1)) 65536)) (add (mul (int (index $o 2)) 256) (int (index $o 3))) -}}
{{- $pow := 1 -}}
{{- range until (int (sub 32 $p)) }}{{- $pow = mul $pow 2 }}{{- end -}}
{{- if ne (int (mod $v $pow)) 0 }}{{- $ok = false }}{{- end -}}
{{- end -}}
{{- end -}}
{{- else if and (contains ":" $addr) (regexMatch "^[0-9A-Fa-f:]+$" $addr) (not (contains ":::" $addr)) -}}
{{- $groups := list -}}
{{- $shape := false -}}
{{- if contains "::" $addr -}}
{{- $halves := splitn "::" 2 $addr -}}
{{- $left := $halves._0 -}}
{{- $right := $halves._1 -}}
{{- $half := "^([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4})*)?$" -}}
{{- if and (regexMatch $half $left) (regexMatch $half $right) -}}
{{- $l := list -}}{{- if $left }}{{- $l = splitList ":" $left }}{{- end -}}
{{- $r := list -}}{{- if $right }}{{- $r = splitList ":" $right }}{{- end -}}
{{- if le (add (len $l) (len $r)) 7 -}}
{{- $shape = true -}}
{{- $zeros := list -}}
{{- range until (int (sub 8 (add (len $l) (len $r)))) }}{{- $zeros = append $zeros "0" }}{{- end -}}
{{- $groups = concat $l $zeros $r -}}
{{- end -}}
{{- end -}}
{{- else if regexMatch "^([0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}$" $addr -}}
{{- $shape = true -}}
{{- $groups = splitList ":" $addr -}}
{{- end -}}
{{- if $shape -}}
{{- $ok = true -}}
{{- /* IPv4-mapped in every spelling: five zero groups then ffff, any case, compressed or not, hex tail. */ -}}
{{- if and (regexMatch "^0{1,4}$" (index $groups 0)) (regexMatch "^0{1,4}$" (index $groups 1)) (regexMatch "^0{1,4}$" (index $groups 2)) (regexMatch "^0{1,4}$" (index $groups 3)) (regexMatch "^0{1,4}$" (index $groups 4)) (eq (lower (index $groups 5)) "ffff") }}{{- $ok = false }}{{- end -}}
{{- if $hasPrefix -}}
{{- $p := int $prefix -}}
{{- if gt $p 128 }}{{- $ok = false }}{{- else -}}
{{- $digits := "0123456789abcdef" -}}
{{- range $i, $g := $groups -}}
{{- $val := 0 -}}
{{- range $c := (regexSplit "" (lower $g) -1) }}
{{- if $c }}{{- range $d := until 16 }}{{- if eq (substr $d (int (add1 $d)) $digits) $c }}{{- $val = add (mul $val 16) $d }}{{- end }}{{- end }}{{- end }}
{{- end -}}
{{- $covered := int (sub $p (mul $i 16)) -}}
{{- if lt $covered 0 }}{{- $covered = 0 }}{{- end -}}
{{- if gt $covered 16 }}{{- $covered = 16 }}{{- end -}}
{{- $pow := 1 -}}
{{- range until (int (sub 16 $covered)) }}{{- $pow = mul $pow 2 }}{{- end -}}
{{- if ne (int (mod $val $pow)) 0 }}{{- $ok = false }}{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $ok }}true{{ end -}}
{{- end -}}

{{/*
authService.database.maxConns -> AUTH_DATABASE_MAX_CONNS. A whole number from
1 to 32; anything else fails the render. A values file and --set-json decode
numbers as float64, --set as int64, so both are accepted when whole; a string,
a bool, null or a fraction is not.
*/}}
{{- define "dev-health.authMaxConns" -}}
{{- $v := .Values.authService.database.maxConns -}}
{{- $ok := false -}}
{{- if has (kindOf $v) (list "int" "int64" "float64") -}}
{{- if eq (toString (int64 $v)) (toString $v) -}}
{{- if and (ge (int64 $v) 1) (le (int64 $v) 32) -}}{{- $ok = true -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $ok -}}{{- fail "authService.database.maxConns must be a whole number from 1 to 32: it is the connection pool of each auth-service replica, replicas x maxConns (plus one surge pod while rolling) is what the auth database must hold" -}}{{- end -}}
{{- int64 $v -}}
{{- end -}}

{{/*
The rolling-update surge of the auth-service Deployment: the one figure the
connection budget and the Deployment strategy share.
*/}}
{{- define "dev-health.authMaxSurge" -}}1{{- end -}}

{{/*
authService.database.connectionBudget: the connections the release may hold on
the auth database, that is max_connections minus superuser_reserved_connections
(Postgres defaults 100 - 3 = 97). 0 turns the check off. Above 0 the render
fails when
  (authService.replicas + surge) x authService.database.maxConns
  + every go-pgbouncer pool (one per (user, database) pair: the transaction
    pooler counts its own role at transaction.poolSize and each
    transaction.extraUsers entry at its own poolSize, else at the transaction
    poolSize, ALL of them times (1 + the rollout surge pods of the transaction
    pooler): a surge pod opens every one of those pools again; the queue-session
    and coordinator-session poolers count poolSize x (1 + their own surge pods))
exceeds it. Poolers count only while goWorkers and goWorkers.pgbouncer are on.
A static upper bound: a pool holds server connections only while it needs them.
Every pooler counts its surge pod (CHAOS-11583): rollingUpdate.maxSurge when set,
else 1. CHAOS-11521 kind measurement: a session pooler's surge pod opened no
server connection (the old pod keeps its clients until it is stopped, 24 to 31 s),
so for the session poolers the count is an upper bound; the transaction pooler's
surge pod serves new connections at once (+18 measured).
*/}}
{{/*
One number the connection budget adds or multiplies: a whole number from min to
max, else the render fails naming the key. The bound keeps the sum from
wrapping, whether or not the schema ran (helm --skip-schema-validation).
*/}}
{{- define "dev-health.budgetTerm" -}}
{{- $v := .value -}}
{{- $ok := false -}}
{{- if has (kindOf $v) (list "int" "int64" "float64") -}}
{{- if eq (toString (int64 $v)) (toString $v) -}}
{{- if and (ge (int64 $v) (int64 .min)) (le (int64 $v) (int64 .max)) -}}{{- $ok = true -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $ok -}}{{- fail (printf "%s must be a whole number from %d to %d when authService.database.connectionBudget is set: the budget adds it with the other pools" .key (int64 .min) (int64 .max)) -}}{{- end -}}
{{- int64 $v -}}
{{- end -}}

{{/*
goWorkers.pgbouncer.<pooler>.rollingUpdate (pooler: transaction, queueSession or
coordinatorSession), validated, as a JSON object for the Deployment's
rollingUpdate; empty when unset (no strategy key: the Kubernetes default, 25
percent, which is one surge pod at one replica). Whole numbers only (the budget
counts surge pods, a percentage cannot be counted) and maxSurge 0 needs
maxUnavailable of 1 or more (Kubernetes refuses both at 0). Takes
(dict "ctx" . "pooler" "<name>").
*/}}
{{- define "dev-health.pgbouncerRollingUpdate" -}}
{{- $pooler := .pooler -}}
{{- $ru := (get .ctx.Values.goWorkers.pgbouncer $pooler).rollingUpdate -}}
{{- if and (not (kindIs "invalid" $ru)) (not (kindIs "map" $ru)) -}}{{- fail (printf "goWorkers.pgbouncer.%s.rollingUpdate must be a map of maxSurge and maxUnavailable, or unset" $pooler) -}}{{- end -}}
{{- $ru = $ru | default dict -}}
{{- if $ru -}}
{{- $out := dict -}}
{{- range $k := list "maxSurge" "maxUnavailable" -}}
{{- if hasKey $ru $k -}}
{{- $v := get $ru $k -}}
{{- $ok := false -}}
{{- if has (kindOf $v) (list "int" "int64" "float64") -}}
{{- if and (eq (toString (int64 $v)) (toString $v)) (ge (int64 $v) 0) (le (int64 $v) 10) -}}{{- $ok = true -}}{{- end -}}
{{- end -}}
{{- if not $ok -}}{{- fail (printf "goWorkers.pgbouncer.%s.rollingUpdate.%s must be a whole number from 0 to 10: the connection budget counts surge pods, a percentage cannot be counted" $pooler $k) -}}{{- end -}}
{{- $_ := set $out $k (int64 $v) -}}
{{- end -}}
{{- end -}}
{{- range $k, $_ := $ru -}}
{{- if not (has $k (list "maxSurge" "maxUnavailable")) -}}{{- fail (printf "goWorkers.pgbouncer.%s.rollingUpdate.%s is not a known key: use maxSurge and maxUnavailable" $pooler $k) -}}{{- end -}}
{{- end -}}
{{- if and (hasKey $out "maxSurge") (eq (int64 (get $out "maxSurge")) 0) (lt (int64 (default 0 (get $out "maxUnavailable"))) 1) -}}{{- fail (printf "goWorkers.pgbouncer.%s.rollingUpdate: maxSurge 0 needs maxUnavailable of 1 or more (Kubernetes refuses a rolling update that may neither surge nor remove a pod)" $pooler) -}}{{- end -}}
{{- toJson $out -}}
{{- end -}}
{{- end -}}

{{/*
Surge pods of a pgbouncer pooler during a rollout, the figure the budget check
and the Deployment share: rollingUpdate.maxSurge when set, else 1 (the
Kubernetes default of 25 percent rounds up to one pod at one replica). Takes
(dict "ctx" . "pooler" "<name>").
*/}}
{{- define "dev-health.pgbouncerSurge" -}}
{{- $j := include "dev-health.pgbouncerRollingUpdate" . -}}
{{- if $j -}}
{{- $ru := fromJson $j -}}
{{- if hasKey $ru "maxSurge" -}}{{- int64 (get $ru "maxSurge") -}}{{- else -}}1{{- end -}}
{{- else -}}1{{- end -}}
{{- end -}}

{{- define "dev-health.authConnectionBudget" -}}
{{- $v := .Values.authService.database.connectionBudget -}}
{{- $ok := false -}}
{{- if has (kindOf $v) (list "int" "int64" "float64") -}}
{{- if eq (toString (int64 $v)) (toString $v) -}}
{{- if ge (int64 $v) 0 -}}{{- $ok = true -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $ok -}}{{- fail "authService.database.connectionBudget must be a whole number of 0 or more (0 = no check): the connections the auth database may hold, max_connections minus superuser_reserved_connections" -}}{{- end -}}
{{- if gt (int64 $v) 0 -}}
{{- $pools := 0 -}}
{{- $txnDetail := "no go-pgbouncer pools" -}}
{{- if and .Values.goWorkers.enabled .Values.goWorkers.pgbouncer.enabled -}}
{{- $p := .Values.goWorkers.pgbouncer -}}
{{- $txn := int64 (include "dev-health.budgetTerm" (dict "key" "goWorkers.pgbouncer.transaction.poolSize" "value" $p.transaction.poolSize "min" 1 "max" 1000)) -}}
{{- $users := $txn -}}
{{- range $u := ($p.transaction.extraUsers | default (list)) -}}
{{- if hasKey $u "poolSize" -}}
{{- $users = add $users (int64 (include "dev-health.budgetTerm" (dict "key" "goWorkers.pgbouncer.transaction.extraUsers[].poolSize" "value" $u.poolSize "min" 1 "max" 1000))) -}}
{{- else -}}
{{- $users = add $users $txn -}}
{{- end -}}
{{- end -}}
{{- $queue := int64 (include "dev-health.budgetTerm" (dict "key" "goWorkers.pgbouncer.queueSession.poolSize" "value" $p.queueSession.poolSize "min" 1 "max" 1000)) -}}
{{- $coordinator := int64 (include "dev-health.budgetTerm" (dict "key" "goWorkers.pgbouncer.coordinatorSession.poolSize" "value" $p.coordinatorSession.poolSize "min" 1 "max" 1000)) -}}
{{- $surge := int64 (include "dev-health.pgbouncerSurge" (dict "ctx" . "pooler" "transaction")) -}}
{{- $queueSurge := int64 (include "dev-health.pgbouncerSurge" (dict "ctx" . "pooler" "queueSession")) -}}
{{- $coordinatorSurge := int64 (include "dev-health.pgbouncerSurge" (dict "ctx" . "pooler" "coordinatorSession")) -}}
{{- $txnTotal := mul $users (add1 $surge) -}}
{{- $queueTotal := mul $queue (add1 $queueSurge) -}}
{{- $coordinatorTotal := mul $coordinator (add1 $coordinatorSurge) -}}
{{- $pools = add $txnTotal (add $queueTotal $coordinatorTotal) -}}
{{- $txnDetail = printf "transaction pooler pools %d x (1 + %d surge pod) = %d, queue-session pool %d x (1 + %d surge pod) = %d, coordinator-session pool %d x (1 + %d surge pod) = %d" $users $surge $txnTotal $queue $queueSurge $queueTotal $coordinator $coordinatorSurge $coordinatorTotal -}}
{{- end -}}
{{- $replicas := int64 (include "dev-health.budgetTerm" (dict "key" "authService.replicas" "value" .Values.authService.replicas "min" 0 "max" 100)) -}}
{{- $direct := mul (add $replicas (int64 (include "dev-health.authMaxSurge" .))) (int64 (include "dev-health.authMaxConns" .)) -}}
{{- $used := add $pools $direct -}}
{{- if gt $used (int64 $v) -}}{{- fail (printf "authService.database.connectionBudget: %d connections are needed but the budget is %d: (authService.replicas %d + %d surge) x authService.database.maxConns %d = %d direct, plus %d go-pgbouncer pool connections (%s). Lower a pool or add capacity before raising the budget" $used (int64 $v) $replicas (int64 (include "dev-health.authMaxSurge" .)) (int64 (include "dev-health.authMaxConns" .)) $direct $pools $txnDetail) -}}{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Why an OTLP egress ipBlock cidr is refused, or "" when it is acceptable. The prefix is parsed as a number: an IPv4
prefix must be /8 to /32 and an IPv6 prefix /32 to /128, so no spelling of 0.0.0.0/0 or ::/0 passes. An IPv6 cidr
written with an embedded dotted IPv4 part is refused; any other is expanded to its 8 groups and refused when it
overlaps the IPv4-mapped range ::ffff:0:0/96 or the NAT64 ranges 64:ff9b::/96 and 64:ff9b:1::/48 (inside them or
containing them), in any spelling. Call with the cidr string.
*/}}
{{- define "dev-health.otlpPeerCidrProblem" -}}
{{- $parts := splitList "/" (lower (trim (toString .))) -}}
{{- if ne (len $parts) 2 -}}
it needs exactly one "/" and a prefix
{{- else -}}
{{- $addr := index $parts 0 -}}
{{- $bits := index $parts 1 -}}
{{- if not (regexMatch "^[0-9]{1,3}$" $bits) -}}
the prefix is not a number
{{- else -}}
{{- $prefix := atoi $bits -}}
{{- if contains ":" $addr -}}
{{- if or (lt $prefix 32) (gt $prefix 128) -}}
an IPv6 prefix must be from /32 to /128
{{- else if contains "." $addr -}}
an IPv6 address with an embedded IPv4 part (the mapped or compatible range) is not a collector address
{{- else -}}
{{- $groups := include "dev-health.ipv6Groups" $addr | fromJsonArray -}}
{{- if ne (len $groups) 8 -}}
the IPv6 address does not parse
{{- else -}}
{{- $reason := "" -}}
{{- range $r := list (list (list 0 0 0 0 0 65535 0 0) 96 "the IPv4-mapped IPv6 range ::ffff:0:0/96") (list (list 100 65435 0 0 0 0 0 0) 96 "the NAT64 range 64:ff9b::/96") (list (list 100 65435 1 0 0 0 0 0) 48 "the NAT64 range 64:ff9b:1::/48") -}}
{{- if and (not $reason) (eq (include "dev-health.ipv6Overlaps" (dict "a" $groups "p" $prefix "b" (index $r 0) "q" (index $r 1))) "true") -}}
{{- $reason = printf "it overlaps %s, which is not a collector address" (index $r 2) -}}
{{- end -}}
{{- end -}}
{{- $reason -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- if or (lt $prefix 8) (gt $prefix 32) -}}
an IPv4 prefix must be from /8 to /32
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The 8 groups of a lower-case IPv6 address as a JSON list of numbers, "::" expanded; [] when it does not parse.
*/}}
{{- define "dev-health.ipv6Groups" -}}
{{- $hex := dict "0" 0 "1" 1 "2" 2 "3" 3 "4" 4 "5" 5 "6" 6 "7" 7 "8" 8 "9" 9 "a" 10 "b" 11 "c" 12 "d" 13 "e" 14 "f" 15 -}}
{{- $halves := splitList "::" . -}}
{{- $raw := list -}}
{{- if eq (len $halves) 1 -}}
{{- $raw = splitList ":" . -}}
{{- else if eq (len $halves) 2 -}}
{{- $left := list -}}
{{- if ne (index $halves 0) "" }}{{- $left = splitList ":" (index $halves 0) }}{{- end -}}
{{- $right := list -}}
{{- if ne (index $halves 1) "" }}{{- $right = splitList ":" (index $halves 1) }}{{- end -}}
{{- $fill := sub 8 (add (len $left) (len $right)) | int -}}
{{- if gt $fill 0 -}}
{{- $raw = $left -}}
{{- range until $fill }}{{- $raw = append $raw "0" }}{{- end -}}
{{- $raw = concat $raw $right -}}
{{- end -}}
{{- end -}}
{{- $out := list -}}
{{- $ok := eq (len $raw) 8 -}}
{{- range $g := $raw -}}
{{- if not (regexMatch "^[0-9a-f]{1,4}$" $g) -}}
{{- $ok = false -}}
{{- else -}}
{{- $v := 0 -}}
{{- range $c := splitList "" $g }}{{- $v = add (mul $v 16) (get $hex $c) }}{{- end -}}
{{- $out = append $out $v -}}
{{- end -}}
{{- end -}}
{{- if and $ok (eq (len $out) 8) -}}{{- toJson $out -}}{{- else -}}[]{{- end -}}
{{- end -}}

{{/*
"true" when the IPv6 prefixes a/p and b/q (8 groups each) overlap: they agree on their first min(p, q) bits.
*/}}
{{- define "dev-health.ipv6Overlaps" -}}
{{- $m := min (int .p) (int .q) -}}
{{- $full := div $m 16 | int -}}
{{- $rem := mod $m 16 | int -}}
{{- $same := true -}}
{{- range $i := until $full -}}
{{- if ne (int (index $.a $i)) (int (index $.b $i)) }}{{- $same = false }}{{- end -}}
{{- end -}}
{{- if and $same (gt $rem 0) -}}
{{- $shift := index (list 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536) (sub 16 $rem | int) -}}
{{- if ne (div (int (index .a $full)) $shift) (div (int (index .b $full)) $shift) }}{{- $same = false }}{{- end -}}
{{- end -}}
{{- if $same }}true{{- end -}}
{{- end -}}
