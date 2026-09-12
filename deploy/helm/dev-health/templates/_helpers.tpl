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
{{- .Values.postgresql.credentials.database }}
{{- else }}
{{- required "goWorkers.pgbouncer.postgres.database is required for external PostgreSQL" .Values.goWorkers.pgbouncer.postgres.database }}
{{- end }}
{{- end }}

{{/*
Worker operational bridge URL — the in-cluster API Service that serves the
bridge the Go PagerDuty stream runner forwards reconciliation to. Auto-computed
so the chart renders a reachable endpoint; override config.WORKER_OPERATIONAL_-
BRIDGE_URL to point at an internal HTTPS origin instead.
*/}}
{{- define "dev-health.operationalBridgeURL" -}}
{{- printf "http://%s-api.%s.svc.cluster.local:%v" (include "dev-health.fullname" .) (include "dev-health.namespace" .) .Values.api.port }}
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
{{- $derivedKeys := list "WORKER_OPERATIONAL_BRIDGE_URL" "GO_API_QUERY_API_URL" }}
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
{{- if not (index .Values.config "WORKER_OPERATIONAL_BRIDGE_URL") }}
WORKER_OPERATIONAL_BRIDGE_URL: {{ include "dev-health.operationalBridgeURL" . | quote }}
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
Secret -- which the weight-0 migrate Job envFroms -- must not carry
MIGRATION_DATABASE_URI at all. `dev-hops migrate postgres` runs
dev-health-worker-migrate whenever that variable is SET
(migrate.py::_run_river_upgrade tests `is not None`, so an empty string is not
enough to suppress it), and at weight 0 that preflight runs before the weight-5
hook has created the roles it requires. Compose keeps the two DSNs in two
different variables for the same reason; here the same DSN reaches Alembic
under the POSTGRES_URI compatibility alias, which db.py::get_postgres_uri
normalises to the async driver form.
*/}}
{{- $riverHookOwnsRiver := .Values.migrations.hook.riverMigrate.enabled }}
{{- range $key, $value := .Values.migrations.hook.secretData }}
{{- if and $value (not (and $riverHookOwnsRiver (eq $key "MIGRATION_DATABASE_URI"))) }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if and (not (index .Values.migrations.hook.secretData "CLICKHOUSE_URI")) (index .Values.secrets.data "CLICKHOUSE_URI") }}
CLICKHOUSE_URI: {{ index .Values.secrets.data "CLICKHOUSE_URI" | quote }}
{{- else if and (not (index .Values.migrations.hook.secretData "CLICKHOUSE_URI")) .Values.clickhouse.enabled }}
CLICKHOUSE_URI: {{ include "dev-health.clickhouseURI" . | quote }}
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
