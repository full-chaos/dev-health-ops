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
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/*
Web image
*/}}
{{- define "dev-health.webImage" -}}
{{- printf "%s:%s" .Values.webImage.repository (default .Chart.AppVersion .Values.webImage.tag) }}
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
Redis URL — auto-computed when valkey.enabled, otherwise from secrets
*/}}
{{- define "dev-health.redisURL" -}}
{{- if .Values.valkey.enabled }}
{{- printf "redis://%s-valkey:6379/0" (include "dev-health.fullname" .) }}
{{- else }}
{{- .Values.config.CELERY_BROKER_URL | default "" }}
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
PostgreSQL URI — auto-computed when postgresql.enabled
*/}}
{{- define "dev-health.postgresURI" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "postgresql+asyncpg://%s:%s@%s-postgresql:5432/%s" .Values.postgresql.credentials.user .Values.postgresql.credentials.password (include "dev-health.fullname" .) .Values.postgresql.credentials.database }}
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
{{- $redisAuto := and .Values.valkey.enabled (not .Values.config.CELERY_BROKER_URL) }}
{{- $redisKeys := list "CELERY_BROKER_URL" "CELERY_RESULT_BACKEND" "REDIS_URL" }}
{{- range $key, $value := .Values.config }}
{{- if or $value (not (and $redisAuto (has $key $redisKeys))) }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if $redisAuto }}
CELERY_BROKER_URL: {{ include "dev-health.redisURL" . | quote }}
CELERY_RESULT_BACKEND: {{ include "dev-health.redisURL" . | quote }}
REDIS_URL: {{ include "dev-health.redisURL" . | quote }}
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
{{- if and .Values.postgresql.enabled (not (index .Values.secrets.data "DATABASE_URI")) }}
DATABASE_URI: {{ include "dev-health.postgresURI" . | quote }}
{{- end }}
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
