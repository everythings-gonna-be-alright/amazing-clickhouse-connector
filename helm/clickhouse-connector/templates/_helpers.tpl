{{/*
Expand the name of the chart.
*/}}
{{- define "clickhouse-connector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "clickhouse-connector.fullname" -}}
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
{{- define "clickhouse-connector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "clickhouse-connector.labels" -}}
helm.sh/chart: {{ include "clickhouse-connector.chart" . }}
{{ include "clickhouse-connector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "clickhouse-connector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clickhouse-connector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Secret with connector credentials.
*/}}
{{- define "clickhouse-connector.secretName" -}}
{{- printf "%s-secrets" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Name of the Valkey Service, matching the naming of the valkey subchart.
*/}}
{{- define "clickhouse-connector.redisFullname" -}}
{{- if .Values.valkey.fullnameOverride }}
{{- .Values.valkey.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "valkey" .Values.valkey.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Valkey pod selector labels, matching the valkey subchart.
*/}}
{{- define "clickhouse-connector.redisSelectorLabels" -}}
app.kubernetes.io/name: {{ default "valkey" .Values.valkey.nameOverride | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Accepted x-auth-token values; authKeyPrevious keeps the old key valid during a rotation.
*/}}
{{- define "clickhouse-connector.authKeys" -}}
{{- $keys := list (required "config.authKey is required" .Values.config.authKey) }}
{{- with .Values.config.authKeyPrevious }}
{{- $keys = append $keys . }}
{{- end }}
{{- join "," $keys }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "clickhouse-connector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "clickhouse-connector.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
