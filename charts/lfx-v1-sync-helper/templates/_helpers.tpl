{{/* Copyright The Linux Foundation and each contributor to LFX. */}}
{{/* SPDX-License-Identifier: MIT */}}

{{/*
Common labels for chart-managed resources. Used by the CloudNativePG
CRs (database.yaml) so operators can query for all resources belonging
to this release with a single kubectl -l selector. The chart's existing
Deployments predate this helper and continue to inline their labels for
compatibility with argocd's live diff — new templates should use this
helper.
*/}}
{{- define "lfx-v1-sync-helper.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.Version }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
CloudNativePG app-secret name. The CNPG operator auto-creates two
secrets per Cluster resource: `<clusterName>-app` and
`<clusterName>-superuser`. The service always uses the app secret (never
the superuser one), so the deployment references this helper instead of
templating `-app` in every valueFrom block.

Referenced from templates/app-deployment.yaml when database.mode is
"database" or "cluster+database". `required` guards against an empty
clusterName rendering an invalid `-app` Secret reference.
*/}}
{{- define "lfx-v1-sync-helper.cloudNativePGAppSecret" -}}
{{- $name := required (printf "database.cloudNativePG.clusterName is required when database.mode=%q (got empty)" .Values.database.mode) .Values.database.cloudNativePG.clusterName -}}
{{- printf "%s-app" $name }}
{{- end }}
