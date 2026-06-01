{{- define "nodusdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "nodusdb.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "nodusdb.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "nodusdb.labels" -}}
app.kubernetes.io/name: {{ include "nodusdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "nodusdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nodusdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "nodusdb.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
