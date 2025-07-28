
{{/*
Common labels (clean version)
*/}}
{{- define "mobilitydbc-cluster.labels" -}}
app.kubernetes.io/name: mobilitydbc
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: Helm
{{- end }}

{{/*
Selector labels
*/}}
{{- define "mobilitydbc-cluster.selectorLabels" -}}
app.kubernetes.io/name: mobilitydbc
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels as comma-separated key=value pairs
It should be kept in sync with "mobilitydbc-cluster.selectorLabels"
*/}}
{{- define "mobilitydbc-cluster.selectorLabelsCSV" -}}
app.kubernetes.io/name=mobilitydbc,app.kubernetes.io/instance={{ .Release.Name }}
{{- end }}
