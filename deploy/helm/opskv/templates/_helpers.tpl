{{- define "opskv.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "opskv.fullname" -}}
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

{{- define "opskv.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name }}
{{- end }}

{{- define "opskv.labels" -}}
app.kubernetes.io/name: {{ include "opskv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{/* Meta DNS list for gateway: meta-0.meta-headless.<ns>.svc.<domain>:port */}}
{{- define "opskv.metaAddrs" -}}
{{- $ns := include "opskv.namespace" . -}}
{{- $port := .Values.ports.meta -}}
{{- $n := int .Values.replicaCount.meta -}}
{{- $domain := .Values.cluster.domain -}}
{{- $parts := list -}}
{{- range $i, $_ := until $n -}}
{{- $parts = append $parts (printf "meta-%d.meta-headless.%s.svc.%s:%d" $i $ns $domain (int $port)) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end }}

{{- define "opskv.storageAddrs" -}}
{{- $ns := include "opskv.namespace" . -}}
{{- $port := .Values.ports.storage -}}
{{- $n := int .Values.replicaCount.storage -}}
{{- $domain := .Values.cluster.domain -}}
{{- $parts := list -}}
{{- range $i, $_ := until $n -}}
{{- $parts = append $parts (printf "storage-%d.storage-headless.%s.svc.%s:%d" $i $ns $domain (int $port)) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end }}
