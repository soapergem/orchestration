{{/*
Shared partials. The four mock services differ only in name, port and a handful
of env vars, so everything structural lives here -- otherwise a change like
"support source packaging" means editing four near-identical templates and
getting one of them subtly wrong.
*/}}

{{/*
Resource name for a service.

`nameSuffix` exists because IN-CLUSTER callers address these by Service name:
Argo's DAG YAML and Flyte's task config carry `approval-service:8091`,
`callback-fetch-service:8090`, `shipping-service:8092` and `fixture-service:8099`
as LITERAL env values, matching the compose DNS names (RUNNING.md 7c). Rendering
them as bare `approval` / `callback-fetch` / `shipping` -- as this chart did
until 2026-08-12 -- silently breaks every in-cluster DAG while the AWS path keeps
working, because Lambdas reach the services through the public ingress instead.

fixture.name is already `fixture-service`, so suffixing is conditional.

Usage: {{ include "bakeoff.name" (dict "svc" .Values.approval "root" $) }}
*/}}
{{- define "bakeoff.name" -}}
{{- $n := .svc.name -}}
{{- $suffix := .root.Values.nameSuffix | default "" -}}
{{- if and $suffix (not (hasSuffix $suffix $n)) -}}
{{- printf "%s%s" $n $suffix -}}
{{- else -}}
{{- $n -}}
{{- end -}}
{{- end -}}

{{/*
Pod spec for a mock service, covering both packaging models.

  packaging: image   -- a pre-built image per service (the AWS path; images are
                        built and pushed to ECR by terraform/aws).
  packaging: source  -- a stock python image, deps pip-installed by an init
                        container into an emptyDir, and app.py mounted from a
                        <name>-src ConfigMap.

Source mode is not a curiosity: building arm64 images from an x86 host needs
qemu binfmt, which rootless podman cannot register (RUNNING.md 7b/9), so the
arm64 cluster has no practical route to per-service images. Keeping both modes in
ONE chart is what lets this be the single source of truth for every cluster.

Usage: {{ include "bakeoff.podSpec" (dict "svc" .Values.approval "root" $ "container" "approval" "env" $envList) }}
*/}}
{{- define "bakeoff.podSpec" -}}
{{- $root := .root -}}
{{- $svc := .svc -}}
{{- $src := printf "%s-src" (include "bakeoff.name" (dict "svc" $svc "root" $root)) -}}
{{- if eq $root.Values.packaging "source" }}
initContainers:
- name: deps
  image: {{ $root.Values.sourcePackaging.pythonImage }}
  command: ["pip", "install", "--no-cache-dir", "--target=/deps", "--quiet"]
  args: {{ $svc.deps | default $root.Values.sourcePackaging.defaultDeps | toJson }}
  volumeMounts:
  - {name: deps, mountPath: /deps}
  resources:
    requests: {cpu: 100m, memory: 128Mi}
{{- end }}
{{- if and (eq $root.Values.packaging "image") $root.Values.image.pullSecretName }}
imagePullSecrets:
- name: {{ $root.Values.image.pullSecretName }}
{{- end }}
containers:
- name: app
{{- if eq $root.Values.packaging "source" }}
  image: {{ $root.Values.sourcePackaging.pythonImage }}
  workingDir: /app
  command: ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "{{ $svc.port }}"]
{{- else }}
  image: {{ $svc.repository }}:{{ $svc.tag }}
  imagePullPolicy: Always
{{- end }}
  ports:
  - containerPort: {{ $svc.port }}
{{- /* Build the whole env list first, then emit the key only if non-empty --
       a bare `env:` with nothing under it is not valid in a pod spec. */}}
{{- $env := .env | default list -}}
{{- if eq $root.Values.packaging "source" }}
{{- $env = prepend $env (dict "name" "PYTHONPATH" "value" "/deps") -}}
{{- end }}
{{- if and $svc.needsAwsResume $root.Values.aws.resumeSecretName }}
{{- /* states:SendTaskSuccess, for resuming a suspended Step Functions
       execution. Optional: a cluster that never runs the Step Functions DAGs
       leaves aws.resumeSecretName empty and these are omitted, rather than the
       pod failing on a missing Secret. */}}
{{- $env = append $env (dict "name" "AWS_REGION" "value" $root.Values.aws.region) -}}
{{- $env = append $env (dict "name" "AWS_ACCESS_KEY_ID" "valueFrom" (dict "secretKeyRef" (dict "name" $root.Values.aws.resumeSecretName "key" "AWS_ACCESS_KEY_ID"))) -}}
{{- $env = append $env (dict "name" "AWS_SECRET_ACCESS_KEY" "valueFrom" (dict "secretKeyRef" (dict "name" $root.Values.aws.resumeSecretName "key" "AWS_SECRET_ACCESS_KEY"))) -}}
{{- end }}
{{- if and $svc.needsGcpResume $root.Values.gcp.resumeSecretName }}
{{- /* Google Workflows resume: an OAuth2 access token minted from this key. */}}
{{- $env = append $env (dict "name" "GOOGLE_APPLICATION_CREDENTIALS" "value" $root.Values.gcp.credentialsPath) -}}
{{- end }}
{{- if $env }}
  env:
{{ toYaml $env | indent 2 }}
{{- end }}
{{- if or (eq $root.Values.packaging "source") (and $svc.needsGcpResume $root.Values.gcp.resumeSecretName) }}
  volumeMounts:
{{- if eq $root.Values.packaging "source" }}
  - {name: src, mountPath: /app, readOnly: true}
  - {name: deps, mountPath: /deps, readOnly: true}
{{- end }}
{{- if and $svc.needsGcpResume $root.Values.gcp.resumeSecretName }}
  - name: gcp-creds
    mountPath: {{ dir $root.Values.gcp.credentialsPath }}
    readOnly: true
{{- end }}
{{- end }}
{{- /* Probe handler is per-service and mutually exclusive: specifying two is
       rejected outright ("may not specify more than 1 handler type"), and a
       server-side patch MERGES rather than replaces, so switching kinds on an
       existing Deployment fails unless the chart matches what is live.
       Default is TCP because most of these expose no /health, and shipping has
       no GET endpoint at all (only POST /shipments) -- probing a path 404s
       forever. fixture-service does have /health, hence healthPath. */}}
  readinessProbe:
{{- if $svc.healthPath }}
    httpGet: {path: {{ $svc.healthPath | quote }}, port: {{ $svc.port }}}
    initialDelaySeconds: {{ $svc.probeInitialDelaySeconds | default 5 }}
    periodSeconds: 5
    failureThreshold: {{ $svc.probeFailureThreshold | default 12 }}
{{- else }}
    tcpSocket: {port: {{ $svc.port }}}
    initialDelaySeconds: {{ $svc.probeInitialDelaySeconds | default 3 }}
    periodSeconds: 5
    failureThreshold: {{ $svc.probeFailureThreshold | default 6 }}
{{- end }}
  resources:
    requests: {cpu: 50m, memory: 128Mi}
    limits: {cpu: 250m, memory: 256Mi}
{{- $wantsGcp := and $svc.needsGcpResume $root.Values.gcp.resumeSecretName -}}
{{- $wantsSrc := eq $root.Values.packaging "source" -}}
{{- if or $wantsSrc $wantsGcp }}
volumes:
{{- if $wantsSrc }}
- name: deps
  emptyDir: {}
- name: src
  configMap: {name: {{ $src }}}
{{- end }}
{{- if $wantsGcp }}
- name: gcp-creds
  secret: {secretName: {{ $root.Values.gcp.resumeSecretName }}}
{{- end }}
{{- end }}
restartPolicy: Always
{{- end -}}

{{/*
Deployment + Service for one mock service.
Usage: {{ include "bakeoff.service" (dict "svc" .Values.approval "root" $ "env" $envList) }}
*/}}
{{- define "bakeoff.service" -}}
{{- $name := include "bakeoff.name" (dict "svc" .svc "root" .root) -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $name }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    app: {{ $name }}
spec:
  replicas: {{ .svc.replicas }}
  selector:
    matchLabels:
      app: {{ $name }}
  template:
    metadata:
      labels:
        app: {{ $name }}
    spec:
{{ include "bakeoff.podSpec" (dict "svc" .svc "root" .root "env" .env) | indent 6 }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    app: {{ $name }}
spec:
  ports:
  - name: http
    port: {{ .svc.port }}
    targetPort: {{ .svc.port }}
  selector:
    app: {{ $name }}
{{- end -}}
