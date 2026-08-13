#!/usr/bin/env bash
#
# Submit one of the bake-off DAGs from its registered WorkflowTemplate.
#
# Why this exists: the four DAGs used to be `kind: Workflow` with a
# `generateName:`, submitted with `kubectl create -f argo/dagN-*.yaml`. That
# works, but it means the engine never learns the definition -- the Workflow
# Templates tab showed only DAG 4's four sub-workflow templates, and "what is
# deployed?" had no answer in the UI. They are now WorkflowTemplates
# (2026-08-12), registered with `kubectl apply`, which needs a submit path.
#
# The upstream way is `argo submit --from workflowtemplate/NAME`, but that needs
# the `argo` CLI. Everything else in RUNNING.md 8 is kubectl-only, so this
# generates the equivalent stub Workflow instead.
#
# Two behaviours worth knowing, both verified 2026-08-12:
#
#   * A template's `spec.arguments` defaults DO resolve for a stub that passes
#     nothing -- `{{workflow.parameters.input}}` etc. come out right.
#   * ...but they are NOT copied onto the submitted Workflow object. `kubectl get
#     wf -o yaml` shows an EMPTY `spec.arguments` unless you overrode something,
#     so the run does not record the parameters it actually used. Pass -p
#     explicitly when a run's inputs need to be self-evident after the fact.
#
# Note this is the OPPOSITE of the templateRef trap in RUNNING.md 8: there, a
# sub-workflow invoked from inside DAG 4 resolves {{workflow.parameters.*}}
# against the CALLER and ignores its own defaults. Same-looking syntax, inverted
# rule, depending on whether the reference is at spec level or task level.
#
# Usage:
#   KCTX=my-arm64-cluster ./submit.sh payment-processing
#   KCTX=my-arm64-cluster ./submit.sh payment-processing -p bakeoff-ns=argo
#   KCTX=my-arm64-cluster ./submit.sh csv-etl-pipeline -p input='{"a":1}'
#
# Templates: csv-etl-pipeline | api-fanout | payment-processing | order-fulfillment
#
# Env:
#   KCTX      kube context (required)
#   ARGO_NS   namespace running argo (default: argo)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context (e.g. KCTX=my-arm64-cluster)}"
ARGO_NS="${ARGO_NS:-argo}"

TEMPLATE="${1:?usage: submit.sh <template-name> [-p name=value ...]}"
shift

# ---- collect -p name=value pairs ------------------------------------------
PARAMS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p)
      [[ $# -ge 2 ]] || { echo "-p needs name=value" >&2; exit 2; }
      name="${2%%=*}"; value="${2#*=}"
      [[ "$name" != "$2" ]] || { echo "-p expects name=value, got '$2'" >&2; exit 2; }
      # Block-scalar the value so JSON, quotes and newlines survive untouched.
      # `|-` not `|`: the clip indicator keeps a trailing newline, and a scalar
      # parameter is then used verbatim in string building. `-p bakeoff-ns=argo`
      # became "argo\n", so every step looked for schema "argo\n_dag3" and DAG 3
      # died in validate-payment. JSON parameters tolerate it; scalars do not.
      PARAMS+=$'\n      - name: '"$name"$'\n        value: |-\n'"$(printf '%s\n' "$value" | sed 's/^/          /')"
      shift 2 ;;
    *) echo "unexpected argument: $1" >&2; exit 2 ;;
  esac
done

if ! kubectl --context "$KCTX" -n "$ARGO_NS" get workflowtemplate "$TEMPLATE" >/dev/null 2>&1; then
  echo "ERROR: no WorkflowTemplate '$TEMPLATE' in namespace $ARGO_NS." >&2
  echo "Register them first:" >&2
  echo "  kubectl --context $KCTX apply -n $ARGO_NS -f argo/templates/ -f argo/dag1-csv-etl.yaml \\" >&2
  echo "    -f argo/dag2-api-fanout.yaml -f argo/dag3-payment.yaml -f argo/dag4-order-fulfillment.yaml" >&2
  echo "Available:" >&2
  kubectl --context "$KCTX" -n "$ARGO_NS" get workflowtemplate -o name >&2
  exit 1
fi

STUB="apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: $TEMPLATE-
  labels:
    workflow: $TEMPLATE
spec:
  workflowTemplateRef:
    name: $TEMPLATE"
[[ -n "$PARAMS" ]] && STUB+="
  arguments:
    parameters:$PARAMS"

NAME=$(printf '%s\n' "$STUB" \
  | kubectl --context "$KCTX" create -n "$ARGO_NS" -f - -o name)

echo "==> submitted $NAME from workflowtemplate/$TEMPLATE"
echo "    kubectl --context $KCTX -n $ARGO_NS get ${NAME} -w"
echo "    kubectl --context $KCTX -n $ARGO_NS logs ${NAME} --follow"
