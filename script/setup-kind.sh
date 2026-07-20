#!/usr/bin/env bash

#
# Set up a kubernetes environment with kind.
#
# * Install Akash CRD
# * Install `akash-services` Namespace
# * Install Network Policies
# * Optionally install metrics-server

set -e

rootdir="$(dirname "$0")/.."

install_ns() {
    set -x
    kubectl apply -f "$rootdir/_docs/kustomize/networking/"
}

install_network_policies() {
    set -x
    kubectl kustomize "$rootdir/_docs/kustomize/akash-services/" | kubectl apply -f-
}

install_crd() {
    set -x
    kubectl apply -f "$rootdir/pkg/apis/akash.network/crd.yaml"
    kubectl apply -f "$rootdir/_docs/kustomize/storage/storageclass.yaml"
    kubectl patch node "${KIND_NAME}-control-plane" -p '{"metadata":{"labels":{"akash.network/storageclasses":"beta2.default.beta3"}}}'

    # AEP-87 two-provider e2e: the in-process replication file driver reads
    # and writes local-path PV host paths directly, so the provisioner's
    # storage directory must resolve to the SAME absolute path on the host
    # and inside the kind node. kind-config.yaml mounts the host dir at that
    # path; here the provisioner is repointed at it.
    if kubectl -n local-path-storage get configmap local-path-config >/dev/null 2>&1; then
        kubectl -n local-path-storage get configmap local-path-config -o yaml \
            | sed 's|/var/local-path-provisioner|/tmp/akash-e2e-storage|g' \
            | kubectl -n local-path-storage apply -f -
        kubectl -n local-path-storage rollout restart deployment local-path-provisioner
    fi
}

install_metrics() {
    set -x
    # https://github.com/kubernetes-sigs/kind/issues/398#issuecomment-621143252
    kubectl apply -f "$rootdir/_docs/kustomize/kind/kind-metrics-server.yaml"

    #  kubectl wait pod --namespace kube-system \
    #    --for=condition=ready \
    #    --selector=k8s-app=metrics-server \
    #    --timeout=90s

    echo "metrics initialized"
}

config_file() {
    if [ "$#" != "2" ]; then
        echo "invalid amount of args"
        usage
    fi

    _run_dir="$1"

    if [[ "$_run_dir" == */ ]]; then
        # shellcheck disable=SC2001
        _run_dir=$(echo "$_run_dir" | sed 's:/*$::')
    fi

    case "$2" in
        calico)
            file="$_run_dir/../kind-config-calico.yaml"
            ;;
        default)
            ;;&
        *)
            file="$_run_dir/kind-config.yaml"
            ;;
    esac

    file=$(realpath "$file")

    echo "$file"
    exit 0
}

usage() {
    cat <<EOF
Install k8s dependencies for integration tests against "KinD"

Usage: $0 [crd|ns|metrics]
  crd:            install the akash CRDs
  ns:             install akash namespace
  metrics:        install CRDs, NS, metrics-server and wait for metrics to be available
  calico-metrics: install CRDs, NS, Network Policies, metrics-server and wait for metrics to be available
  networking:     install essential k8s namespace and network policies for Akash services
  config-file:    print path to the config file depending on requested network type.
                  first argument - path to _run example, second argument - config name
                  allowed config names: default, calico
EOF
    exit 1
}

case "${1:-metrics}" in
    crd)
        install_crd
        ;;
    ns)
        install_ns
        ;;
    metrics)
#        install_crd
#        install_ns
        install_metrics
        ;;
    calico-metrics)
        install_crd
        install_ns
        install_metrics
        install_network_policies
        ;;
    networking)
        install_ns
        install_network_policies
        ;;
    config-file)
        shift
        config_file "$@"
        ;;
    *) usage
        ;;
esac
