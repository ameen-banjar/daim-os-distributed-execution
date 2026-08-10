#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  build-essential clang make pkg-config \
  openvswitch-switch mininet iperf3 \
  iproute2 iputils-ping net-tools ethtool tcpdump \
  python3 python3-pip python3-setuptools \
  jq sysstat procps git ca-certificates

# The controller package is installed separately from the OS packages so the
# exact version and installation log are recorded.
python3 -m pip install --break-system-packages -r "$(dirname "$0")/controller_requirements.txt"

sudo systemctl enable --now openvswitch-switch

printf 'ubuntu_release='; . /etc/os-release; printf '%s\n' "$PRETTY_NAME"
printf 'kernel='; uname -r
printf 'architecture='; uname -m
printf 'ovs='; ovs-vsctl --version | head -1
printf 'mininet='; mn --version
printf 'iperf3='; iperf3 --version | head -1
printf 'clang='; clang --version | head -1
printf 'gcc='; gcc --version | head -1
