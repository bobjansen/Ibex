#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# lib.sh — shared helpers for the ibex AWS benchmark runners.
#
# Sourced by run.sh, run-per-engine.sh and build-ami.sh. Centralises the bits
# they all need: loading .config, finding the repo commit, the "did you push?"
# preflight, AMI resolution (prefer a baked AMI from build-ami.sh, fall back to
# the stock Ubuntu image) and building the EC2 user-data script.
#
# Every function prints its result on stdout (and diagnostics on stderr) so the
# callers can capture ids with $(...). Nothing here launches instances.

# ── Config ─────────────────────────────────────────────────────────────────
# Source benchmarking/aws/.config (written by setup.sh / build-ami.sh) so
# S3_BUCKET, AWS_REGION and IBEX_AMI become available. Safe to call repeatedly.
bench_load_config() {
    local script_dir="$1"
    local config_file="$script_dir/.config"
    [[ -f "$config_file" ]] && source "$config_file"
    return 0
}

# Persist a KEY=VALUE pair into .config, replacing any existing line for KEY.
bench_save_config() {
    local script_dir="$1" key="$2" value="$3"
    local config_file="$script_dir/.config"
    touch "$config_file"
    if grep -q "^${key}=" "$config_file" 2>/dev/null; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$config_file"
    else
        printf '%s=%s\n' "$key" "$value" >> "$config_file"
    fi
}

# ── Git ────────────────────────────────────────────────────────────────────
# Echo the https form of the origin remote (the instance clones over https).
bench_repo_url() {
    local repo_root="$1"
    git -C "$repo_root" remote get-url origin 2>/dev/null \
        | sed -e 's|git@github.com:|https://github.com/|' -e 's|\.git$||' \
        | sed 's|$|.git|'
}

# Fail loudly if HEAD is not reachable on the branch's upstream — the instance
# clones origin and checks out the exact commit, so a local-only commit would
# make it terminate during user-data. Mirrors the original run.sh preflight.
bench_require_pushed() {
    local repo_root="$1" commit="$2" branch="$3" repo_url="$4"
    local upstream upstream_remote
    upstream=$(git -C "$repo_root" rev-parse --abbrev-ref --symbolic-full-name "@{u}" 2>/dev/null || true)
    if [[ -z "$upstream" ]]; then
        echo "WARNING: branch '$branch' has no upstream; cannot verify ${commit:0:8} is cloneable." >&2
        return 0
    fi
    upstream_remote=$(git -C "$repo_root" config "branch.${branch}.remote" 2>/dev/null || true)
    [[ -n "$upstream_remote" ]] && git -C "$repo_root" fetch --quiet "$upstream_remote" 2>/dev/null || true
    if ! git -C "$repo_root" merge-base --is-ancestor "$commit" "$upstream"; then
        echo "ERROR: commit ${commit:0:8} is not on upstream ${upstream}." >&2
        echo "The AWS runner clones ${repo_url} and checks out the exact local HEAD." >&2
        echo "Push this commit first, then rerun:" >&2
        echo "  git push ${upstream_remote:-origin} ${branch}" >&2
        return 1
    fi
}

# ── AMI resolution ───────────────────────────────────────────────────────────
# Latest stock Ubuntu 24.04 (Noble) amd64 server image. Used to BUILD the AMI
# and as the fallback when no baked AMI is configured.
bench_base_ubuntu_ami() {
    local region="$1"
    aws ec2 describe-images \
        --region "$region" \
        --owners 099720109477 \
        --filters \
            "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
            "Name=state,Values=available" \
        --query "sort_by(Images, &CreationDate)[-1].ImageId" \
        --output text
}

# Resolve the AMI a benchmark run should boot. Prefers the baked AMI ($IBEX_AMI,
# normally set in .config by build-ami.sh) when it still exists and is available;
# otherwise falls back to the stock Ubuntu image so runs work without an AMI.
bench_resolve_ami() {
    local region="$1"
    if [[ -n "${IBEX_AMI:-}" && "${IBEX_AMI}" != "none" ]]; then
        local state
        state=$(aws ec2 describe-images --region "$region" --image-ids "$IBEX_AMI" \
            --query "Images[0].State" --output text 2>/dev/null || true)
        if [[ "$state" == "available" ]]; then
            echo "Using baked AMI $IBEX_AMI" >&2
            echo "$IBEX_AMI"
            return 0
        fi
        echo "WARNING: baked AMI ${IBEX_AMI} not available (state=${state:-missing}); using stock Ubuntu." >&2
    fi
    echo "Using stock Ubuntu image (no baked AMI configured)." >&2
    bench_base_ubuntu_ami "$region"
}

# ── Instance topology ────────────────────────────────────────────────────────
# EC2 advertises vCPUs, and on every current x86 instance family a vCPU is a
# HYPERTHREAD: r7i.2xlarge is 8 vCPU but only 4 physical cores. Reading "8 vCPU"
# as "8 cores" understates the per-core result and overstates the thread-scaling
# headroom, so every runner prints the real topology before it launches.
#
# Echoes "<vcpus> <cores> <threads_per_core> <memory_mib>"; all four are "?" if
# the describe call fails (no permission, unknown type) — a runner must not die
# because it could not print a nicety.
bench_instance_topology() {
    local region="$1" instance_type="$2" out
    out=$(aws ec2 describe-instance-types \
        --region "$region" \
        --instance-types "$instance_type" \
        --query 'InstanceTypes[0].[VCpuInfo.DefaultVCpus,VCpuInfo.DefaultCores,VCpuInfo.DefaultThreadsPerCore,MemoryInfo.SizeInMiB]' \
        --output text 2>/dev/null) || out=""
    if [[ -z "$out" || "$out" == *None* ]]; then
        echo "? ? ? ?"
        return 0
    fi
    echo "$out"
}

# One human line for the launch banner, e.g.
#   r7i.4xlarge: 16 vCPU = 8 physical cores x 2 threads/core, 128 GiB
bench_topology_line() {
    local region="$1" instance_type="$2"
    local vcpus cores tpc mem
    read -r vcpus cores tpc mem <<< "$(bench_instance_topology "$region" "$instance_type")"
    if [[ "$vcpus" == "?" ]]; then
        echo "${instance_type}: topology unknown (describe-instance-types unavailable)"
        return 0
    fi
    printf '%s: %s vCPU = %s physical cores x %s threads/core, %s GiB\n' \
        "$instance_type" "$vcpus" "$cores" "$tpc" "$((mem / 1024))"
}

# Emit the --cpu-options argument array for run-instances into the named array.
# ThreadsPerCore=1 disables SMT, which is how you measure PHYSICAL-core scaling
# without a sibling thread quietly doing half the work. Empty when unset, so the
# instance keeps its default (SMT on).
#   bench_cpu_options_args CPU_ARGS "$region" "$type" "$threads_per_core"
bench_cpu_options_args() {
    local -n _out="$1"
    local region="$2" instance_type="$3" tpc="$4"
    _out=()
    [[ -z "$tpc" ]] && return 0
    local vcpus cores default_tpc mem
    read -r vcpus cores default_tpc mem <<< "$(bench_instance_topology "$region" "$instance_type")"
    if [[ "$cores" == "?" ]]; then
        echo "WARNING: cannot resolve core count for ${instance_type}; ignoring --threads-per-core." >&2
        return 0
    fi
    _out=(--cpu-options "CoreCount=${cores},ThreadsPerCore=${tpc}")
}

# ── Provenance manifest ──────────────────────────────────────────────────────
# Written next to every result artifact and uploaded alongside it. A benchmark
# page whose numbers cannot be traced to a commit and a box is a screenshot, not
# a measurement — docs/window-ohlc.html carried a bare date for months and no
# one could say afterwards what produced it.
#
#   bench_write_manifest <path> <suite> <commit> <branch> <region> <type> [K=V ...]
# Extra KEY=VALUE pairs become additional string fields.
bench_write_manifest() {
    local path="$1" suite="$2" commit="$3" branch="$4" region="$5" instance_type="$6"
    shift 6
    local vcpus cores tpc mem
    read -r vcpus cores tpc mem <<< "$(bench_instance_topology "$region" "$instance_type")"
    mkdir -p "$(dirname "$path")"
    {
        printf '{\n'
        printf '  "suite": "%s",\n' "$suite"
        printf '  "commit": "%s",\n' "$commit"
        printf '  "branch": "%s",\n' "$branch"
        printf '  "generated_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '  "region": "%s",\n' "$region"
        printf '  "instance_type": "%s",\n' "$instance_type"
        printf '  "vcpus": "%s",\n' "$vcpus"
        printf '  "physical_cores": "%s",\n' "$cores"
        printf '  "threads_per_core": "%s",\n' "$tpc"
        printf '  "memory_gib": "%s"' "$([[ "$mem" == "?" ]] && echo "?" || echo "$((mem / 1024))")"
        local kv
        for kv in "$@"; do
            printf ',\n  "%s": "%s"' "${kv%%=*}" "${kv#*=}"
        done
        printf '\n}\n'
    } > "$path"
}

bench_security_group() {
    local region="$1"
    aws ec2 describe-security-groups \
        --region "$region" \
        --filters "Name=group-name,Values=ibex-bench" \
        --query "SecurityGroups[0].GroupId" \
        --output text
}

# ── User-data ────────────────────────────────────────────────────────────────
# Emit the EC2 user-data script. Args: <repo_url> <commit> [KEY=VALUE ...]; the
# KEY=VALUE pairs are exported (shell-quoted) before bootstrap.sh runs.
#
# The repo step is clone-or-update so this works on BOTH a stock Ubuntu image
# (clones /ibex fresh) and a baked AMI (reuses the pre-cloned /ibex, keeping its
# warmed build-release so the per-run rebuild is incremental). build-release is
# gitignored, so the checkout never disturbs the cached Arrow build.
bench_user_data() {
    local repo_url="$1" commit="$2"; shift 2
    local exports="" kv key value
    for kv in "$@"; do
        key="${kv%%=*}"
        value="${kv#*=}"
        # $(...) strips trailing newlines, so append the newline outside it.
        exports+="$(printf 'export %s=%q' "$key" "$value")"$'\n'
    done
    cat <<EOF
#!/bin/bash
# Stream to /var/log/ibex-bench.log AND the EC2 serial console so progress is
# visible via \`aws ec2 get-console-output\` even without SSH.
if [[ -w /dev/ttyS0 ]]; then
    exec > >(stdbuf -oL tee -a /var/log/ibex-bench.log /dev/ttyS0) 2>&1
else
    exec > >(stdbuf -oL tee -a /var/log/ibex-bench.log /dev/console) 2>&1
fi
set -Eeuo pipefail
set -x
userdata_exit() {
    code=\$?
    echo "user-data exited with status \${code}"
    if [[ "\${code}" -ne 0 ]]; then
        shutdown -h now
    fi
}
trap userdata_exit EXIT
# Retry apt-get on transient dpkg/apt lock contention -- e.g. unattended-upgrades
# holding /var/lib/dpkg/lock-frontend in the first seconds after boot, which is
# exactly what killed the 2026-07-18 R run here: apt-get exited 100 before the
# repo was even cloned, set -Eeuo pipefail propagated it, and userdata_exit
# self-terminated the instance.
apt_get_retry() {
    attempt=1; max_attempts=36; delay=5
    while ! apt-get "\$@"; do
        if (( attempt >= max_attempts )); then
            echo "apt-get \$* failed after \${max_attempts} attempts" >&2
            return 1
        fi
        echo "apt-get \$* failed (attempt \${attempt}/\${max_attempts}, likely a dpkg/apt lock) -- retrying in \${delay}s" >&2
        sleep "\$delay"
        attempt=\$((attempt + 1))
    done
}
apt_get_retry update -qq
apt_get_retry install -y ca-certificates git
if [[ ! -d /ibex/.git ]]; then
    git clone "${repo_url}" /ibex
fi
cd /ibex
git fetch --all --tags --quiet || true
git checkout --force "${commit}" || {
    echo "FATAL: commit ${commit} not found on origin — did you 'git push'?"
    shutdown -h now
    exit 1
}
${exports}bash /ibex/benchmarking/aws/bootstrap.sh
EOF
}
