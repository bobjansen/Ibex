#!/usr/bin/env bash
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
apt-get update -qq
apt-get install -y ca-certificates git
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
