#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# Put the native box into (or out of) its measuring state: perf allowed for an
# unprivileged user, kernel symbols visible, the NMI watchdog off (it holds a
# hardware counter), and every core's frequency locked so runs compare.
#
#   scripts/perf-box.sh on [MHZ]   # default 3200: the lock earlier counter runs used
#   scripts/perf-box.sh off        # restore what `on` saved
#   scripts/perf-box.sh status
#
# `on` records the previous settings in ~/.cache/ibex/perf-box.state, so `off`
# puts back exactly what was there (a reboot also resets everything).
# Settings do not persist across reboots; rerun `on` after one.
#
# Locking is min = max = MHZ with turbo left enabled: on intel_pstate, turning
# turbo off would cap the P-cores at base clock (2.1 GHz on the i7-13700)
# rather than at MHZ. Pick a MHZ every core can hold all-core (the E-cores top
# out at 4.1 GHz).
#
# Core choice is the harness's, not this script's: one thread per P-core is
# `taskset -c 0,2,4,6,8,10,12,14` (CPUs 2k and 2k+1 are one core's two
# hyperthreads; 16-23 are E-cores).
set -euo pipefail

STATE="${XDG_CACHE_HOME:-$HOME/.cache}/ibex/perf-box.state"
CPUFREQ=/sys/devices/system/cpu/cpu*/cpufreq

sysctl_keys=(kernel.perf_event_paranoid kernel.kptr_restrict kernel.nmi_watchdog)

status() {
    for k in "${sysctl_keys[@]}"; do
        printf '%-28s %s\n' "$k" "$(sysctl -n "$k")"
    done
    printf '%-28s %s\n' "intel_pstate/no_turbo" \
        "$(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo n/a)"
    # One line per distinct (governor, epp, min, max) combination.
    for d in $(printf '%s\n' $CPUFREQ | sort -V); do
        cpu=${d%/cpufreq}
        cpu=${cpu##*/}
        printf '%s %s %s %s %s\n' "$(cat "$d/scaling_governor")" \
            "$(cat "$d/energy_performance_preference" 2>/dev/null || echo -)" \
            "$(($(cat "$d/scaling_min_freq") / 1000))" "$(($(cat "$d/scaling_max_freq") / 1000))" "$cpu"
    done | awk '{k=$1" epp="$2" min="$3"MHz max="$4"MHz"; c[k]=c[k] (c[k]?",":"") substr($5,4)}
                END{for (k in c) printf "%-28s %s\n", "cpus " c[k], k}'
}

on() {
    local mhz="${1:-3200}"
    if [[ -e "$STATE" ]]; then
        echo "perf-box: already on (state in $STATE); run 'off' first" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$STATE")"
    {
        for k in "${sysctl_keys[@]}"; do
            echo "sysctl $k $(sysctl -n "$k")"
        done
        for d in $CPUFREQ; do
            echo "cpufreq $d $(cat "$d/scaling_governor") $(cat "$d/scaling_min_freq")" \
                "$(cat "$d/scaling_max_freq")" \
                "$(cat "$d/energy_performance_preference" 2>/dev/null || echo -)"
        done
    } >"$STATE"

    sudo sysctl -q kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 kernel.nmi_watchdog=0
    sudo cpupower -c all frequency-set -g performance >/dev/null
    # Raise max before min, or a min above the current max is refused.
    for d in $CPUFREQ; do
        echo $((mhz * 1000)) | sudo tee "$d/scaling_max_freq" >/dev/null
        echo $((mhz * 1000)) | sudo tee "$d/scaling_min_freq" >/dev/null
    done
    echo "perf-box: on (${mhz} MHz locked; previous state saved to $STATE)"
    status
}

off() {
    if [[ ! -e "$STATE" ]]; then
        echo "perf-box: nothing saved ($STATE missing); is it on?" >&2
        exit 1
    fi
    while read -r kind a b c d e; do
        case "$kind" in
            sysctl) sudo sysctl -q "$a=$b" ;;
            cpufreq)
                echo "$b" | sudo tee "$a/scaling_governor" >/dev/null
                # Lower min before max, the reverse of `on`.
                echo "$c" | sudo tee "$a/scaling_min_freq" >/dev/null
                echo "$d" | sudo tee "$a/scaling_max_freq" >/dev/null
                if [[ "$e" != - ]]; then
                    echo "$e" | sudo tee "$a/energy_performance_preference" >/dev/null
                fi
                ;;
        esac
    done <"$STATE"
    rm -f "$STATE"
    echo "perf-box: off (restored)"
    status
}

case "${1:-status}" in
    on) on "${2:-}" ;;
    off) off ;;
    status) status ;;
    *)
        echo "usage: $0 on [MHZ] | off | status" >&2
        exit 2
        ;;
esac
