#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# Install an ADBC driver so `adbc_connect("<name>", ...)` finds it by name.
#
#   scripts/install_adbc_driver.sh [--dest DIR] sqlite|postgresql|duckdb|mysql ...
#
# sqlite and postgresql are Apache Arrow ADBC's own builds, taken from their
# PyPI wheels (a wheel is a zip; the C driver inside needs only
# libc/libstdc++, and Python is not involved). duckdb (DuckDB Foundation, MIT)
# and mysql (ADBC Driver Foundry, Apache-2.0) are the tarballs of Columnar's
# public driver registry, the ones `dbc install` fetches; dbc is not needed.
# Every download is pinned by version and SHA-256 below and verified before
# anything is unpacked. Needs curl (or wget), unzip, tar, and sha256sum (or
# shasum).
#
# Each driver goes to DIR/<name>/ with a manifest DIR/<name>.toml. DIR is, in
# order: --dest, the first entry of $ADBC_DRIVER_PATH, or
# ${XDG_CONFIG_HOME:-~/.config}/adbc/drivers -- all places the ADBC driver
# manager searches. A --dest elsewhere needs ADBC_DRIVER_PATH=DIR at run time.
#
# To bump a wheel driver: change ADBC_VERSION and every hash in wheel_for(),
# taking them from https://pypi.org/pypi/adbc-driver-<name>/<version>/json.
# To bump a registry driver: change its version in registry_driver() and every
# hash in tarball_for(). The registry index
# (https://dbc-cdn.columnar.tech/index.yaml) lists versions but no checksums:
# download each tarball once and record its SHA-256.
set -euo pipefail

ADBC_VERSION="1.12.0"

# wheel_for NAME PLATFORM -> "FILENAME SHA256 URL", or nothing if unsupported.
wheel_for() {
    case "$1/$2" in
    sqlite/linux_amd64)
        echo "adbc_driver_sqlite-1.12.0-py3-none-manylinux_2_28_x86_64.whl" \
            "3005a80bedf6624c6856da98037ea943a791aa8e82dad458259e0558be32912c" \
            "https://files.pythonhosted.org/packages/69/10/a3156f19fadd254a4f58a328a8aa9472c981ff93bb4d23f3c22a4341796e/adbc_driver_sqlite-1.12.0-py3-none-manylinux_2_28_x86_64.whl" ;;
    sqlite/linux_arm64)
        echo "adbc_driver_sqlite-1.12.0-py3-none-manylinux_2_28_aarch64.whl" \
            "c987d03e3f4850e57f218c8a0b9d224209123af642469ee1f36901c5a51725bd" \
            "https://files.pythonhosted.org/packages/6c/99/415bf90eb912403d2d5d0c31baa1cedf200bd510f40027ee8fd3421c4c02/adbc_driver_sqlite-1.12.0-py3-none-manylinux_2_28_aarch64.whl" ;;
    sqlite/macos_arm64)
        echo "adbc_driver_sqlite-1.12.0-py3-none-macosx_11_0_arm64.whl" \
            "5a81f53791e4aec69afbf8f77dac6acf48749fd84684e86601eafdd36d2eb7c3" \
            "https://files.pythonhosted.org/packages/e6/31/5d1d637e6ae76fcc57d5116d537aa78d2ab687354d5a0b2d527e085b61d4/adbc_driver_sqlite-1.12.0-py3-none-macosx_11_0_arm64.whl" ;;
    sqlite/macos_amd64)
        echo "adbc_driver_sqlite-1.12.0-py3-none-macosx_10_15_x86_64.whl" \
            "2d5b3e9d0b5dbc66324b0ccf2ded886e3781f901be986892d319529b05536d3b" \
            "https://files.pythonhosted.org/packages/a1/f7/c35740269d3a5e3aa07b9ab155d4e943a7f5267f64d8f7396d5e14184a02/adbc_driver_sqlite-1.12.0-py3-none-macosx_10_15_x86_64.whl" ;;
    postgresql/linux_amd64)
        echo "adbc_driver_postgresql-1.12.0-py3-none-manylinux_2_26_x86_64.manylinux_2_28_x86_64.whl" \
            "2c2dc9c29db07ba3e0caf293c57a7ab1259dd772d3725ff1f1aeedb7a1895dd4" \
            "https://files.pythonhosted.org/packages/00/bb/ee19e7d56824c05892f82a3a2abca94fd2345b7de3dc22baac9e65a46acc/adbc_driver_postgresql-1.12.0-py3-none-manylinux_2_26_x86_64.manylinux_2_28_x86_64.whl" ;;
    postgresql/linux_arm64)
        echo "adbc_driver_postgresql-1.12.0-py3-none-manylinux_2_26_aarch64.manylinux_2_28_aarch64.whl" \
            "b523f15051b27eef18c3a822296c2d94b894be552a0dbe49fe14059e2c706155" \
            "https://files.pythonhosted.org/packages/93/60/3b018e75661ac14a7aab7bb5cc1a95d72ddb9684abaee1e88a685406df81/adbc_driver_postgresql-1.12.0-py3-none-manylinux_2_26_aarch64.manylinux_2_28_aarch64.whl" ;;
    postgresql/macos_arm64)
        echo "adbc_driver_postgresql-1.12.0-py3-none-macosx_11_0_arm64.whl" \
            "03c617aee8796f38a0a2f1af50ceae92d40f0974f3abbe7eefbaf009fecdc5ce" \
            "https://files.pythonhosted.org/packages/2f/d3/f17e69423ed7217b70155d8e531c5cf6fb74f3b598a583e6cfe541dc3e7e/adbc_driver_postgresql-1.12.0-py3-none-macosx_11_0_arm64.whl" ;;
    postgresql/macos_amd64)
        echo "adbc_driver_postgresql-1.12.0-py3-none-macosx_10_15_x86_64.whl" \
            "28548d9e16497d2cb4750bc8e9e1abad3d0f981c7c0ff7afe70323f4b71c70aa" \
            "https://files.pythonhosted.org/packages/7d/ba/152bbe1d4a1cc13e2da72e76a5045ee25338bc75294f1ebf04c90b787287/adbc_driver_postgresql-1.12.0-py3-none-macosx_10_15_x86_64.whl" ;;
    esac
}

REGISTRY="https://dbc-cdn.columnar.tech"

# registry_driver NAME -> "VERSION LICENSE ENTRYPOINT PUBLISHER...", or nothing.
# ENTRYPOINT is "-" for the default (AdbcDriverInit).
registry_driver() {
    case "$1" in
    duckdb) echo "v1.5.6 MIT duckdb_adbc_init DuckDB Foundation" ;;
    mysql) echo "v0.6.1 Apache-2.0 - ADBC Driver Foundry" ;;
    esac
}

# tarball_for NAME PLATFORM -> "SHA256 LIBRARY", or nothing if unsupported.
tarball_for() {
    case "$1/$2" in
    duckdb/linux_amd64) echo "bef6e5e5ea2f5a1f0b2a29915a1a614c97d28392590275ed99d67a4bbf4bdd27 libduckdb.so" ;;
    duckdb/linux_arm64) echo "513478625a429b6fabdb4e5d4b807e0b0c5d924360ba49810f02a49bf7bc2e86 libduckdb.so" ;;
    duckdb/macos_amd64) echo "2e7acaf724f1347defbc3ef3cf608ee708e64419a9da01334906fad4c9cb95f8 libduckdb.dylib" ;;
    duckdb/macos_arm64) echo "7544265abbc4615228b83771360c107cdadf12f1d64c0a914f778d2ed66d3c52 libduckdb.dylib" ;;
    mysql/linux_amd64) echo "ae0dbc60fde99528d1823087e7d952177a2841fddc4760634502ad9483895539 libadbc_driver_mysql.so" ;;
    mysql/linux_arm64) echo "bc47b508fbb19df7338624bfca2d1743df01403bf67116bf25f2eba86911456e libadbc_driver_mysql.so" ;;
    mysql/macos_arm64) echo "b9c3b97b7c3274470077666039e15883d9ecaeb4252be00c8e2ddf464df4ae58 libadbc_driver_mysql.dylib" ;;
    esac
}

KNOWN="sqlite, postgresql, duckdb, mysql"

die() { echo "install_adbc_driver: $*" >&2; exit 1; }

usage() {
    sed -n '5,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

platform() {
    local os arch
    case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=macos ;;
    *) die "unsupported OS $(uname -s); install a driver by hand and pass its path" ;;
    esac
    case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) die "unsupported CPU $(uname -m)" ;;
    esac
    echo "${os}_${arch}"
}

fetch() {  # URL OUT
    if command -v curl >/dev/null; then
        curl -fsSL --retry 3 -o "$2" "$1"
    elif command -v wget >/dev/null; then
        wget -q -O "$2" "$1"
    else
        die "need curl or wget"
    fi
}

sha256_of() {
    if command -v sha256sum >/dev/null; then
        sha256sum "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null; then
        shasum -a 256 "$1" | cut -d' ' -f1
    else
        die "need sha256sum or shasum"
    fi
}

dest=""
drivers=()
while [[ $# -gt 0 ]]; do
    case "$1" in
    --dest) [[ $# -ge 2 ]] || usage 2; dest="$2"; shift 2 ;;
    --dest=*) dest="${1#--dest=}"; shift ;;
    -h | --help) usage 0 ;;
    -*) usage 2 ;;
    *) drivers+=("$1"); shift ;;
    esac
done
[[ ${#drivers[@]} -gt 0 ]] || usage 2

if [[ -z "$dest" ]]; then
    if [[ -n "${ADBC_DRIVER_PATH:-}" ]]; then
        dest="${ADBC_DRIVER_PATH%%:*}"
    else
        dest="${XDG_CONFIG_HOME:-$HOME/.config}/adbc/drivers"
    fi
fi
mkdir -p "$dest"
dest="$(cd "$dest" && pwd)"

plat="$(platform)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# write_manifest NAME VERSION PUBLISHER LICENSE URL LIBRARY ENTRYPOINT
write_manifest() {
    {
        echo "manifest_version = 1"
        echo
        echo "name = 'ADBC $1 driver'"
        echo "version = '$2'"
        echo "publisher = '$3'"
        echo "license = '$4'"
        echo "url = '$5'"
        if [[ "$7" != "-" ]]; then
            echo
            echo "[Driver]"
            echo "entrypoint = '$7'"
        fi
        echo
        echo "[Driver.shared]"
        echo "${plat} = '${dest}/$1/$6'"
    } >"$dest/$1.toml"
    echo "Installed $dest/$1/$6"
    echo "  manifest $dest/$1.toml -> adbc_connect(\"$1\", ...)"
}

for name in "${drivers[@]}"; do
    if spec="$(wheel_for "$name" "$plat")" && [[ -n "$spec" ]]; then
        command -v unzip >/dev/null || die "need unzip"
        read -r file sha url <<<"$spec"
        echo "Downloading $file"
        fetch "$url" "$work/$file"
        actual="$(sha256_of "$work/$file")"
        [[ "$actual" == "$sha" ]] || die "$file: SHA-256 mismatch (expected $sha, got $actual)"

        lib="libadbc_driver_${name}.so"
        unzip -q -o -j "$work/$file" "adbc_driver_${name}/$lib" -d "$work/$name"
        mkdir -p "$dest/$name"
        install -m 0755 "$work/$name/$lib" "$dest/$name/$lib"
        write_manifest "$name" "$ADBC_VERSION" \
            "Apache Arrow ADBC (PyPI wheel ${file}, sha256 ${sha})" \
            Apache-2.0 https://arrow.apache.org/adbc/ "$lib" -
        continue
    fi

    meta="$(registry_driver "$name")"
    [[ -n "$meta" ]] || die "unknown driver '$name' (known: $KNOWN)"
    spec="$(tarball_for "$name" "$plat")"
    [[ -n "$spec" ]] || die "no pinned '$name' driver for $plat"
    command -v tar >/dev/null || die "need tar"
    read -r version license entrypoint publisher <<<"$meta"
    read -r sha lib <<<"$spec"
    file="${name}_${plat}_${version}.tar.gz"
    echo "Downloading $file"
    fetch "$REGISTRY/$name/$version/$file" "$work/$file"
    actual="$(sha256_of "$work/$file")"
    [[ "$actual" == "$sha" ]] || die "$file: SHA-256 mismatch (expected $sha, got $actual)"

    mkdir -p "$work/$name"
    tar -xzf "$work/$file" -C "$work/$name" "$lib"
    mkdir -p "$dest/$name"
    install -m 0755 "$work/$name/$lib" "$dest/$name/$lib"
    write_manifest "$name" "$version" \
        "$publisher (driver registry ${file}, sha256 ${sha})" \
        "$license" "$REGISTRY/$name/$version/$file" "$lib" "$entrypoint"
done

default_dir="${XDG_CONFIG_HOME:-$HOME/.config}/adbc/drivers"
if [[ "$dest" != "$default_dir" && ":${ADBC_DRIVER_PATH:-}:" != *":$dest:"* ]]; then
    echo "Note: $dest is not searched by default; set ADBC_DRIVER_PATH=$dest"
fi
