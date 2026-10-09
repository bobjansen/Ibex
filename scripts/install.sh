#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# install.sh — install a released Ibex on Linux (x86_64) or macOS (arm64).
#
#   curl -fsSL https://github.com/bobjansen/Ibex/releases/latest/download/install.sh | sh
#
# Downloads the release archive for this platform, checks it against the
# release's SHA256SUMS, unpacks it into $IBEX_HOME (default ~/.ibex) and runs
# `ibex --version` to prove it starts. The tree is self-contained (bin/ibex,
# bin/ui, lib/ibex), so uninstalling is `rm -rf ~/.ibex` plus the launcher.
#
# Environment:
#   IBEX_VERSION   release tag to install (default: the latest release)
#   IBEX_HOME      install directory (default: ~/.ibex)
#   IBEX_BIN_DIR   where the `ibex` launcher goes (default: ~/.local/bin)
#   IBEX_REPO      GitHub owner/repo (default: bobjansen/Ibex)
#   IBEX_ARCHIVE   install this local archive instead of downloading (CI)

set -eu

REPO="${IBEX_REPO:-bobjansen/Ibex}"
IBEX_HOME="${IBEX_HOME:-$HOME/.ibex}"
BIN_DIR="${IBEX_BIN_DIR:-$HOME/.local/bin}"

say() { printf '%s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"; }
need uname
need tar
need mktemp

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64 | Linux-amd64) PLATFORM=linux-x86_64 ;;
    Darwin-arm64 | Darwin-aarch64) PLATFORM=macos-arm64 ;;
    *)
        die "no prebuilt Ibex for $(uname -s) $(uname -m). Build from source:
  https://github.com/${REPO}#building"
        ;;
esac

download() {  # url dest
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 3 -o "$2" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$2" "$1"
    else
        die "need curl or wget to download Ibex"
    fi
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d' ' -f1
    else
        say ""
    fi
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ibex-install.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT INT TERM

if [ -n "${IBEX_ARCHIVE:-}" ]; then
    [ -f "$IBEX_ARCHIVE" ] || die "IBEX_ARCHIVE=$IBEX_ARCHIVE does not exist"
    ARCHIVE="$IBEX_ARCHIVE"
    say "Installing Ibex from $ARCHIVE"
else
    TAG="${IBEX_VERSION:-}"
    if [ -z "$TAG" ]; then
        # /releases/latest redirects to /releases/tag/<tag>: no API call, so no
        # rate limit and no JSON parsing.
        need curl
        TAG="$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
            "https://github.com/${REPO}/releases/latest")"
        TAG="${TAG##*/}"
        case "$TAG" in
            "" | latest | releases) die "could not determine the latest Ibex release" ;;
        esac
    fi
    ASSET="ibex-${TAG}-${PLATFORM}.tar.gz"
    BASE="https://github.com/${REPO}/releases/download/${TAG}"
    say "Downloading Ibex ${TAG} (${PLATFORM})"
    ARCHIVE="$TMP/$ASSET"
    download "$BASE/$ASSET" "$ARCHIVE" || die "download failed: $BASE/$ASSET"

    if download "$BASE/SHA256SUMS" "$TMP/SHA256SUMS" 2>/dev/null; then
        want="$(grep " ${ASSET}\$" "$TMP/SHA256SUMS" | cut -d' ' -f1)"
        got="$(sha256_of "$ARCHIVE")"
        if [ -n "$want" ] && [ -n "$got" ]; then
            [ "$want" = "$got" ] || die "checksum mismatch for $ASSET (expected $want, got $got)"
        else
            say "warning: could not verify the checksum of $ASSET"
        fi
    else
        say "warning: release has no SHA256SUMS; skipping checksum verification"
    fi
fi

mkdir -p "$TMP/unpack"
tar -xzf "$ARCHIVE" -C "$TMP/unpack"
# The archive holds one top-level directory, ibex-<tag>-<platform>/.
SRC="$(find "$TMP/unpack" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [ -z "$SRC" ] || [ ! -x "$SRC/bin/ibex" ]; then
    die "unexpected archive layout (no bin/ibex)"
fi

# Replace the previous install whole, so no stale plugin outlives an upgrade.
mkdir -p "$(dirname "$IBEX_HOME")"
rm -rf "$IBEX_HOME.new"
mv "$SRC" "$IBEX_HOME.new"
rm -rf "$IBEX_HOME"
mv "$IBEX_HOME.new" "$IBEX_HOME"

# Prove it starts. A missing system library is the usual Linux failure: name it.
if ! version="$("$IBEX_HOME/bin/ibex" --version 2>&1)"; then
    say "$version" >&2
    if [ "$PLATFORM" = linux-x86_64 ] && command -v ldd >/dev/null 2>&1; then
        missing="$(ldd "$IBEX_HOME/bin/ibex" 2>/dev/null | awk '/not found/ {print $1}' | sort -u | tr '\n' ' ')"
        if [ -n "$missing" ]; then
            say "" >&2
            say "Ibex needs these system libraries: $missing" >&2
            say "On Debian/Ubuntu: sudo apt install libreadline8t64 libcurl4t64" >&2
        fi
    fi
    die "Ibex was installed to $IBEX_HOME but does not start"
fi

# A launcher script rather than a symlink: the binary locates bin/ui and
# lib/ibex from its own path, and a script keeps that path the real one.
mkdir -p "$BIN_DIR"
cat > "$BIN_DIR/ibex" <<EOF
#!/bin/sh
exec "$IBEX_HOME/bin/ibex" "\$@"
EOF
chmod +x "$BIN_DIR/ibex"

say ""
say "Installed $version to $IBEX_HOME"
case ":${PATH}:" in
    *":$BIN_DIR:"*)
        say "Run 'ibex' to start the REPL, or 'ibex ui --demo' for the browser UI."
        ;;
    *)
        say "$BIN_DIR is not on your PATH. Add it, e.g. in ~/.bashrc or ~/.zshrc:"
        say ""
        say "    export PATH=\"$BIN_DIR:\$PATH\""
        say ""
        say "or run Ibex directly: $BIN_DIR/ibex"
        ;;
esac
