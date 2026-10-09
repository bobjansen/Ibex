# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# install.ps1 — install a released Ibex on Windows (x86_64).
#
#   powershell -ExecutionPolicy Bypass -c "irm https://github.com/bobjansen/Ibex/releases/latest/download/install.ps1 | iex"
#
# Downloads the release zip, checks it against the release's SHA256SUMS,
# unpacks it into $env:IBEX_HOME (default %LOCALAPPDATA%\Programs\Ibex), puts
# its bin\ on the user PATH and runs `ibex --version` to prove it starts.
#
# Environment:
#   IBEX_VERSION   release tag to install (default: the latest release)
#   IBEX_HOME      install directory
#   IBEX_REPO      GitHub owner/repo (default: bobjansen/Ibex)
#   IBEX_ARCHIVE   install this local zip instead of downloading (CI)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'  # Invoke-WebRequest is very slow with it on

$Repo = if ($env:IBEX_REPO) { $env:IBEX_REPO } else { 'bobjansen/Ibex' }
$IbexHome = if ($env:IBEX_HOME) { $env:IBEX_HOME } else { Join-Path $env:LOCALAPPDATA 'Programs\Ibex' }
$Platform = 'windows-x86_64'

if (-not [Environment]::Is64BitOperatingSystem) {
    throw "No prebuilt Ibex for 32-bit Windows. Build from source: https://github.com/$Repo#building"
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$Tmp = Join-Path ([IO.Path]::GetTempPath()) ("ibex-install-" + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $Tmp | Out-Null

try {
    if ($env:IBEX_ARCHIVE) {
        $Archive = $env:IBEX_ARCHIVE
        Write-Host "Installing Ibex from $Archive"
    } else {
        $Tag = $env:IBEX_VERSION
        if (-not $Tag) {
            $Tag = (Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest").tag_name
        }
        $Asset = "ibex-$Tag-$Platform.zip"
        $Base = "https://github.com/$Repo/releases/download/$Tag"
        Write-Host "Downloading Ibex $Tag ($Platform)"
        $Archive = Join-Path $Tmp $Asset
        Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset" -OutFile $Archive

        try {
            $Sums = Join-Path $Tmp 'SHA256SUMS'
            Invoke-WebRequest -UseBasicParsing -Uri "$Base/SHA256SUMS" -OutFile $Sums
            $Line = Get-Content $Sums | Where-Object { $_ -match "\s$([regex]::Escape($Asset))$" }
            if ($Line) {
                $Want = ($Line -split '\s+')[0].ToLower()
                $Got = (Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLower()
                if ($Want -ne $Got) { throw "Checksum mismatch for $Asset (expected $Want, got $Got)" }
            } else {
                Write-Warning "SHA256SUMS has no entry for $Asset; skipping checksum verification"
            }
        } catch [System.Net.WebException] {
            Write-Warning "Release has no SHA256SUMS; skipping checksum verification"
        }
    }

    $Unpack = Join-Path $Tmp 'unpack'
    Expand-Archive -Path $Archive -DestinationPath $Unpack
    # The archive holds one top-level directory, ibex-<tag>-<platform>\.
    $Src = Get-ChildItem -Path $Unpack -Directory | Select-Object -First 1
    if (-not $Src -or -not (Test-Path (Join-Path $Src.FullName 'bin\ibex.exe'))) {
        throw 'Unexpected archive layout (no bin\ibex.exe)'
    }

    # Replace the previous install whole, so no stale plugin outlives an upgrade.
    if (Test-Path $IbexHome) { Remove-Item -Recurse -Force $IbexHome }
    New-Item -ItemType Directory -Force -Path (Split-Path $IbexHome) | Out-Null
    Move-Item -Path $Src.FullName -Destination $IbexHome
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}

$Bin = Join-Path $IbexHome 'bin'
$Version = & (Join-Path $Bin 'ibex.exe') --version
if ($LASTEXITCODE -ne 0) {
    throw "Ibex was installed to $IbexHome but does not start. A missing Microsoft Visual C++ runtime is the usual cause: https://aka.ms/vs/17/release/vc_redist.x64.exe"
}

$UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$Entries = @($UserPath -split ';' | Where-Object { $_ })
if ($Entries -notcontains $Bin) {
    [Environment]::SetEnvironmentVariable('Path', (($Entries + $Bin) -join ';'), 'User')
    $env:Path = "$env:Path;$Bin"
    $PathNote = "Added $Bin to your user PATH; open a new terminal to pick it up."
} else {
    $PathNote = ''
}

Write-Host ''
Write-Host "Installed $Version to $IbexHome"
if ($PathNote) { Write-Host $PathNote }
Write-Host "Run 'ibex' to start the REPL, or 'ibex ui --demo' for the browser UI."
