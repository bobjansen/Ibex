# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# Install an ADBC driver on Windows so `adbc::connect("<name>", ...)` finds it
# by name. The Windows counterpart of scripts/install_adbc_driver.sh.
#
#   powershell -ExecutionPolicy Bypass -File scripts\install_adbc_driver.ps1 sqlite postgresql duckdb mysql
#   ... [-Dest DIR] [-NoRegistry] sqlite
#
# sqlite and postgresql are Apache Arrow ADBC's own builds, taken from their
# PyPI wheels (a wheel is a zip; Python is not involved). duckdb (DuckDB
# Foundation, MIT) and mysql (ADBC Driver Foundry, Apache-2.0) are the tarballs
# of Columnar's public driver registry, the ones `dbc install` fetches; dbc is
# not needed. Every download is pinned by version and SHA-256 below and
# verified before anything is unpacked. Uses only what ships with Windows 10
# and later: PowerShell 5.1 and tar.exe.
#
# Each driver goes to DIR\<name>\ (DIR defaults to
# %LOCALAPPDATA%\ADBC\Drivers) and is registered under
# HKCU\SOFTWARE\ADBC\Drivers\<name>, where the ADBC driver manager looks up a
# bare driver name on Windows. With -NoRegistry a manifest DIR\<name>.toml is
# written instead; then set ADBC_DRIVER_PATH=DIR at run time.
#
# To bump a wheel driver: change $AdbcVersion and every hash in $Wheels, taking
# them from https://pypi.org/pypi/adbc-driver-<name>/<version>/json. To bump a
# registry driver: change its entry in $RegistryDrivers; the registry index
# (https://dbc-cdn.columnar.tech/index.yaml) has no checksums, so download the
# tarball once and record its SHA-256.

# PositionalBinding off: otherwise a bare driver name binds to -Dest, the first
# declared parameter, and -Drivers is reported missing.
[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$Dest = (Join-Path $env:LOCALAPPDATA 'ADBC\Drivers'),
    [switch]$NoRegistry,
    [Parameter(Mandatory = $true, Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$Drivers
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'  # Invoke-WebRequest is very slow with it

$AdbcVersion = '1.12.0'

# Windows x64 only: Apache publishes no Windows arm64 wheels.
$Wheels = @{
    'sqlite' = @{
        File   = 'adbc_driver_sqlite-1.12.0-py3-none-win_amd64.whl'
        Sha256 = '0982bfc06158c2140b5c490b1a1325019c827b158f8432a30d49c8a0c18533ad'
        Url    = 'https://files.pythonhosted.org/packages/f4/d9/3245d741936100365ea77c434f84a0985523467bd812e5e11bb9b36d7152/adbc_driver_sqlite-1.12.0-py3-none-win_amd64.whl'
    }
    'postgresql' = @{
        File   = 'adbc_driver_postgresql-1.12.0-py3-none-win_amd64.whl'
        Sha256 = '5a3b5262eed6f28fb4c782b532e6a65caed1f2268fab7be736335ead49eed9dc'
        Url    = 'https://files.pythonhosted.org/packages/9d/02/7aa782cbb0134b09d1e67757c81332e0cd5e5697beecc8852468482193b0/adbc_driver_postgresql-1.12.0-py3-none-win_amd64.whl'
    }
}

$Registry = 'https://dbc-cdn.columnar.tech'

# Windows x64 tarballs. Entrypoint $null means the default (AdbcDriverInit).
$RegistryDrivers = @{
    'duckdb' = @{
        Version    = 'v1.5.6'
        Sha256     = '58e7cd73ef89b02b609849fc589b6896aa8fd007e5e474944ecd02b3ce809c06'
        Library    = 'duckdb.dll'
        Entrypoint = 'duckdb_adbc_init'
        License    = 'MIT'
        Publisher  = 'DuckDB Foundation'
    }
    'mysql' = @{
        Version    = 'v0.6.1'
        Sha256     = '7e6787e048720a5f03c07b47e516e5773fa0e0316b2775f933c51f00148d5087'
        Library    = 'libadbc_driver_mysql.dll'
        Entrypoint = $null
        License    = 'Apache-2.0'
        Publisher  = 'ADBC Driver Foundry'
    }
}

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -ne 'AMD64') {
    throw "install_adbc_driver: unsupported CPU '$arch' (only x64 drivers are published)"
}

Add-Type -AssemblyName System.IO.Compression.FileSystem

New-Item -ItemType Directory -Force -Path $Dest | Out-Null
$Dest = (Resolve-Path $Dest).Path
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("ibex-adbc-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null

# Download $Url to $Out and check it against $Sha256.
function Get-PinnedFile([string]$Url, [string]$Out, [string]$Sha256) {
    Write-Host "Downloading $(Split-Path -Leaf $Out)"
    Invoke-WebRequest -UseBasicParsing -Uri $Url -OutFile $Out
    $actual = (Get-FileHash -Algorithm SHA256 -Path $Out).Hash.ToLowerInvariant()
    if ($actual -ne $Sha256) {
        throw "install_adbc_driver: $(Split-Path -Leaf $Out): SHA-256 mismatch (expected $Sha256, got $actual)"
    }
}

# Make driver $name at $Target findable by name: a registry key, or with
# -NoRegistry a manifest DIR\<name>.toml.
function Register-Driver([string]$name, [string]$Target, [string]$Version, [string]$Source,
                         [string]$License, $Entrypoint) {
    if ($NoRegistry) {
        $manifest = Join-Path $Dest "$name.toml"
        $lines = @(
            'manifest_version = 1',
            '',
            "name = 'ADBC $name driver'",
            "version = '$Version'",
            "publisher = '$Source'",
            "license = '$License'",
            ''
        )
        if ($Entrypoint) {
            $lines += @('[Driver]', "entrypoint = '$Entrypoint'", '')
        }
        $lines += @('[Driver.shared]', "windows_amd64 = '$Target'")
        # TOML: no byte-order mark (Windows PowerShell's UTF8 encoding adds one).
        [System.IO.File]::WriteAllLines($manifest, $lines, (New-Object System.Text.UTF8Encoding $false))
        Write-Host "  manifest $manifest -> set ADBC_DRIVER_PATH=$Dest, then adbc::connect(`"$name`", ...)"
    } else {
        $key = "HKCU:\SOFTWARE\ADBC\Drivers\$name"
        New-Item -Path $key -Force | Out-Null
        New-ItemProperty -Path $key -Name 'driver' -Value $Target -PropertyType String -Force | Out-Null
        New-ItemProperty -Path $key -Name 'name' -Value "ADBC $name driver" -PropertyType String -Force | Out-Null
        New-ItemProperty -Path $key -Name 'version' -Value $Version -PropertyType String -Force | Out-Null
        New-ItemProperty -Path $key -Name 'source' -Value $Source -PropertyType String -Force | Out-Null
        New-ItemProperty -Path $key -Name 'manifest_version' -Value 1 -PropertyType DWord -Force | Out-Null
        if ($Entrypoint) {
            New-ItemProperty -Path $key -Name 'entrypoint' -Value $Entrypoint -PropertyType String -Force | Out-Null
        } else {
            Remove-ItemProperty -Path $key -Name 'entrypoint' -ErrorAction SilentlyContinue
        }
        Write-Host "  registered $key -> adbc::connect(`"$name`", ...)"
    }
}

try {
    foreach ($name in $Drivers) {
        $targetDir = Join-Path $Dest $name
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

        if ($Wheels.ContainsKey($name)) {
            $wheel = $Wheels[$name]
            $download = Join-Path $work $wheel.File
            Get-PinnedFile $wheel.Url $download $wheel.Sha256

            # The wheel names the Windows DLL libadbc_driver_<name>.so; give it a .dll name.
            $entryName = "adbc_driver_$name/libadbc_driver_$name.so"
            $target = Join-Path $targetDir "adbc_driver_$name.dll"
            $zip = [System.IO.Compression.ZipFile]::OpenRead($download)
            try {
                $entry = $zip.GetEntry($entryName)
                if ($null -eq $entry) {
                    throw "install_adbc_driver: $($wheel.File) has no $entryName"
                }
                [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $target, $true)
            } finally {
                $zip.Dispose()
            }
            Write-Host "Installed $target"
            Register-Driver $name $target $AdbcVersion `
                "Apache Arrow ADBC (PyPI wheel $($wheel.File), sha256 $($wheel.Sha256))" `
                'Apache-2.0' $null
        } elseif ($RegistryDrivers.ContainsKey($name)) {
            $d = $RegistryDrivers[$name]
            $file = "${name}_windows_amd64_$($d.Version).tar.gz"
            $download = Join-Path $work $file
            Get-PinnedFile "$Registry/$name/$($d.Version)/$file" $download $d.Sha256

            # tar.exe (bsdtar) ships with Windows 10 1803 and later.
            & tar.exe -xzf $download -C $targetDir $d.Library
            if ($LASTEXITCODE -ne 0) {
                throw "install_adbc_driver: could not unpack $($d.Library) from $file"
            }
            $target = Join-Path $targetDir $d.Library
            Write-Host "Installed $target"
            Register-Driver $name $target $d.Version `
                "$($d.Publisher) (driver registry $file, sha256 $($d.Sha256))" `
                $d.License $d.Entrypoint
        } else {
            $known = @($Wheels.Keys) + @($RegistryDrivers.Keys)
            throw "install_adbc_driver: no pinned '$name' driver (known: $($known -join ', '))"
        }
    }
} finally {
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
