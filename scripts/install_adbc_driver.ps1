# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# Install an Apache ADBC driver on Windows so `read_adbc("<name>", ...)` finds
# it by name. The Windows counterpart of scripts/install_adbc_driver.sh.
#
#   powershell -ExecutionPolicy Bypass -File scripts\install_adbc_driver.ps1 sqlite postgresql
#   ... [-Dest DIR] [-NoRegistry] sqlite
#
# The drivers are Apache Arrow ADBC's own builds, taken from their PyPI wheels
# (a wheel is a zip; Python is not involved). Every download is pinned by
# version and SHA-256 below and verified before anything is unpacked. Uses only
# what ships with Windows PowerShell 5.1 and later.
#
# Each driver goes to DIR\<name>\adbc_driver_<name>.dll (DIR defaults to
# %LOCALAPPDATA%\ADBC\Drivers) and is registered under
# HKCU\SOFTWARE\ADBC\Drivers\<name>, where the ADBC driver manager looks up a
# bare driver name on Windows. With -NoRegistry a manifest DIR\<name>.toml is
# written instead; then set ADBC_DRIVER_PATH=DIR at run time.
#
# To bump a driver: change $AdbcVersion and every hash in $Wheels, taking
# them from https://pypi.org/pypi/adbc-driver-<name>/<version>/json.

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

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -ne 'AMD64') {
    throw "install_adbc_driver: unsupported CPU '$arch' (only x64 drivers are published)"
}

Add-Type -AssemblyName System.IO.Compression.FileSystem

New-Item -ItemType Directory -Force -Path $Dest | Out-Null
$Dest = (Resolve-Path $Dest).Path
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("ibex-adbc-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null

try {
    foreach ($name in $Drivers) {
        if (-not $Wheels.ContainsKey($name)) {
            throw "install_adbc_driver: no pinned '$name' driver (known: $($Wheels.Keys -join ', '))"
        }
        $wheel = $Wheels[$name]
        $download = Join-Path $work $wheel.File

        Write-Host "Downloading $($wheel.File)"
        Invoke-WebRequest -UseBasicParsing -Uri $wheel.Url -OutFile $download
        $actual = (Get-FileHash -Algorithm SHA256 -Path $download).Hash.ToLowerInvariant()
        if ($actual -ne $wheel.Sha256) {
            throw "install_adbc_driver: $($wheel.File): SHA-256 mismatch (expected $($wheel.Sha256), got $actual)"
        }

        # The wheel names the Windows DLL libadbc_driver_<name>.so; give it a .dll name.
        $entryName = "adbc_driver_$name/libadbc_driver_$name.so"
        $targetDir = Join-Path $Dest $name
        $target = Join-Path $targetDir "adbc_driver_$name.dll"
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
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

        $source = "Apache Arrow ADBC (PyPI wheel $($wheel.File), sha256 $($wheel.Sha256))"
        if ($NoRegistry) {
            $manifest = Join-Path $Dest "$name.toml"
            $lines = @(
                'manifest_version = 1',
                '',
                "name = 'ADBC $name driver'",
                "version = '$AdbcVersion'",
                "publisher = '$source'",
                "license = 'Apache-2.0'",
                '',
                '[Driver.shared]',
                "windows_amd64 = '$target'"
            )
            # TOML: no byte-order mark (Windows PowerShell's UTF8 encoding adds one).
            [System.IO.File]::WriteAllLines($manifest, $lines, (New-Object System.Text.UTF8Encoding $false))
            Write-Host "  manifest $manifest -> set ADBC_DRIVER_PATH=$Dest, then read_adbc(`"$name`", ...)"
        } else {
            $key = "HKCU:\SOFTWARE\ADBC\Drivers\$name"
            New-Item -Path $key -Force | Out-Null
            New-ItemProperty -Path $key -Name 'driver' -Value $target -PropertyType String -Force | Out-Null
            New-ItemProperty -Path $key -Name 'name' -Value "ADBC $name driver" -PropertyType String -Force | Out-Null
            New-ItemProperty -Path $key -Name 'version' -Value $AdbcVersion -PropertyType String -Force | Out-Null
            New-ItemProperty -Path $key -Name 'source' -Value $source -PropertyType String -Force | Out-Null
            New-ItemProperty -Path $key -Name 'manifest_version' -Value 1 -PropertyType DWord -Force | Out-Null
            Write-Host "  registered $key -> read_adbc(`"$name`", ...)"
        }
    }
} finally {
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
