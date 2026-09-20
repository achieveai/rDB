#Requires -Version 7.0
<#
.SYNOPSIS
    The M6 evidence gate (ADR-0031, test plan M6-114).

.DESCRIPTION
    Reads every `*.json` artifact in the evidence directory and fails the build when one of them
    is not evidence:

      * the envelope is wrong - `schema` is not 1, `values` is empty, `build.git_sha` or
        `run.utc` is missing, or `disclaimer` is not the fixed constant `write_evidence()`
        emits. A malformed evidence file is worse than none, because it looks like evidence;
      * `run.full_scale` disagrees with `run.scale_factor`;
      * `run.full_scale` is false during a run that was explicitly invoked with
        `RETCD_EVIDENCE=1`. A full-scale request that silently degraded is a build problem, not
        a measurement.

    The last rule is the one ADR-0031 names. It is enforced when `RETCD_EVIDENCE=1` is set in
    the environment, or when -RequireFullScale is passed.

.PARAMETER Path
    Directory holding the artifacts. Defaults to `docs/evidence` next to this script.

.PARAMETER RequireFullScale
    Enforce the full-scale rule regardless of `RETCD_EVIDENCE`.

.EXAMPLE
    pwsh scripts/evidence-gate.ps1
    Checks the envelope of every artifact; reduced-scale artifacts pass.

.EXAMPLE
    $env:RETCD_EVIDENCE = '1'; pwsh scripts/evidence-gate.ps1
    Additionally fails if any artifact recorded a reduced-scale run.
#>
[CmdletBinding()]
param(
    [string]$Path = (Join-Path $PSScriptRoot '..' 'docs' 'evidence'),
    [switch]$RequireFullScale
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Kept byte-identical with `config_testkit::evidence::DISCLAIMER`. An artifact whose disclaimer
# was edited by hand is rejected here rather than quoted later.
$Disclaimer = 'Dev-host evidence. Not a production claim; production designation requires re-running on target hardware (spec §20, §12.2).'

$enforceFullScale = $RequireFullScale.IsPresent -or ($env:RETCD_EVIDENCE -eq '1')
$failures = [System.Collections.Generic.List[string]]::new()

if (-not (Test-Path -LiteralPath $Path)) {
    Write-Error "evidence directory not found: $Path"
    exit 1
}

$resolved = (Resolve-Path -LiteralPath $Path).Path
$files = @(Get-ChildItem -LiteralPath $resolved -Filter '*.json' -File | Sort-Object Name)

Write-Host "evidence gate: $resolved ($($files.Count) artifact(s), full-scale rule $(if ($enforceFullScale) { 'enforced' } else { 'not enforced' }))"

if ($files.Count -eq 0 -and $enforceFullScale) {
    Write-Error 'a full-scale run produced no artifacts at all'
    exit 1
}

foreach ($file in $files) {
    $name = $file.Name
    try {
        $artifact = Get-Content -LiteralPath $file.FullName -Raw -Encoding utf8 | ConvertFrom-Json
    }
    catch {
        $failures.Add("${name}: does not parse as JSON ($($_.Exception.Message))")
        continue
    }

    foreach ($field in @('schema', 'name', 'host', 'build', 'run', 'values', 'disclaimer')) {
        if ($null -eq $artifact.PSObject.Properties[$field]) {
            $failures.Add("${name}: missing top-level field '$field'")
        }
    }
    if ($failures.Count -gt 0 -and $failures[-1].StartsWith("${name}: missing")) { continue }

    if ($artifact.schema -ne 1) {
        $failures.Add("${name}: schema is $($artifact.schema), not 1")
    }
    if ($artifact.disclaimer -ne $Disclaimer) {
        $failures.Add("${name}: disclaimer is not the fixed constant emitted by write_evidence()")
    }
    if (-not $artifact.build.git_sha) {
        $failures.Add("${name}: build.git_sha is empty")
    }
    if (-not $artifact.run.utc) {
        $failures.Add("${name}: run.utc is empty")
    }
    if (@($artifact.values.PSObject.Properties).Count -eq 0) {
        $failures.Add("${name}: values is empty - the row measured nothing")
    }

    $factor = [double]$artifact.run.scale_factor
    $full = [bool]$artifact.run.full_scale
    if ($full -ne ($factor -ge 1.0)) {
        $failures.Add("${name}: run.full_scale=$full disagrees with run.scale_factor=$factor")
    }
    if ($enforceFullScale -and -not $full) {
        $failures.Add("${name}: full_scale is false (scale_factor $factor) during a RETCD_EVIDENCE=1 run")
    }

    if ($failures.Count -eq 0 -or -not ($failures[-1].StartsWith("${name}:"))) {
        Write-Host "  ok   $name (scale_factor $factor, full_scale $full)"
    }
}

if ($failures.Count -gt 0) {
    Write-Host ''
    Write-Host 'evidence gate FAILED:' -ForegroundColor Red
    foreach ($failure in $failures) { Write-Host "  - $failure" -ForegroundColor Red }
    exit 1
}

Write-Host 'evidence gate passed'
exit 0
