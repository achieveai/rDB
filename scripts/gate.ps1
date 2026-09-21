#Requires -Version 7.0
<#
.SYNOPSIS
    The workspace gate: format, lint, test - under the environment the rows were accepted in.

.DESCRIPTION
    The point of this script is the environment, not the three cargo invocations. Every M4-M6
    acceptance run was made with RETCD_TEST_DEADLINE_SCALE=3, a private target directory and a
    fresh log root, but nothing in the repository set them, so `cargo test --workspace` on a
    loaded host ran the cluster rows at a third of the patience they were accepted with and
    failed on capacity rows that are not broken (m4_69). A knob every contributor has to know
    about from somewhere else is not a knob.

    RETCD_TEST_DEADLINE_SCALE stretches deadlines only. Raft timers keep their real values, so
    the rows still test the real thing; a deadline here is a bound on how long a poll may wait
    for an observed state, never a sleep. See crates/config-testkit/src/poll.rs.

    The deps stage is the one non-cargo check: rdb-* may depend on config-*, never the reverse
    (rdb ADR-0002). It reads `cargo metadata` and fails on any config-* package that names an
    rdb-* dependency of any kind, dev and build included, because a dev-dependency is how the
    reverse edge would arrive first.

    The drift stage is the second non-cargo check. Every M7 test plan declares the contract
    commit its section 15 was written against; this fails when that commit is no longer the
    newest one to touch crates/rdb-core/src/contracts. All four teams held a stale basis at
    once on 2026-09-20, and a stale basis always over-holds: rows report Unavailable on types
    that have already landed. The rule lives in scripts/drift-check.sh and this script calls
    it rather than restating it, because two copies of a rule drift apart.

    Kept in sync with scripts/gate.sh.

.PARAMETER Stage
    fmt, deps, drift, lint, test, or all (the default).

.PARAMETER CargoArgs
    Extra arguments passed through to cargo, for narrowing a test stage.

.EXAMPLE
    pwsh scripts/gate.ps1
    The full gate.

.EXAMPLE
    pwsh scripts/gate.ps1 test -p config-testkit --test m4_watch_faults_cluster
    One suite, same environment.
#>
[CmdletBinding()]
param(
    [ValidateSet('fmt', 'deps', 'drift', 'lint', 'test', 'all')]
    [string]$Stage = 'all',
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CargoArgs = @()
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

# A private target directory: two cargo invocations sharing one lock each other out, and a
# test run that blocks on a build lock burns its own deadlines waiting.
if (-not $env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR = '.rtargets/gate' }
# Incremental artifacts are worthless for a full clean gate and cost disk on every run.
$env:CARGO_INCREMENTAL = '0'
if (-not $env:RETCD_TEST_DEADLINE_SCALE) { $env:RETCD_TEST_DEADLINE_SCALE = '3' }
# One log root per invocation, inside the private target dir, so one gate run's logs never mix
# with another's. Separating the binaries *within* a run is already config-log's job: it writes
# each into a test_run_id subdirectory under this root.
if (-not $env:RETCD_TEST_LOG_DIR) {
    $stamp = (Get-Date -Format 'yyyyMMdd-HHmmss')
    $env:RETCD_TEST_LOG_DIR = Join-Path $PWD "$($env:CARGO_TARGET_DIR)/test-logs/$stamp-$PID"
}

Write-Host "gate: target=$($env:CARGO_TARGET_DIR) scale=$($env:RETCD_TEST_DEADLINE_SCALE) logs=$($env:RETCD_TEST_LOG_DIR)"

function Invoke-Cargo([string[]]$Arguments) {
    & cargo @Arguments
    if ($LASTEXITCODE -ne 0) { throw "cargo $($Arguments -join ' ') failed with exit code $LASTEXITCODE" }
}

function Test-Deps {
    $json = & cargo metadata --format-version 1 --no-deps
    if ($LASTEXITCODE -ne 0) { throw "cargo metadata failed with exit code $LASTEXITCODE" }
    $meta = $json | ConvertFrom-Json
    $bad = @()
    foreach ($pkg in $meta.packages) {
        if ($pkg.name -notlike 'config-*') { continue }
        foreach ($dep in $pkg.dependencies) {
            if ($dep.name -notlike 'rdb-*') { continue }
            $kind = if ($dep.kind) { $dep.kind } else { 'normal' }
            $bad += "deps: $($pkg.name) depends on $($dep.name) ($kind)"
        }
    }
    if ($bad.Count -gt 0) {
        $bad | ForEach-Object { [Console]::Error.WriteLine($_) }
        throw 'deps: config-* must never depend on rdb-* (rdb ADR-0002)'
    }
}

function Test-Drift {
    # One implementation of the rule, in scripts/drift-check.sh. Restating it here in
    # PowerShell would be a second source of truth, which is the thing the drift table itself
    # keeps getting wrong.
    $bash = (Get-Command bash -ErrorAction SilentlyContinue).Source
    if (-not $bash) {
        # Not skipped. A gate that quietly drops a check is worse than one that stops.
        throw 'drift: bash not found. It ships with Git for Windows, which this repo already requires.'
    }
    & $bash 'scripts/drift-check.sh'
    if ($LASTEXITCODE -ne 0) { throw "drift: check failed (exit $LASTEXITCODE)" }
}

if ($Stage -in 'fmt', 'all')  { Write-Host '== fmt';    Invoke-Cargo @('fmt', '--all', '--check') }
if ($Stage -in 'deps', 'all') { Write-Host '== deps';   Test-Deps }
if ($Stage -in 'drift', 'all') { Test-Drift }
if ($Stage -in 'lint', 'all') { Write-Host '== clippy'; Invoke-Cargo @('clippy', '--workspace', '--all-targets', '--', '-D', 'warnings') }
if ($Stage -in 'test', 'all') {
    Write-Host '== test'
    # `--workspace` is dropped when the caller names a package: cargo ignores `-p` after
    # `--workspace` rather than rejecting it, so a scoped run silently became the whole
    # workspace (observed 2026-09-21). Same rule as gate.sh.
    $scoped = $CargoArgs | Where-Object { $_ -eq '-p' -or $_ -like '-p*' -or $_ -eq '--package' -or $_ -like '--package=*' }
    $scope = if ($scoped) { @() } else { @('--workspace') }
    Invoke-Cargo (@('test') + $scope + @('--no-fail-fast') + $CargoArgs)
}

Write-Host "gate: $Stage OK"
