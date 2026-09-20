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

    Kept in sync with scripts/gate.sh.

.PARAMETER Stage
    fmt, lint, test, or all (the default).

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
    [ValidateSet('fmt', 'lint', 'test', 'all')]
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
# One log root per invocation. The DuckDB assertions union every file they glob before they
# filter, so a sibling binary still writing into a shared root collapses the read for all of
# them - which presents as a logging bug, not a concurrency one.
if (-not $env:RETCD_TEST_LOG_DIR) {
    $stamp = (Get-Date -Format 'yyyyMMdd-HHmmss')
    $env:RETCD_TEST_LOG_DIR = Join-Path $PWD "$($env:CARGO_TARGET_DIR)/test-logs/$stamp-$PID"
}

Write-Host "gate: target=$($env:CARGO_TARGET_DIR) scale=$($env:RETCD_TEST_DEADLINE_SCALE) logs=$($env:RETCD_TEST_LOG_DIR)"

function Invoke-Cargo([string[]]$Arguments) {
    & cargo @Arguments
    if ($LASTEXITCODE -ne 0) { throw "cargo $($Arguments -join ' ') failed with exit code $LASTEXITCODE" }
}

if ($Stage -in 'fmt', 'all')  { Write-Host '== fmt';    Invoke-Cargo @('fmt', '--all', '--check') }
if ($Stage -in 'lint', 'all') { Write-Host '== clippy'; Invoke-Cargo @('clippy', '--workspace', '--all-targets', '--', '-D', 'warnings') }
if ($Stage -in 'test', 'all') { Write-Host '== test';   Invoke-Cargo (@('test', '--workspace', '--no-fail-fast') + $CargoArgs) }

Write-Host "gate: $Stage OK"
