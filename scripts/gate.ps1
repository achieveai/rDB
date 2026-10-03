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

    The purity stage is the third non-cargo check, and row M7F-42: rdb-core's [dependencies]
    are the five ADR-rdb-0002 names, nothing under crates/rdb-core/src reaches a clock, a
    random source, the filesystem, the network, a thread or async, and no HashMap sits on a
    path a trace reaches. It lives in scripts/purity-check.sh and this script calls it, for
    the same reason the drift stage does.

    The drift stage is the second non-cargo check. Every M7 test plan declares the contract
    commit its section 15 was written against; this fails when that commit is no longer the
    newest one to touch crates/rdb-core/src/contracts. All four teams held a stale basis at
    once on 2026-09-20, and a stale basis always over-holds: rows report Unavailable on types
    that have already landed. The rule lives in scripts/drift-check.sh and this script calls
    it rather than restating it, because two copies of a rule drift apart.

    Kept in sync with scripts/gate.sh.

.PARAMETER Stage
    fmt, deps, drift, purity, lint, test, or all (the default).

    Every argument after the stage passes through to cargo, for narrowing a test stage.

.EXAMPLE
    pwsh scripts/gate.ps1
    The full gate.

.EXAMPLE
    pwsh scripts/gate.ps1 test -p config-testkit --test m4_watch_faults_cluster
    One suite, same environment.
#>
# A plain script on purpose: [CmdletBinding()] or any [Parameter()] adds PowerShell's common
# parameters, and then cargo's `-p` is refused as ambiguous (-ProgressAction, -PipelineVariable).
# Unbound arguments land in $args instead, in the order given.
param(
    [ValidateSet('fmt', 'deps', 'drift', 'purity', 'lint', 'test', 'all')]
    [string]$Stage = 'all'
)
$CargoArgs = [string[]]$args

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

# A private target directory: two cargo invocations sharing one lock each other out, and a
# test run that blocks on a build lock burns its own deadlines waiting.
if (-not $env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR = '.rtargets/gate' }
# Incremental artifacts are worthless for a full clean gate and cost disk on every run.
$env:CARGO_INCREMENTAL = '0'
if (-not $env:RETCD_TEST_DEADLINE_SCALE) { $env:RETCD_TEST_DEADLINE_SCALE = '3' }
# Port-0 binds draw from this range, below the OS dynamic pool, which other processes on the
# host can exhaust (os error 10055). See crates/config-gossip/src/ports.rs.
if (-not $env:RETCD_TEST_PORT_RANGE) { $env:RETCD_TEST_PORT_RANGE = '20000-26999' }
# One log root per invocation, inside the private target dir, so one gate run's logs never mix
# with another's. Separating the binaries *within* a run is already config-log's job: it writes
# each into a test_run_id subdirectory under this root.
# An absolute target dir is used as-is: Join-Path would prefix $PWD onto `C:/...` and put
# the logs inside the repo. A rooted `/c/...` is kept as given, so the logs land where cargo
# puts the target dir.
$targetRoot = if ([System.IO.Path]::IsPathRooted($env:CARGO_TARGET_DIR)) { $env:CARGO_TARGET_DIR } else { Join-Path $PWD $env:CARGO_TARGET_DIR }
if (-not $env:RETCD_TEST_LOG_DIR) {
    $stamp = (Get-Date -Format 'yyyyMMdd-HHmmss')
    $env:RETCD_TEST_LOG_DIR = "$targetRoot/test-logs/$stamp-$PID"
}
# Cluster data roots and other per-test directories (config_testkit::fs::temp_dir), one root
# per invocation like the logs, and never %TEMP%, which held 839 of them (5.7 GB) on
# 2026-09-27. Kept out of the log root, whose `<run>/*/*.jsonl` glob must match only logs.
# A root the script chose is removed after a passing test stage; one you set is left alone.
$gateOwnsData = -not $env:RETCD_TEST_DATA_DIR
if ($gateOwnsData) { $env:RETCD_TEST_DATA_DIR = "$targetRoot/test-data/$(Get-Date -Format 'yyyyMMdd-HHmmss')-$PID" }

Write-Host "gate: target=$($env:CARGO_TARGET_DIR) scale=$($env:RETCD_TEST_DEADLINE_SCALE) ports=$($env:RETCD_TEST_PORT_RANGE) logs=$($env:RETCD_TEST_LOG_DIR) data=$($env:RETCD_TEST_DATA_DIR)"

# `exit`, not `throw`: an uncaught throw ends the script with exit code 1, so a test failure
# (cargo's 101) and a build or usage error looked the same to the caller. gate.sh passes cargo's
# code through; so does this.
function Invoke-Cargo([string[]]$Arguments) {
    & cargo @Arguments
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("gate: cargo $($Arguments -join ' ') failed with exit code $LASTEXITCODE")
        exit $LASTEXITCODE
    }
}

function Test-Deps {
    $json = & cargo metadata --format-version 1 --no-deps
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("gate: cargo metadata failed with exit code $LASTEXITCODE")
        exit $LASTEXITCODE
    }
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

# `$Name` rather than `$Stage`, which would shadow this script's own parameter.
function Invoke-BashCheck([string]$Script, [string]$Name) {
    # One implementation of each rule, in its own shell script. Restating either of them here in
    # PowerShell would be a second source of truth, which is the thing both checks exist to
    # catch: the drift table itself keeps getting it wrong.
    $bash = (Get-Command bash -ErrorAction SilentlyContinue).Source
    if (-not $bash) {
        # Not skipped. A gate that quietly drops a check is worse than one that stops.
        throw "${Name}: bash not found. It ships with Git for Windows, which this repo already requires."
    }
    & $bash $Script
    if ($LASTEXITCODE -ne 0) { throw "${Name}: check failed (exit $LASTEXITCODE)" }
}

function Test-Drift { Invoke-BashCheck 'scripts/drift-check.sh' 'drift' }

function Test-Purity { Invoke-BashCheck 'scripts/purity-check.sh' 'purity' }

# Warns, never fails: outbound connects still draw from the OS dynamic pool (issue #5). The
# measurement lives in scripts/port-preflight.sh, so gate.sh and this script share one copy.
function Show-PortPreflight {
    $bash = (Get-Command bash -ErrorAction SilentlyContinue).Source
    if (-not $bash) { Write-Host 'gate: ports preflight not run (bash not found)'; return }
    & $bash 'scripts/port-preflight.sh'
}

if ($Stage -in 'fmt', 'all')  { Write-Host '== fmt';    Invoke-Cargo @('fmt', '--all', '--check') }
if ($Stage -in 'deps', 'all') { Write-Host '== deps';   Test-Deps }
if ($Stage -in 'drift', 'all') { Test-Drift }
if ($Stage -in 'purity', 'all') { Test-Purity }
if ($Stage -in 'lint', 'all') { Write-Host '== clippy'; Invoke-Cargo @('clippy', '--workspace', '--all-targets', '--', '-D', 'warnings') }
if ($Stage -in 'test', 'all') {
    Write-Host '== test'
    Show-PortPreflight
    # `--workspace` is dropped when the caller names a package: cargo ignores `-p` after
    # `--workspace` rather than rejecting it, so a scoped run silently became the whole
    # workspace (observed 2026-09-21). Same rule as gate.sh.
    $scoped = $CargoArgs | Where-Object { $_ -eq '-p' -or $_ -like '-p*' -or $_ -eq '--package' -or $_ -like '--package=*' }
    $scope = if ($scoped) { @() } else { @('--workspace') }
    Invoke-Cargo (@('test') + $scope + @('--no-fail-fast') + $CargoArgs)
    # Reached only when cargo passed. Every test process has exited by now, so no file under
    # the root is open, and this takes what a dropped `Cluster` could not remove before its
    # process ended. A failing run keeps its data for inspection.
    if ($gateOwnsData -and (Test-Path $env:RETCD_TEST_DATA_DIR)) { Remove-Item -Recurse -Force $env:RETCD_TEST_DATA_DIR }
}

Write-Host "gate: $Stage OK"
