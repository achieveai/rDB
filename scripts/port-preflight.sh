#!/usr/bin/env bash
# The test stage's port preflight: warns, never fails.
#
# Outbound connects — tonic channels, raw TcpStream::connect, memberlist's own push/pull — take
# their local port from the OS dynamic pool. RETCD_TEST_PORT_RANGE moves port-0 listeners out of
# that pool and cannot move these (issue #5). When other processes hold the pool in state Bound
# (2026-10-01: two IIS w3wp processes held ~15,700 of 16,384), cluster and gossip rows fail with
# `os error 10055` or never converge, and the cause is the host, not the change. This prints how
# much of the pool is held so a red run says so up front. See AGENTS.md "Running the gate".
#
# Called by scripts/gate.sh and scripts/gate.ps1, so the rule has one copy. Windows only.

set -uo pipefail

if [ "${OS:-}" != "Windows_NT" ]; then
  echo "gate: ports preflight not run (not Windows)"
  exit 0
fi
ps=$(command -v powershell.exe || command -v pwsh || true)
if [ -z "$ps" ]; then
  echo "gate: ports preflight not run (no powershell.exe or pwsh on PATH)"
  exit 0
fi

# One line out: "<start> <size> <bound>". netsh gives the pool; Get-NetTCPConnection the
# Bound ports in it, which `netstat` does not list. Only "no Bound port at all" reads as 0: any
# other failure of the measurement (CIM, WMI, access, a missing cmdlet) prints a non-number,
# so the run says "not run" instead of a reassuring bound=0 (review F-003).
measured=$("$ps" -NoProfile -NonInteractive -Command '
  $d = netsh int ipv4 show dynamicport tcp | Out-String
  $start = [int]([regex]::Match($d, "Start Port\s*:\s*(\d+)").Groups[1].Value)
  $size = [int]([regex]::Match($d, "Number of Ports\s*:\s*(\d+)").Groups[1].Value)
  try {
    $bound = @(Get-NetTCPConnection -State Bound -ErrorAction Stop |
      Where-Object { $_.LocalPort -ge $start -and $_.LocalPort -lt ($start + $size) }).Count
  } catch {
    if ($_.FullyQualifiedErrorId -notlike "CmdletizationQuery_NotFound*") {
      "unmeasured: $($_.Exception.Message)"
      exit
    }
    $bound = 0
  }
  "$start $size $bound"' 2>&1 | tr -d '\r')

read -r start size bound <<<"$measured"
case "$start$size$bound" in
  '' | *[!0-9]*)
    echo "gate: ports preflight not run (could not measure the pool): $measured"
    exit 0
    ;;
esac
if [ "$size" -eq 0 ]; then
  echo "gate: ports preflight not run (netsh reported no dynamic pool): $measured"
  exit 0
fi

echo "gate: dynamic tcp pool start=$start size=$size bound=$bound"
if [ $((bound * 2)) -ge "$size" ]; then
  echo "gate: WARNING: $bound of $size dynamic TCP ports are Bound by processes on this host." \
    "Outbound connects draw from this pool, so 10055 errors and gossip rows that do not converge" \
    "in this run are the host, not the change. Owners: Get-NetTCPConnection -State Bound |" \
    "Group-Object OwningProcess" >&2
fi
exit 0
