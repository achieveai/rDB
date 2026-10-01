# freeze.ps1: freeze (suspend) or thaw (resume) one node.exe process, like Ctrl-Z / kill -STOP.
# Windows has no SIGSTOP; this calls ntdll NtSuspendProcess. Used by LOAD-TESTING.txt (presence).
#   powershell -NoProfile -File freeze.ps1 suspend <pid>
#   powershell -NoProfile -File freeze.ps1 resume  <pid>
# Only node.exe is allowed, so a typo cannot freeze something else.
param(
  [Parameter(Mandatory, Position = 0)][ValidateSet('suspend', 'resume')][string]$Action,
  [Parameter(Mandatory, Position = 1)][int]$ProcessId
)
$p = Get-Process -Id $ProcessId -ErrorAction Stop
if ($p.ProcessName -ne 'node') { throw "pid $ProcessId is '$($p.ProcessName)', not node. Refusing." }
Add-Type -Namespace Nt -Name P -MemberDefinition @'
[DllImport("ntdll.dll")] public static extern uint NtSuspendProcess(IntPtr h);
[DllImport("ntdll.dll")] public static extern uint NtResumeProcess(IntPtr h);
'@
$rc = if ($Action -eq 'suspend') { [Nt.P]::NtSuspendProcess($p.Handle) } else { [Nt.P]::NtResumeProcess($p.Handle) }
if ($rc -ne 0) { throw "ntdll returned 0x$($rc.ToString('x'))" }
"$Action ok: pid $ProcessId ($(Get-Date -Format 'HH:mm:ss.fff') local)"
