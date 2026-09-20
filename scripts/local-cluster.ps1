<#
.SYNOPSIS
    Run a local rEtcd cluster with one command: up, down, status, logs, clean.

.DESCRIPTION
    Generates N node TOML documents plus a signed bootstrap manifest for an insecure,
    dev-only cluster (ADR-0010: tls.mode = "insecure" behind --allow-insecure-dev;
    ADR-0012: --dev-allow-all), starts N config-server processes, waits for the ready
    line and a leader election through the health endpoint, and prints a status table.

    See docs/quickstart-local.md for the one-command quickstart this script implements.

.PARAMETER Nodes
    Node count for a fresh cluster. Default 3. Ignored when reusing an existing -Dir.

.PARAMETER Dir
    Cluster directory. Default .\.local-cluster. Holds one subdirectory per node plus the
    signed bootstrap manifest and small state files this script reads back on every command.

.PARAMETER BasePort
    First loopback port used. Each node claims 4 consecutive-ish ports
    (BasePort + (i-1)*10 + {1 peer, 2 client, 3 gossip, 4 health}). Default 17300.

.PARAMETER Node
    Node id for `logs`.

.PARAMETER Tail
    Lines to show for `logs`. Default 200.

.PARAMETER Follow
    Keep streaming for `logs`, like `tail -f`.

.PARAMETER TimeoutSec
    Deadline for "process printed its ready line" and "cluster has a leader". Default 30.

.EXAMPLE
    ./scripts/local-cluster.ps1 up
    ./scripts/local-cluster.ps1 up -Nodes 5 -Dir .\.my-cluster
    ./scripts/local-cluster.ps1 status
    ./scripts/local-cluster.ps1 logs -Node 1 -Follow
    ./scripts/local-cluster.ps1 down
    ./scripts/local-cluster.ps1 clean
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('up', 'down', 'status', 'logs', 'clean')]
    [string]$Command = 'up',

    [int]$Nodes = 3,
    [string]$Dir = '.\.local-cluster',
    [int]$BasePort = 17300,
    [Nullable[int]]$Node = $null,
    [int]$Tail = 200,
    [switch]$Follow,
    [int]$TimeoutSec = 30
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# ------------------------------------------------------------------------------------------
# Paths and binary
# ------------------------------------------------------------------------------------------

function Get-RepoRoot {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Get-ConfigServerBinary {
    $repoRoot = Get-RepoRoot
    $targetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $repoRoot 'target' }
    $exeName = if ($IsWindows -or -not (Test-Path variable:IsWindows)) { 'config-server.exe' } else { 'config-server' }
    $exe = Join-Path $targetDir (Join-Path 'debug' $exeName)
    if (-not (Test-Path $exe)) {
        Write-Host "config-server binary not found at $exe -- building (cargo build -p config-server)..."
        Push-Location $repoRoot
        try {
            & cargo build -p config-server
            if ($LASTEXITCODE -ne 0) {
                throw "cargo build -p config-server failed with exit code $LASTEXITCODE"
            }
        }
        finally {
            Pop-Location
        }
    }
    if (-not (Test-Path $exe)) {
        throw "config-server binary still not found at $exe after building"
    }
    return $exe
}

function Get-OpenSsl {
    $cmd = Get-Command openssl -ErrorAction SilentlyContinue
    if (-not $cmd) {
        throw "openssl was not found on PATH. It is required to sign the bootstrap manifest " +
              "(Ed25519, see docs/quickstart-local.md). Git for Windows ships one under " +
              "<git-install>\usr\bin; add it to PATH, or install OpenSSL directly."
    }
    return $cmd.Source
}

function Resolve-DirPath {
    param([string]$Path, [switch]$NoCreate)
    if (-not (Test-Path $Path)) {
        if ($NoCreate) {
            return [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $Path))
        }
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
    return (Resolve-Path $Path).Path
}

# ------------------------------------------------------------------------------------------
# Small state files: KEY=VALUE, one per line. No JSON dependency, shared shape with the
# .sh sibling so both scripts read/write the same cluster directory.
# ------------------------------------------------------------------------------------------

function Set-Utf8NoBom {
    param([string]$Path, [string[]]$Lines)
    $content = ($Lines -join "`n") + "`n"
    $enc = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $content, $enc)
}

function Read-EnvFile {
    param([string]$Path)
    $result = @{}
    if (-not (Test-Path $Path)) { return $result }
    foreach ($line in Get-Content -Path $Path) {
        if ($line -match '^\s*#' -or $line -match '^\s*$') { continue }
        $idx = $line.IndexOf('=')
        if ($idx -lt 0) { continue }
        $result[$line.Substring(0, $idx)] = $line.Substring($idx + 1)
    }
    return $result
}

# ------------------------------------------------------------------------------------------
# Fresh cluster generation
# ------------------------------------------------------------------------------------------

function New-ClusterId {
    $bytes = New-Object byte[] 16
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    -join ($bytes | ForEach-Object { $_.ToString('x2') })
}

function New-SignedManifest {
    param([string]$ManifestDir, [string]$ManifestPath)
    $openssl = Get-OpenSsl
    $keyPath = Join-Path $ManifestDir 'signing.pem'
    $pubDer = Join-Path $ManifestDir 'manifest.pub.der'
    $pubPath = Join-Path $ManifestDir 'manifest.pub'
    $sigPath = Join-Path $ManifestDir 'manifest.sig'

    & $openssl genpkey -algorithm ed25519 -out $keyPath *>$null
    if ($LASTEXITCODE -ne 0) { throw "openssl genpkey failed (exit $LASTEXITCODE)" }

    & $openssl pkey -in $keyPath -pubout -outform DER -out $pubDer *>$null
    if ($LASTEXITCODE -ne 0) { throw "openssl pkey (public key export) failed (exit $LASTEXITCODE)" }

    # RFC 8410 SubjectPublicKeyInfo for Ed25519 is a fixed 12-byte header followed by the raw
    # 32-byte public key; config-server's manifest verifier wants exactly those 32 raw bytes
    # (crates/config-server/src/manifest.rs::verify_document), not PEM/DER.
    $der = [System.IO.File]::ReadAllBytes($pubDer)
    if ($der.Length -ne 44) { throw "unexpected Ed25519 SPKI DER length $($der.Length), expected 44" }
    $rawPub = $der[12..43]
    [System.IO.File]::WriteAllBytes($pubPath, $rawPub)
    Remove-Item $pubDer -ErrorAction SilentlyContinue

    # -rawin: sign the exact message bytes (PureEdDSA), producing the raw 64-byte signature the
    # daemon verifies against the manifest bytes as written on disk.
    & $openssl pkeyutl -sign -inkey $keyPath -rawin -in $ManifestPath -out $sigPath *>$null
    if ($LASTEXITCODE -ne 0) { throw "openssl pkeyutl -sign failed (exit $LASTEXITCODE)" }
}

function New-Cluster {
    param([string]$DirFull, [int]$NodeCount, [int]$Base)

    if ($NodeCount -lt 1) { throw "-Nodes must be at least 1" }

    $existing = Get-ChildItem -Path $DirFull -Force -ErrorAction SilentlyContinue
    if ($existing) {
        throw "$DirFull is not empty and has no cluster.env; refusing to overwrite it. " +
              "Pick an empty/absent -Dir, or run 'clean' first."
    }

    $manifestDir = Join-Path $DirFull 'manifest'
    New-Item -ItemType Directory -Path $manifestDir -Force | Out-Null

    $clusterId = New-ClusterId
    $nodeInfo = @()
    for ($i = 1; $i -le $NodeCount; $i++) {
        $offset = $Base + ($i - 1) * 10
        $nodeInfo += [pscustomobject]@{
            Id     = $i
            Peer   = "127.0.0.1:$($offset + 1)"
            Client = "127.0.0.1:$($offset + 2)"
            Gossip = "127.0.0.1:$($offset + 3)"
            Health = "127.0.0.1:$($offset + 4)"
            Dir    = Join-Path $DirFull "node-$i"
        }
    }

    foreach ($n in $nodeInfo) {
        New-Item -ItemType Directory -Path $n.Dir -Force | Out-Null
        New-Item -ItemType Directory -Path (Join-Path $n.Dir 'data') -Force | Out-Null
        New-Item -ItemType Directory -Path (Join-Path $n.Dir 'logs') -Force | Out-Null
        Set-Utf8NoBom -Path (Join-Path $n.Dir 'node.env') -Lines @(
            "NODE_ID=$($n.Id)"
            "PEER=$($n.Peer)"
            "CLIENT=$($n.Client)"
            "GOSSIP=$($n.Gossip)"
            "HEALTH=$($n.Health)"
        )
    }

    # Bootstrap manifest (ADR-0011 §4.3): signed over the exact bytes, so it is written before
    # it is signed, and every voter's endpoint here must equal what that node actually binds --
    # which is why every port is fixed up front rather than ephemeral (port 0).
    $manifestLines = @(
        "cluster_id = `"$clusterId`""
        "recovery_epoch = 0"
        "expires_at = `"2120-01-01T00:00:00Z`""
        ""
    )
    foreach ($n in $nodeInfo) {
        $manifestLines += "[[voter]]"
        $manifestLines += "node_id = $($n.Id)"
        $manifestLines += "peer = `"$($n.Peer)`""
        $manifestLines += "client = `"$($n.Client)`""
        $manifestLines += ""
    }
    $manifestPath = Join-Path $manifestDir 'manifest.toml'
    Set-Utf8NoBom -Path $manifestPath -Lines $manifestLines
    New-SignedManifest -ManifestDir $manifestDir -ManifestPath $manifestPath

    # Relative to each node's own directory (../manifest/...), not absolute: every path in the
    # node TOML is resolved relative to the document's own directory (config-server/README.md),
    # and a relative path needs no Windows-vs-POSIX separator handling in either sibling script.
    $manifestPathToml = '../manifest/manifest.toml'
    $sigPathToml = '../manifest/manifest.sig'
    $pubPathToml = '../manifest/manifest.pub'

    foreach ($n in $nodeInfo) {
        $otherGossip = ($nodeInfo | Where-Object { $_.Id -ne $n.Id } |
            ForEach-Object { "`"$($_.Gossip)`"" }) -join ', '
        $configLines = @(
            "[node]"
            "node_id = $($n.Id)"
            "cluster_id = `"$clusterId`""
            "recovery_epoch = 0"
            "data_dir = `"data`""
            ""
            "[listen]"
            "peer = `"$($n.Peer)`""
            "client = `"$($n.Client)`""
            "gossip = `"$($n.Gossip)`""
            ""
            "[tls]"
            "mode = `"insecure`""
            ""
            "[gossip]"
            "seeds = [$otherGossip]"
            ""
            "[manifest]"
            "path = `"$manifestPathToml`""
            "sig = `"$sigPathToml`""
            "signing_key_pub = `"$pubPathToml`""
            ""
            "[raft]"
            "heartbeat_ms = 150"
            "election_min_ms = 450"
            "election_max_ms = 900"
        )
        Set-Utf8NoBom -Path (Join-Path $n.Dir 'config.toml') -Lines $configLines
    }

    Set-Utf8NoBom -Path (Join-Path $DirFull 'cluster.env') -Lines @(
        "CLUSTER_ID=$clusterId"
        "NODE_COUNT=$NodeCount"
        "BASE_PORT=$Base"
    )
}

# ------------------------------------------------------------------------------------------
# Process lifecycle
# ------------------------------------------------------------------------------------------

function Start-Node {
    param([string]$NodeDir, [switch]$Form)

    $exe = Get-ConfigServerBinary
    $nodeEnv = Read-EnvFile (Join-Path $NodeDir 'node.env')
    $configPath = Join-Path $NodeDir 'config.toml'
    $logDir = Join-Path $NodeDir 'logs'
    $stopFile = Join-Path $NodeDir 'stop'
    $stdout = Join-Path $NodeDir 'stdout.log'
    $stderr = Join-Path $NodeDir 'stderr.log'
    Remove-Item $stopFile, $stdout, $stderr -ErrorAction SilentlyContinue

    $argList = @(
        '--config', $configPath,
        '--log-dir', $logDir,
        '--shutdown-file', $stopFile,
        '--health-listen', $nodeEnv.HEALTH,
        '--allow-insecure-dev',
        '--dev-allow-all'
    )
    if ($Form) { $argList += '--form' }

    $proc = Start-Process -FilePath $exe -ArgumentList $argList -PassThru -NoNewWindow `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WorkingDirectory $NodeDir
    Set-Content -Path (Join-Path $NodeDir 'pid') -Value $proc.Id -NoNewline

    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $readyLine = $null
    while ((Get-Date) -lt $deadline) {
        if (Test-Path $stdout) {
            $line = Get-Content -Path $stdout -TotalCount 1 -ErrorAction SilentlyContinue
            if ($line) { $readyLine = $line; break }
        }
        if ($proc.HasExited) {
            $errText = Get-Content -Path $stderr -Raw -ErrorAction SilentlyContinue
            throw "node in '$NodeDir' exited before printing a ready line (exit $($proc.ExitCode)): $errText"
        }
        Start-Sleep -Milliseconds 150
    }
    if (-not $readyLine) {
        throw "node in '$NodeDir' did not print a ready line within $TimeoutSec s"
    }
    return ($readyLine | ConvertFrom-Json)
}

function Stop-Node {
    param([string]$NodeDir, [int]$Timeout = 15)

    $pidFile = Join-Path $NodeDir 'pid'
    if (-not (Test-Path $pidFile)) { return $false }
    $procId = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $procId) { Remove-Item $pidFile -ErrorAction SilentlyContinue; return $false }

    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if (-not $proc -or $proc.ProcessName -notlike 'config-server*') {
        Remove-Item $pidFile -ErrorAction SilentlyContinue
        return $false
    }

    # The documented mechanism (crates/config-server/README.md "Shutdown"): Windows has no
    # SIGTERM, so the daemon polls this file every 100ms and drains on its own.
    New-Item -ItemType File -Path (Join-Path $NodeDir 'stop') -Force | Out-Null

    $deadline = (Get-Date).AddSeconds($Timeout)
    while ((Get-Date) -lt $deadline) {
        $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if (-not $proc) { break }
        Start-Sleep -Milliseconds 150
    }
    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "  node in '$NodeDir' (pid $procId) did not exit within ${Timeout}s; killing"
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
    return $true
}

function Get-NodeHealth {
    param([string]$HealthAddr, [int]$Timeout = 2)
    try {
        return Invoke-RestMethod -Uri "http://$HealthAddr/health" -TimeoutSec $Timeout
    }
    catch {
        return $null
    }
}

function Get-NodeRuntimeStatus {
    param([string]$NodeDir)

    $nodeEnv = Read-EnvFile (Join-Path $NodeDir 'node.env')
    $pidFile = Join-Path $NodeDir 'pid'
    $running = $false
    $procId = $null
    if (Test-Path $pidFile) {
        $procId = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ($procId) {
            $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
            if ($proc -and $proc.ProcessName -like 'config-server*') { $running = $true }
        }
    }
    $health = $null
    if ($running) { $health = Get-NodeHealth -HealthAddr $nodeEnv.HEALTH }

    [pscustomobject]@{
        Node    = $nodeEnv.NODE_ID
        Client  = $nodeEnv.CLIENT
        Peer    = $nodeEnv.PEER
        Gossip  = $nodeEnv.GOSSIP
        Health  = $nodeEnv.HEALTH
        Pid     = $procId
        Running = $running
        Ready   = if ($health) { [bool]$health.ready } else { $false }
        Leader  = if ($health) { $health.current_leader } else { $null }
    }
}

function Wait-ClusterFormed {
    param([string]$DirFull, [int]$NodeCount, [int]$Timeout)

    $deadline = (Get-Date).AddSeconds($Timeout)
    $statuses = $null
    while ((Get-Date) -lt $deadline) {
        $statuses = 1..$NodeCount | ForEach-Object { Get-NodeRuntimeStatus (Join-Path $DirFull "node-$_") }
        $allReady = -not ($statuses | Where-Object { -not $_.Ready })
        $hasLeader = [bool]($statuses | Where-Object { $null -ne $_.Leader })
        if ($allReady -and $hasLeader) { return $statuses }
        Start-Sleep -Milliseconds 200
    }
    Show-Table $statuses
    throw "cluster in '$DirFull' did not finish forming within $Timeout s (no leader elected across every node)"
}

function Show-Table {
    param($Statuses)
    $Statuses |
        Select-Object Node, Client, Peer, Gossip, Health,
            @{N = 'Status'; E = { if ($_.Running) { if ($_.Ready) { 'ready' } else { 'starting' } } else { 'stopped' } } },
            @{N = 'Leader'; E = { if ($null -ne $_.Leader) { $_.Leader } else { '-' } } },
            Pid |
        Format-Table -AutoSize |
        Out-String -Width 200 |
        Write-Host
}

# ------------------------------------------------------------------------------------------
# Subcommands
# ------------------------------------------------------------------------------------------

function Invoke-Up {
    $dirFull = Resolve-DirPath -Path $Dir
    $clusterEnvPath = Join-Path $dirFull 'cluster.env'

    if (Test-Path $clusterEnvPath) {
        $clusterEnv = Read-EnvFile $clusterEnvPath
        $nodeCount = [int]$clusterEnv.NODE_COUNT
        if ($Nodes -ne 3 -and $Nodes -ne $nodeCount) {
            Write-Host "note: -Nodes $Nodes ignored; '$dirFull' already holds a $nodeCount-node cluster"
        }

        $statuses = 1..$nodeCount | ForEach-Object { Get-NodeRuntimeStatus (Join-Path $dirFull "node-$_") }
        $runningCount = @($statuses | Where-Object { $_.Running }).Count

        if ($runningCount -eq $nodeCount) {
            Write-Host "Cluster already running in '$dirFull'."
            Show-Table $statuses
            return
        }
        if ($runningCount -gt 0) {
            throw "'$dirFull' is partially running ($runningCount/$nodeCount node(s) alive). " +
                  "Run '$($MyInvocation.MyCommand.Name) down -Dir $Dir' first."
        }

        Write-Host "Restarting existing $nodeCount-node cluster in '$dirFull' (already formed; no --form)..."
        for ($i = 1; $i -le $nodeCount; $i++) {
            Start-Node -NodeDir (Join-Path $dirFull "node-$i") | Out-Null
        }
    }
    else {
        Write-Host "Creating a fresh $Nodes-node cluster in '$dirFull'..."
        New-Cluster -DirFull $dirFull -NodeCount $Nodes -Base $BasePort
        $nodeCount = $Nodes

        # Followers first: --form starts replicating the initial membership immediately, and a
        # follower that is not listening yet would just fail the first append and wait a retry
        # (mirrors crates/config-server/tests/support/mod.rs::start_all).
        for ($i = 2; $i -le $nodeCount; $i++) {
            Start-Node -NodeDir (Join-Path $dirFull "node-$i") | Out-Null
        }
        Start-Node -NodeDir (Join-Path $dirFull "node-1") -Form | Out-Null
    }

    Write-Host "Waiting for the cluster to elect a leader..."
    $statuses = Wait-ClusterFormed -DirFull $dirFull -NodeCount $nodeCount -Timeout $TimeoutSec
    Write-Host "Cluster is up."
    Show-Table $statuses
}

function Invoke-Down {
    $dirFull = Resolve-DirPath -Path $Dir -NoCreate
    $clusterEnvPath = Join-Path $dirFull 'cluster.env'
    if (-not (Test-Path $clusterEnvPath)) {
        Write-Host "no cluster at '$dirFull'"
        return
    }
    $clusterEnv = Read-EnvFile $clusterEnvPath
    $nodeCount = [int]$clusterEnv.NODE_COUNT
    $stopped = 0
    for ($i = 1; $i -le $nodeCount; $i++) {
        if (Stop-Node -NodeDir (Join-Path $dirFull "node-$i") -Timeout $TimeoutSec) { $stopped++ }
    }
    Write-Host "Stopped $stopped/$nodeCount node(s) in '$dirFull'."
}

function Invoke-Status {
    $dirFull = Resolve-DirPath -Path $Dir -NoCreate
    $clusterEnvPath = Join-Path $dirFull 'cluster.env'
    if (-not (Test-Path $clusterEnvPath)) {
        Write-Host "no cluster at '$dirFull'"
        return
    }
    $clusterEnv = Read-EnvFile $clusterEnvPath
    $nodeCount = [int]$clusterEnv.NODE_COUNT
    $statuses = 1..$nodeCount | ForEach-Object { Get-NodeRuntimeStatus (Join-Path $dirFull "node-$_") }
    Show-Table $statuses
}

function Invoke-Logs {
    if ($null -eq $Node) { throw "-Node <id> is required for 'logs'" }
    $dirFull = Resolve-DirPath -Path $Dir -NoCreate
    $nodeDir = Join-Path $dirFull "node-$Node"
    if (-not (Test-Path $nodeDir)) { throw "no such node directory: '$nodeDir'" }
    $logFile = Join-Path $nodeDir "logs\$Node.jsonl"
    if (-not (Test-Path $logFile)) { throw "log file not written yet: '$logFile'" }
    if ($Follow) {
        Get-Content -Path $logFile -Tail $Tail -Wait
    }
    else {
        Get-Content -Path $logFile -Tail $Tail
    }
}

function Invoke-Clean {
    $dirFull = Resolve-DirPath -Path $Dir -NoCreate
    if (-not (Test-Path $dirFull)) {
        Write-Host "nothing to clean at '$dirFull'"
        return
    }
    if (Test-Path (Join-Path $dirFull 'cluster.env')) {
        Invoke-Down
    }
    Remove-Item -Recurse -Force $dirFull
    Write-Host "Removed '$dirFull'."
}

switch ($Command) {
    'up' { Invoke-Up }
    'down' { Invoke-Down }
    'status' { Invoke-Status }
    'logs' { Invoke-Logs }
    'clean' { Invoke-Clean }
}
