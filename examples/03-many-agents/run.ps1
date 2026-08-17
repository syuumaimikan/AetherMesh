# Starts a controller and N agents on this machine, each with its own identity.
#
#   .\run.ps1 -Agents 4
param([int]$Agents = 4)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path "$PSScriptRoot\..\..").Path
$state = Join-Path $env:TEMP "aethermesh-example"
New-Item -ItemType Directory -Force -Path $state | Out-Null

$bin = Join-Path $root "target\release"
if (-not (Test-Path (Join-Path $bin "aether-controller.exe"))) {
    Write-Host "building..."
    Push-Location $root
    cargo build --release -p aether-controller -p aether-agent
    Pop-Location
}

if (-not $env:RUST_LOG) { $env:RUST_LOG = "info" }

$controller = Start-Process -PassThru -WindowStyle Hidden `
    -FilePath (Join-Path $bin "aether-controller.exe") `
    -ArgumentList "--listen", "127.0.0.1:7000", "--client-listen", "127.0.0.1:7100" `
    -RedirectStandardOutput (Join-Path $state "controller.log") `
    -RedirectStandardError (Join-Path $state "controller.err")
$controller.Id | Set-Content (Join-Path $state "controller.pid")
Start-Sleep -Seconds 1

$pids = foreach ($i in 0..($Agents - 1)) {
    # Separate identity files: agents sharing one would all claim to be the
    # same node, and the mesh would look like one machine reconnecting.
    $agent = Start-Process -PassThru -WindowStyle Hidden `
        -FilePath (Join-Path $bin "aether-agent.exe") `
        -ArgumentList "--controller", "127.0.0.1:7000", "--heartbeat-secs", "2",
                      "--identity-path", (Join-Path $state "node-$i") `
        -RedirectStandardOutput (Join-Path $state "agent-$i.log") `
        -RedirectStandardError (Join-Path $state "agent-$i.err")
    $agent.Id
}
$pids | Set-Content (Join-Path $state "agents.pid")

Start-Sleep -Seconds 2
Write-Host "controller + $Agents agents running; logs in $state"
Write-Host "submit work:  python $root\sdk\python\examples\hash.py"
Write-Host "stop:         $PSScriptRoot\stop.ps1"
