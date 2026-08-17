# Stops what run.ps1 started.
$state = Join-Path $env:TEMP "aethermesh-example"

foreach ($file in "agents.pid", "controller.pid") {
    $path = Join-Path $state $file
    if (Test-Path $path) {
        Get-Content $path | ForEach-Object {
            Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
        }
        Remove-Item $path
    }
}

Write-Host "stopped"
