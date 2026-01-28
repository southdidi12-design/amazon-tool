$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$batPath = Join-Path $repoRoot "scripts\run_sync_job.bat"

if (-not (Test-Path $batPath)) {
  Write-Error "Missing run_sync_job.bat: $batPath"
  exit 1
}

$taskName = "AmazonToolSync"
& schtasks /Create /F /TN $taskName /SC HOURLY /MO 3 /TR $batPath | Out-Host

Write-Host "Task created: $taskName"
