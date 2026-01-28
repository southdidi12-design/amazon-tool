$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$batPath = Join-Path $repoRoot "scripts\run_autopilot_job.bat"

if (-not (Test-Path $batPath)) {
  Write-Error "Missing run_autopilot_job.bat: $batPath"
  exit 1
}

$taskName = "AmazonToolAutopilot"
$startTime = "01:00"
& schtasks /Create /F /TN $taskName /SC DAILY /MO 1 /ST $startTime /TR $batPath | Out-Host

Write-Host "Task created: $taskName"
