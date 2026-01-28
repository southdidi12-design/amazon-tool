$taskName = "AmazonToolSync"
& schtasks /Delete /F /TN $taskName | Out-Host
Write-Host "Task removed: $taskName"
