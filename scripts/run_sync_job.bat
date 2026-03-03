@echo off
setlocal

cd /d "%~dp0\.."

if exist "db_path.txt" (
  set /p HNV_DB_FILE=<db_path.txt
)

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "scripts\sync_job.py"
  exit /b %ERRORLEVEL%
)

where py >nul 2>&1
if %ERRORLEVEL%==0 (
  py -3 "scripts\sync_job.py"
  exit /b %ERRORLEVEL%
)

where python >nul 2>&1
if %ERRORLEVEL%==0 (
  python "scripts\sync_job.py"
  exit /b %ERRORLEVEL%
)

echo Python not found in PATH.
exit /b 1
