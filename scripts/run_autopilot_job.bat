@echo off
setlocal

cd /d "%~dp0\.."

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "scripts\automation_job.py"
  exit /b %ERRORLEVEL%
)

if exist ".venv\bin\python" (
  ".venv\bin\python" "scripts\automation_job.py"
  exit /b %ERRORLEVEL%
)

if exist "E:\msys64\ucrt64\bin\python.exe" (
  "E:\msys64\ucrt64\bin\python.exe" "scripts\automation_job.py"
  exit /b %ERRORLEVEL%
)

where py >nul 2>&1
if %ERRORLEVEL%==0 (
  py -3 "scripts\automation_job.py"
  exit /b %ERRORLEVEL%
)

where python >nul 2>&1
if %ERRORLEVEL%==0 (
  python "scripts\automation_job.py"
  exit /b %ERRORLEVEL%
)

echo Python not found in PATH.
exit /b 1
