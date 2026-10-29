@echo off
cd /d "%~dp0"
python -B run_local.py %*
set "LOCAL_RUN_EXIT=%ERRORLEVEL%"
pause
exit /b %LOCAL_RUN_EXIT%
