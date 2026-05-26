@echo off
cd /d "%~dp0"
echo Killing anything on port 5050...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5050" ^| findstr LISTENING') do (
  echo taskkill PID %%a
  taskkill /PID %%a /F >nul 2>&1
)
timeout /t 2 /nobreak >nul
echo Starting dashboard on http://127.0.0.1:5050 ...
start "Polymarket Dashboard" py -3 app.py
timeout /t 3 /nobreak >nul
netstat -ano | findstr ":5050" | findstr LISTENING
if errorlevel 1 (
  echo FAILED - check py -3 and pip install -r requirements.txt
) else (
  echo OK - open http://127.0.0.1:5050/livetest
)
pause
