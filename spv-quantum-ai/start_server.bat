@echo off
title SPV Quantum AI Server

cd /d D:\KotakAlgo\spv-quantum-ai

echo =====================================
echo Starting SPV Quantum AI Server...
echo =====================================

if not exist venv\Scripts\python.exe (
    echo ERROR: venv not found at venv\Scripts\python.exe
    echo Run: python -m venv venv  then  venv\Scripts\pip install -r requirements.txt
    pause
    exit /b 1
)

REM Kill any process already bound to port 8000 so restart doesn't fail
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    taskkill /F /PID %%P >nul 2>&1
)

start http://127.0.0.1:8000

venv\Scripts\python.exe -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000

echo.
echo Server stopped.
pause
