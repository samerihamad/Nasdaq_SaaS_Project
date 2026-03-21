@echo off
REM Use project venv so backup thread finds googleapiclient, cryptography, etc.
cd /d "%~dp0"
title NATB main.py
echo.
echo === NATB main.py ===
echo While running: press Ctrl+C here to STOP the engine.
echo.
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" main.py
) else (
  python main.py
)
echo.
echo [main.py exited, code %ERRORLEVEL%]
pause
