@echo off
REM Run Telegram bot from project root (required for "import bot.*").
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -m bot.dashboard
) else (
  python -m bot.dashboard
)
exit /b %ERRORLEVEL%
