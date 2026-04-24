@echo off
REM Windows wrapper: Ship + Auto-Deploy
REM Usage: ship-and-deploy.bat

echo [SHIP-AND-DEPLOY] Starting deployment pipeline...

REM Step 1: Run Windsurf ship command
echo [SHIP-AND-DEPLOY] Uploading files...
ship

IF %ERRORLEVEL% NEQ 0 (
    echo [SHIP-AND-DEPLOY] Ship failed. Aborting.
    exit /b 1
)

REM Step 2: Run post-ship hook
echo [SHIP-AND-DEPLOY] Triggering server auto-deploy...
bash ./post-ship.sh

IF %ERRORLEVEL% NEQ 0 (
    echo [SHIP-AND-DEPLOY] Auto-deploy failed. Check SSH connection.
    exit /b 1
)

echo [SHIP-AND-DEPLOY] Success! Files shipped and server restarted.
