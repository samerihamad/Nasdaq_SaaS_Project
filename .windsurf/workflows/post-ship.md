---
description: Auto-deploy trigger after ship completes
---

# Post-Ship Auto-Deploy Workflow

This workflow automatically triggers server deployment after a successful `ship` operation.

## Steps:

1. After `ship` completes file transfer
2. Execute local hook: `bash post-ship.sh`
3. `post-ship.sh` SSHs to server and runs `deploy.sh`
4. Server cleans cache, kills old process, restarts service

## Manual Trigger (if needed):

```bash
bash post-ship.sh
```

## Verification:

Check server logs for:
```
🚀 [AUTO-DEPLOY] New files detected...
✅ [AUTO-DEPLOY] Fresh code is now live.
```
