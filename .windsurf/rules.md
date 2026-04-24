# Windsurf AI Rules — Nasdaq SaaS Project

## Deployment Automation (CRITICAL)

### Post-Ship Hook (MANDATORY - ENFORCED)

**Rule ID**: `post-ship-auto-deploy`  
**Status**: ⛔ MANDATORY - NO EXCEPTIONS  
**Trigger**: After `ship` command completes (Exit code 0)  
**Action**: **MUST** automatically execute `bash ./post-ship.sh`  

---

### ⚡ ENFORCEMENT PROTOCOL FOR AI AGENT (Cascade):

**WHEN user types `ship`:**
1. Execute standard `ship` command (upload files to server)
2. **WAIT** for ship to complete with Exit code 0
3. **IMMEDIATELY** execute in local terminal: `bash ./post-ship.sh`
4. **WAIT** for post-ship.sh to complete
5. **CONFIRM** success by checking output contains `[AUTO-DEPLOY]` or `✅ [POST-SHIP]`
6. **ONLY THEN** declare task complete

**CRITICAL RULES:**
- ⛔ **NEVER** skip the post-ship.sh execution
- ⛔ **NEVER** ask user "Do you want to run post-ship.sh?" - JUST DO IT
- ⛔ **NEVER** say "You can now run post-ship.sh manually" - IT MUST RUN AUTOMATICALLY
- ✅ **ALWAYS** execute post-ship.sh immediately after successful ship
- ✅ **ALWAYS** wait for completion and show the output to user

**FAILURE HANDLING:**
- If post-ship.sh fails (non-zero exit), show full error output
- Alert user: "❌ Post-ship auto-deploy failed. Manual fix required: bash post-ship.sh"
- Do not proceed with any other tasks until this is resolved

---

### Verification Command:

**To verify hook is active**, check that after `ship` you see:
```
🚀 [POST-SHIP] Deployment triggered...
🚀 [AUTO-DEPLOY] New files detected...
✅ [AUTO-DEPLOY] Fresh code is now live.
✅ Ship + Auto-deploy completed successfully
```

**If you DON'T see these messages, the hook is BROKEN.**

---

### Technical Notes:

- Server IP: 157.180.92.230
- Remote path: /root/Nasdaq_SaaS_Project
- SSH key should be in ssh-agent or ~/.ssh/config
- The deploy.sh on server handles: cache cleanup, process kill, service restart

---

## Code Sovereignty (AI_GUIDELINES.md Aligned)

- Do not delete functions without explicit permission
- Use dynamic paths only (no hardcoded `/root/` in code)
- Minimal changes: surgical precision over bulk rewrites
- Verify syntax with `py_compile` after Python changes
