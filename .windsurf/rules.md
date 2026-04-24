# Windsurf AI Rules — Nasdaq SaaS Project

## Deployment Automation

### Post-Ship Hook (MANDATORY)

**Rule ID**: `post-ship-auto-deploy`  
**Trigger**: After `ship` command completes successfully  
**Action**: Automatically execute `bash ./post-ship.sh`

**Instructions for AI Agent:**
1. When user types `ship`, proceed with standard file upload
2. IMMEDIATELY after ship completes (Exit code 0), run: `bash ./post-ship.sh`
3. Wait for post-ship.sh to complete before declaring success
4. If post-ship.sh fails, alert the user with the error output

**Rationale:**
- The server needs auto-restart after code updates
- Manual execution is error-prone and forgotten
- This hook ensures fresh code is always live immediately after ship

**Verification Steps:**
1. Confirm server IP is configured in `post-ship.sh` (currently: 157.180.92.230)
2. Ensure SSH key authentication works
3. Monitor for `[AUTO-DEPLOY]` messages in output

---

## Code Sovereignty (AI_GUIDELINES.md Aligned)

- Do not delete functions without explicit permission
- Use dynamic paths only (no hardcoded `/root/` in code)
- Minimal changes: surgical precision over bulk rewrites
- Verify syntax with `py_compile` after Python changes
