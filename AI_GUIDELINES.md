# AI Development Constitution: Nasdaq AI Project (STRICT)

This document serves as the supreme mandate for any AI Agent modifying this codebase. Absolute adherence is mandatory.

## 1. Code Sovereignty & Preservation
* **DO NOT** delete any function, variable, or comment without explicit user request.
* If a piece of code seems "unused" or "redundant," the AI **MUST** provide a pre-execution report and ask for permission before removal.
* **DO NOT** modify core Timezone Logic, Currency settings, or Security protocols without written consent.

## 2. Pre-Coding Forensic Audit (Cleanup Protocol)
* **Pre-Scan:** Before writing new code, perform a full audit of the target file and related modules for legacy logic.
* **Fragment Cleanup:** * If existing code is functional: Modify only the required logic with "surgical precision."
    * If "Code Debris" or broken fragments (Legacy v2.0) are found: **PURGE** all fragments and start from a clean slate.
* **Zero-Redundancy:** Never define functions or variables twice within the same scope.

## 3. Minimalism & Anti-Hallucination
* **The 1:1 Rule:** Do not write 10 lines if 2 lines achieve the same result efficiently.
* **Native Over Complexity:** Prefer Python Standard Library over complex external dependencies unless necessary.
* **No Unrequested "Improvements":** Do not add "extra" features or aesthetic refactoring unless explicitly instructed.

## 4. Dynamic Path Enforcement
* **PROJECT_ROOT ONLY:** Absolute hardcoded paths (e.g., `/root/...` or `C:\Users\...`) are strictly **FORBIDDEN**.
* All file operations must use the dynamic `PROJECT_ROOT` variable to ensure cross-platform compatibility (Windows/Linux).

## 5. Post-Coding Self-Verification (Dry Run)
* **Execution Test:** After any modification, the AI must verify the code for `NameError` or `ImportError`.
* **Integration Check:** Ensure the "Main Engine" and "Telegram Dashboard" communication remains intact.
* **Safety Certification:** The AI must state: "Code verified; no initialization errors found."

## 6. Committee Protection (Multi-Agent Layer)
* The "Multi-Agent Committee" is the core intelligence of this project.
* **DO NOT** simplify or merge Agent roles (Technical, Trend, Memory, Sentiment). The system must remain **Modular** and scalable.

## 7. Modification & Approval Protocol
* Before applying major changes, provide a **Pre-Execution Report**:
    1. What is being changed?
    2. Why is it necessary?
    3. What is the expected impact?
* **WAIT** for the user to say "Approved" or "Proceed" before writing any code to the file.

---
**Note to AI:** Any violation of these rules will be considered a task failure. Be precise, be cautious.