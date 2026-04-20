"""
System Purge & Reset Tool — NATB v2.0

Maintenance script to perform a clean sweep of all local databases and memory files
to fix 'state_hash_mismatch' errors and start fresh with 100% synchronized state.

Usage:
    python tools/system_purge.py [--dry-run] [--confirm]

Safety:
    - Only deletes DATA (keeps table structures intact)
    - Preserves .env and config files
    - Creates backup before purge (optional)
    - Logs all actions
"""

import os
import sys
import json
import sqlite3
import hashlib
import argparse
import shutil
from datetime import datetime
from pathlib import Path

# Ensure project root is in path
# Handle both Windows and Linux paths
if sys.platform == 'win32':
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
else:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import ADMIN_CHAT_ID
from database.db_manager import DB_PATH, create_db
from bot.notifier import send_telegram_message

# Data directory paths
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
MEMORY_FILE = os.path.join(DATA_DIR, "agent_memory.json")
LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", os.path.join(PROJECT_ROOT, "logs"))

# Tables to purge (data only, structure preserved)
TABLES_TO_PURGE = [
    "trades",
    "trade_sessions", 
    "pending_signals",
    "trade_rejections",
    "daily_risk_state",
    "pending_limit_orders",
]


def log_action(msg: str, print_stdout: bool = True):
    """Log purge action with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[SystemPurge {timestamp}] {msg}"
    if print_stdout:
        print(log_line)
    return log_line


def create_backup() -> str:
    """Create timestamped backup of critical files before purge."""
    backup_dir = os.path.join(DATA_DIR, "backups")
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_subdir = os.path.join(backup_dir, f"pre_purge_{timestamp}")
    os.makedirs(backup_subdir, exist_ok=True)
    
    # Backup database
    if os.path.exists(DB_PATH):
        db_backup = os.path.join(backup_subdir, "trading.db.bak")
        shutil.copy2(DB_PATH, db_backup)
        log_action(f"Database backed up to: {db_backup}")
    
    # Backup agent memory
    if os.path.exists(MEMORY_FILE):
        memory_backup = os.path.join(backup_subdir, "agent_memory.json.bak")
        shutil.copy2(MEMORY_FILE, memory_backup)
        log_action(f"Agent memory backed up to: {memory_backup}")
    
    log_action(f"Backup completed in: {backup_subdir}")
    return backup_subdir


def purge_database_tables(dry_run: bool = False) -> dict:
    """
    Purge all trading data from database tables.
    Keeps table structures and subscriber data intact.
    """
    results = {"purged": [], "errors": [], "rows_deleted": 0}
    
    if not os.path.exists(DB_PATH):
        log_action("Database file not found — nothing to purge")
        return results
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for table in TABLES_TO_PURGE:
            try:
                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                )
                if not cursor.fetchone():
                    log_action(f"Table '{table}' does not exist — skipping")
                    continue
                
                # Count rows before deletion
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if dry_run:
                    log_action(f"[DRY-RUN] Would delete {count} rows from '{table}'")
                    results["rows_deleted"] += count
                else:
                    # Delete all rows (keep structure)
                    cursor.execute(f"DELETE FROM {table}")
                    deleted = cursor.rowcount
                    results["rows_deleted"] += deleted
                    results["purged"].append(table)
                    log_action(f"Purged {deleted} rows from '{table}'")
                
            except Exception as e:
                error_msg = f"Error purging '{table}': {e}"
                log_action(error_msg)
                results["errors"].append(error_msg)
        
        if not dry_run:
            conn.commit()
            log_action("Database purge committed")
        
        conn.close()
        
    except Exception as e:
        error_msg = f"Database purge failed: {e}"
        log_action(error_msg)
        results["errors"].append(error_msg)
    
    return results


def reset_agent_memory(dry_run: bool = False) -> bool:
    """
    Reset agent_memory.json to empty structure.
    Preserves file, clears all experiences.
    """
    if not os.path.exists(MEMORY_FILE):
        log_action("Agent memory file not found — creating fresh")
        if not dry_run:
            os.makedirs(DATA_DIR, exist_ok=True)
    
    empty_structure = {
        "version": "1.1",
        "created_at": datetime.now().isoformat(),
        "purged_at": datetime.now().isoformat(),
        "experiences": {},
        "symbol_stats": {},
        "metadata": {
            "total_outcomes": 0,
            "total_wins": 0,
            "total_losses": 0,
            "last_purge": datetime.now().isoformat(),
        }
    }
    
    if dry_run:
        log_action(f"[DRY-RUN] Would reset agent_memory.json to empty structure")
        return True
    
    try:
        with open(MEMORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(empty_structure, f, indent=2)
        log_action("Agent memory reset to empty structure")
        return True
    except Exception as e:
        log_action(f"Error resetting agent memory: {e}")
        return False


def clear_execution_logs(dry_run: bool = False) -> bool:
    """Clear execution_events.txt log files."""
    logs_cleared = 0
    
    if os.path.exists(LOG_ROOT):
        for root, dirs, files in os.walk(LOG_ROOT):
            for file in files:
                if file == "execution_events.txt":
                    filepath = os.path.join(root, file)
                    if dry_run:
                        log_action(f"[DRY-RUN] Would clear: {filepath}")
                    else:
                        try:
                            # Truncate but keep file
                            with open(filepath, 'w') as f:
                                f.write(f"# Log cleared by SystemPurge at {datetime.now().isoformat()}\n")
                            logs_cleared += 1
                            log_action(f"Cleared execution log: {filepath}")
                        except Exception as e:
                            log_action(f"Error clearing {filepath}: {e}")
    
    log_action(f"Cleared {logs_cleared} execution log files")
    return logs_cleared > 0


def get_database_status() -> dict:
    """Get current database statistics."""
    status = {
        "exists": False,
        "tables": {},
        "total_rows": 0,
        "status": "unknown"
    }
    
    if not os.path.exists(DB_PATH):
        status["status"] = "missing"
        return status
    
    status["exists"] = True
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for table in TABLES_TO_PURGE + ["subscribers"]:
            try:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                )
                if cursor.fetchone():
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    status["tables"][table] = count
                    if table != "subscribers":
                        status["total_rows"] += count
            except Exception as e:
                status["tables"][table] = f"error: {e}"
        
        conn.close()
        
        # Determine cleanliness
        if status["total_rows"] == 0:
            status["status"] = "clean"
        elif status["total_rows"] < 10:
            status["status"] = "lightly_used"
        else:
            status["status"] = "active"
            
    except Exception as e:
        status["status"] = f"error: {e}"
    
    return status


def get_memory_status() -> dict:
    """Get agent memory file status."""
    status = {
        "exists": False,
        "experiences_count": 0,
        "status": "unknown"
    }
    
    if not os.path.exists(MEMORY_FILE):
        status["status"] = "missing"
        return status
    
    status["exists"] = True
    
    try:
        with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        experiences = data.get("experiences", {})
        total_exp = sum(len(exp_list) for exp_list in experiences.values())
        status["experiences_count"] = total_exp
        
        if total_exp == 0:
            status["status"] = "clean"
        elif total_exp < 10:
            status["status"] = "lightly_used"
        else:
            status["status"] = "active"
            
    except Exception as e:
        status["status"] = f"error: {e}"
    
    return status


def perform_system_purge(dry_run: bool = False, backup: bool = True) -> dict:
    """
    Perform complete system purge.
    
    Returns dict with results of purge operations.
    """
    log_action("=" * 60)
    log_action("SYSTEM PURGE INITIATED")
    log_action("=" * 60)
    log_action(f"Mode: {'DRY-RUN' if dry_run else 'LIVE'}")
    log_action(f"Backup: {'YES' if backup else 'NO'}")
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run,
        "backup_path": None,
        "database": {},
        "memory": False,
        "logs": False,
        "errors": []
    }
    
    # Create backup if requested
    if backup and not dry_run:
        results["backup_path"] = create_backup()
    
    # Purge database tables
    log_action("\n--- Purging Database Tables ---")
    results["database"] = purge_database_tables(dry_run=dry_run)
    
    # Reset agent memory
    log_action("\n--- Resetting Agent Memory ---")
    results["memory"] = reset_agent_memory(dry_run=dry_run)
    
    # Clear execution logs
    log_action("\n--- Clearing Execution Logs ---")
    results["logs"] = clear_execution_logs(dry_run=dry_run)
    
    # Summary
    log_action("\n" + "=" * 60)
    log_action("PURGE SUMMARY")
    log_action("=" * 60)
    
    if dry_run:
        log_action("DRY-RUN completed. No actual changes made.")
        log_action(f"Would delete: {results['database'].get('rows_deleted', 0)} database rows")
    else:
        log_action("PURGE completed successfully.")
        log_action(f"Deleted: {results['database'].get('rows_deleted', 0)} database rows")
        log_action(f"Purged tables: {', '.join(results['database'].get('purged', []))}")
        log_action(f"Agent memory: {'Reset' if results['memory'] else 'Failed'}")
        log_action(f"Execution logs: {'Cleared' if results['logs'] else 'Failed'}")
        if results["backup_path"]:
            log_action(f"Backup saved to: {results['backup_path']}")
    
    if results["database"].get("errors"):
        log_action(f"\nErrors encountered: {len(results['database']['errors'])}")
        for err in results["database"]["errors"]:
            log_action(f"  - {err}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="System Purge & Reset Tool for NATB v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python tools/system_purge.py --dry-run          # Preview what would be purged
    python tools/system_purge.py --confirm          # Execute live purge with backup
    python tools/system_purge.py --confirm --no-backup  # Execute without backup
        """
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview purge without making actual changes"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm live purge (required for actual deletion)"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup before purge (not recommended)"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current system status without purging"
    )
    
    args = parser.parse_args()
    
    # Show status only
    if args.status:
        print("=" * 60)
        print("SYSTEM STATUS REPORT")
        print("=" * 60)
        
        db_status = get_database_status()
        print(f"\nDatabase: {DB_PATH}")
        print(f"  Exists: {db_status['exists']}")
        print(f"  Status: {db_status['status']}")
        print(f"  Total Rows: {db_status['total_rows']}")
        print("  Table Counts:")
        for table, count in db_status['tables'].items():
            print(f"    - {table}: {count}")
        
        mem_status = get_memory_status()
        print(f"\nAgent Memory: {MEMORY_FILE}")
        print(f"  Exists: {mem_status['exists']}")
        print(f"  Status: {mem_status['status']}")
        print(f"  Experiences: {mem_status['experiences_count']}")
        
        print("\n" + "=" * 60)
        return
    
    # Validate arguments
    if not args.dry_run and not args.confirm:
        print("ERROR: Must specify --dry-run or --confirm")
        print("\nUse --dry-run to preview, or --confirm to execute purge.")
        print("Run with --help for more information.")
        sys.exit(1)
    
    # Execute purge
    dry_run = args.dry_run
    backup = not args.no_backup
    
    results = perform_system_purge(dry_run=dry_run, backup=backup)
    
    # Exit code based on success
    if results["database"].get("errors"):
        sys.exit(2)
    
    print("\n[SystemPurge] Operation completed.")


if __name__ == "__main__":
    main()
