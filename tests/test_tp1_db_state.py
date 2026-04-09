import os
import sqlite3
import tempfile
import unittest


class TestTP1SiblingDBState(unittest.TestCase):
    def _make_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="nasdaq_saas_test_", suffix=".db")
        os.close(fd)
        conn = sqlite3.connect(path)
        conn.execute(
            """
            CREATE TABLE trades (
                parent_session TEXT,
                leg_role TEXT,
                status TEXT,
                sync_status TEXT
            )
            """
        )
        conn.commit()
        conn.close()
        return path

    def test_is_sibling_tp1_leg_closed_true_when_status_closed(self):
        import core.executor as ex

        db_path = self._make_db()
        old_db = ex.DB_PATH
        ex.DB_PATH = db_path
        try:
            ps = "session-123"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO trades (parent_session, leg_role, status, sync_status) VALUES (?,?,?,?)",
                (ps, "TP1", "CLOSED", ""),
            )
            conn.commit()
            conn.close()

            self.assertTrue(ex.is_sibling_tp1_leg_closed(ps))
        finally:
            ex.DB_PATH = old_db
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_is_sibling_tp1_leg_closed_true_when_pending_sync(self):
        import core.executor as ex

        db_path = self._make_db()
        old_db = ex.DB_PATH
        ex.DB_PATH = db_path
        try:
            ps = "session-456"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO trades (parent_session, leg_role, status, sync_status) VALUES (?,?,?,?)",
                (ps, "TP1", "OPEN", "PENDING_SYNC"),
            )
            conn.commit()
            conn.close()

            self.assertTrue(ex.is_sibling_tp1_leg_closed(ps))
        finally:
            ex.DB_PATH = old_db
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_is_sibling_tp1_leg_closed_false_when_tp1_open_and_not_pending(self):
        import core.executor as ex

        db_path = self._make_db()
        old_db = ex.DB_PATH
        ex.DB_PATH = db_path
        try:
            ps = "session-789"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO trades (parent_session, leg_role, status, sync_status) VALUES (?,?,?,?)",
                (ps, "TP1", "OPEN", ""),
            )
            conn.commit()
            conn.close()

            self.assertFalse(ex.is_sibling_tp1_leg_closed(ps))
        finally:
            ex.DB_PATH = old_db
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_is_sibling_tp1_leg_closed_false_when_parent_session_missing(self):
        import core.executor as ex

        self.assertFalse(ex.is_sibling_tp1_leg_closed(None))
        self.assertFalse(ex.is_sibling_tp1_leg_closed(""))


if __name__ == "__main__":
    unittest.main()

