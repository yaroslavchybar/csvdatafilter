import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple


def ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS permissions (
                telegram_user_id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                allowed INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        con.commit()
    finally:
        con.close()


def get_permission(db_path: Path, user_id: int) -> Optional[Tuple[int, str]]:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT allowed, email FROM permissions WHERE telegram_user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1])
    finally:
        con.close()


def set_permission(db_path: Path, user_id: int, email: str, allowed: int) -> None:
    now = int(time.time())
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            REPLACE INTO permissions(telegram_user_id, email, allowed, created_at, updated_at)
            VALUES(?, ?, ?, COALESCE((SELECT created_at FROM permissions WHERE telegram_user_id = ?), ?), ?)
            """,
            (user_id, email, int(allowed), user_id, now, now),
        )
        con.commit()
    finally:
        con.close()
