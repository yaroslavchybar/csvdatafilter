import os
import csv
from pathlib import Path
from typing import Dict, List

from clean_data import detect_csv_separator
from supabase_client import get_client

BATCH_SIZE = 500

# Columns we expect to handle based on provided schema
KEEP_COLS = {
    "id",
    "user_name",
    "full_name",
    "created_at",
    "status",
    "assigned_to",
    "assignment_date",
    "subscribed_at",
}


def _read_rows(path: Path) -> List[Dict]:
    sep = detect_csv_separator(str(path))
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter=sep)
        rows: List[Dict] = []
        for r in rdr:
            row = {k: r.get(k) for k in KEEP_COLS}
            # normalize empty strings to None
            for k, v in list(row.items()):
                if isinstance(v, str) and v.strip() == "":
                    row[k] = None
            rows.append(row)
        return rows


def upload_to_supabase(csv_path: Path) -> int:
    """Uploads rows from csv_path into Supabase table via upsert.

    Controlled by env vars:
      - SUPABASE_UPLOAD: 'true' to enable
      - SUPABASE_TABLE: target table (default instagram_accounts_staging)
      - SUPABASE_ON_CONFLICT: conflict column (default 'id')
    """
    if os.getenv("SUPABASE_UPLOAD", "false").lower() != "true":
        return 0

    table = os.getenv("SUPABASE_TABLE", "instagram_accounts_staging")
    on_conflict = (os.getenv("SUPABASE_ON_CONFLICT", "") or "").strip()

    client = get_client()
    rows = _read_rows(Path(csv_path))
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        if on_conflict:
            client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
        else:
            client.table(table).insert(chunk).execute()
        total += len(chunk)
    return total
