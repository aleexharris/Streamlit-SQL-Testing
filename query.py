import hashlib
import time
import pandas as pd
from typing import Literal
from dataclasses import dataclass

from consts import QUERY_PREVIEW_LEN


class Sql(str):
    def preview(self) -> str:
        query = self.strip().rstrip(";")
        if query.upper().strip().endswith("LIMIT"):
            query = query.rsplit("LIMIT", 1)[0].strip()
        elif "LIMIT" in query.upper():
            query = query.rsplit("LIMIT", 1)[0].strip()
        return f"{query}\nLIMIT {QUERY_PREVIEW_LEN};"


@dataclass
class Query:
    ts: int
    id: str
    sql: Sql
    preview: None | pd.DataFrame
    status: Literal["preview", "running", "completed", "failed"] = "preview"
    error_message: str = ""

    @classmethod
    def from_str(cls, s: str):
        ts = int(time.time())
        id = hashlib.sha256(f"{ts}:{s}".encode()).hexdigest()
        return cls(ts, id, Sql(s), None)
