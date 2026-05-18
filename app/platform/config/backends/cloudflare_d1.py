import logging
from typing import Any, Dict, List

from .base import ConfigBackend
from ._serde import flatten, unflatten
from app.control.account.backends.cloudflare_d1 import get_d1_database

_TABLE       = "config_store"
_VERSION_KEY = "__version__"

logger = logging.getLogger("cloudflare_d1_config")


class CloudflareD1ConfigBackend(ConfigBackend):
    """Cloudflare Workers D1 Serverless Config Backend.
    
    Stores dotted key-value config overrides inside a D1 database table.
    """

    def __init__(self) -> None:
        self._ready = False

    async def _execute(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        db = get_d1_database()
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        res_js = await stmt.all()
        res = res_js.to_py()
        return res.get("results", [])

    async def _run(self, sql: str, *params: Any) -> int:
        db = get_d1_database()
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        res_js = await stmt.run()
        res = res_js.to_py()
        meta = res.get("meta", {})
        return meta.get("changes", 0)

    async def _ensure_table(self) -> None:
        if self._ready:
            return
        await self._run(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        self._ready = True

    async def load(self) -> dict[str, Any]:
        await self._ensure_table()
        rows = await self._execute(f"SELECT key, value FROM {_TABLE} WHERE key != ?", _VERSION_KEY)
        flat = {row.get("key"): row.get("value") for row in rows if row.get("key") is not None}
        return unflatten(flat)

    async def apply_patch(self, patch: dict[str, Any]) -> None:
        await self._ensure_table()
        flat = flatten(patch)
        if not flat:
            return
        
        for k, v in flat.items():
            await self._run(f"INSERT OR REPLACE INTO {_TABLE} (key, value) VALUES (?, ?)", k, str(v))
            
        # Atomically increment version counter
        await self._run(f"""
            INSERT OR REPLACE INTO {_TABLE} (key, value) VALUES (
                '{_VERSION_KEY}',
                CAST(COALESCE((SELECT CAST(value AS INTEGER) FROM {_TABLE} WHERE key = '{_VERSION_KEY}'), 0) + 1 AS TEXT)
            )
        """)

    async def version(self) -> object:
        await self._ensure_table()
        rows = await self._execute(f"SELECT value FROM {_TABLE} WHERE key = ?", _VERSION_KEY)
        if rows:
            val = rows[0].get("value")
            return int(val) if val else 0
        return 0

    async def close(self) -> None:
        pass
