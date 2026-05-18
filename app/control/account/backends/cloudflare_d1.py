import json
import logging
from typing import Any, List, Dict

from app.platform.runtime.clock import now_ms
from ..commands import AccountPatch, AccountUpsert, BulkReplacePoolCommand, ListAccountsQuery
from ..enums import AccountStatus
from ..models import (
    AccountChangeSet,
    AccountMutationResult,
    AccountPage,
    AccountRecord,
    RuntimeSnapshot,
)
from ..quota_defaults import default_quota_set

# Pyodide/Cloudflare FFI JS module
try:
    import js
except ImportError:
    js = None

logger = logging.getLogger("cloudflare_d1")

_db_instance = None


def set_d1_database(db: Any) -> None:
    """Explicitly inject D1 database binding."""
    global _db_instance
    _db_instance = db


def get_d1_database() -> Any:
    """Retrieve active D1 database binding."""
    global _db_instance
    if _db_instance is not None:
        return _db_instance
    if js is not None:
        try:
            return js.env.DB
        except AttributeError:
            pass
    raise RuntimeError("Cloudflare D1 database binding not initialized. Call set_d1_database(env.DB) first.")


def _row_to_record(row: Dict[str, Any]) -> AccountRecord:
    d = dict(row)
    d["tags"] = json.loads(d.get("tags") or "[]")
    
    heavy_raw = d.pop("quota_heavy", "{}") or "{}"
    grok_4_3_raw = d.pop("quota_grok_4_3", "{}") or "{}"
    heavy_dict = json.loads(heavy_raw)
    grok_4_3_dict = json.loads(grok_4_3_raw)
    
    d["quota"] = {
        "auto": json.loads(d.pop("quota_auto", "{}") or "{}"),
        "fast": json.loads(d.pop("quota_fast", "{}") or "{}"),
        "expert": json.loads(d.pop("quota_expert", "{}") or "{}"),
        **({"heavy": heavy_dict} if heavy_dict else {}),
        **({"grok_4_3": grok_4_3_dict} if grok_4_3_dict else {}),
    }
    d["ext"] = json.loads(d.get("ext") or "{}")
    return AccountRecord.model_validate(d)


class CloudflareD1AccountRepository:
    """Cloudflare Workers Native Python D1 Account Repository.
    
    Executes SQLite statements natively on Cloudflare's serverless D1 database 
    using the JavaScript FFI layer provided by Pyodide.
    """

    def __init__(self, binding_name: str = "DB") -> None:
        self.binding_name = binding_name
        self._initialized = False

    async def _execute(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        db = get_d1_database()
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        res_js = await stmt.all()
        res = res_js.to_py()
        return res.get("results", [])

    async def _run(self, sql: str, *params: Any) -> int:
        """Run a mutating statement and return rowcount or success."""
        db = get_d1_database()
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        res_js = await stmt.run()
        res = res_js.to_py()
        meta = res.get("meta", {})
        return meta.get("changes", 0)

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        # Create tables
        await self._run("""
            CREATE TABLE IF NOT EXISTS accounts (
                token TEXT PRIMARY KEY,
                pool TEXT NOT NULL DEFAULT 'basic',
                status TEXT NOT NULL DEFAULT 'active',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                quota_auto TEXT NOT NULL DEFAULT '{}',
                quota_fast TEXT NOT NULL DEFAULT '{}',
                quota_expert TEXT NOT NULL DEFAULT '{}',
                quota_heavy TEXT NOT NULL DEFAULT '{}',
                quota_grok_4_3 TEXT NOT NULL DEFAULT '{}',
                usage_use_count INTEGER NOT NULL DEFAULT 0,
                usage_fail_count INTEGER NOT NULL DEFAULT 0,
                usage_sync_count INTEGER NOT NULL DEFAULT 0,
                last_use_at INTEGER,
                last_fail_at INTEGER,
                last_fail_reason TEXT,
                last_sync_at INTEGER,
                last_clear_at INTEGER,
                state_reason TEXT,
                deleted_at INTEGER,
                ext TEXT NOT NULL DEFAULT '{}',
                revision INTEGER NOT NULL DEFAULT 0
            );
        """)
        await self._run("""
            CREATE TABLE IF NOT EXISTS account_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        await self._run("""
            INSERT OR IGNORE INTO account_meta (key, value) VALUES ('revision', '0');
        """)
        # Idempotent migration check for quota_grok_4_3 column
        columns_js = await get_d1_database().prepare("PRAGMA table_info(accounts)").all()
        columns = columns_js.to_py().get("results", [])
        has_grok_4_3 = any(c.get("name") == "quota_grok_4_3" for c in columns)
        if not has_grok_4_3:
            await self._run("ALTER TABLE accounts ADD COLUMN quota_grok_4_3 TEXT NOT NULL DEFAULT '{}'")
            
        self._initialized = True

    async def initialize(self) -> None:
        await self._ensure_initialized()

    async def get_revision(self) -> int:
        await self._ensure_initialized()
        rows = await self._execute("SELECT value FROM account_meta WHERE key = 'revision'")
        if rows:
            return int(rows[0].get("value") or 0)
        return 0

    async def _bump_revision(self) -> int:
        await self._run("UPDATE account_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'revision'")
        return await self.get_revision()

    async def runtime_snapshot(self) -> RuntimeSnapshot:
        await self._ensure_initialized()
        rev = await self.get_revision()
        rows = await self._execute("SELECT * FROM accounts WHERE deleted_at IS NULL")
        return RuntimeSnapshot(revision=rev, items=[_row_to_record(r) for r in rows])

    async def scan_changes(self, since_revision: int, *, limit: int = 5000) -> AccountChangeSet:
        await self._ensure_initialized()
        rev = await self.get_revision()
        rows = await self._execute("SELECT * FROM accounts WHERE revision > ? ORDER BY revision ASC LIMIT ?", since_revision, limit)
        
        items: List[AccountRecord] = []
        deleted: List[str] = []
        for row in rows:
            r = _row_to_record(row)
            if r.is_deleted():
                deleted.append(r.token)
            else:
                items.append(r)
        
        return AccountChangeSet(
            revision=rev,
            items=items,
            deleted_tokens=deleted,
            has_more=len(rows) == limit,
        )

    async def upsert_accounts(self, items: list[AccountUpsert]) -> AccountMutationResult:
        if not items:
            return AccountMutationResult()
        await self._ensure_initialized()
        rev = await self._bump_revision()
        ts = now_ms()
        count = 0
        for item in items:
            try:
                token = AccountRecord.model_validate({"token": item.token, "pool": item.pool}).token
            except Exception:
                continue
            pool = item.pool if item.pool in ("basic", "super", "heavy") else "basic"
            qs = default_quota_set(pool)
            
            await self._run("""
                INSERT OR REPLACE INTO accounts (
                    token, pool, status, created_at, updated_at, deleted_at, tags,
                    quota_auto, quota_fast, quota_expert, quota_heavy, quota_grok_4_3,
                    usage_use_count, usage_fail_count, usage_sync_count, ext, revision
                ) VALUES (?, ?, 'active', ?, ?, NULL, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?)
            """,
                token,
                pool,
                ts,
                ts,
                json.dumps(item.tags),
                json.dumps(qs.auto.to_dict()),
                json.dumps(qs.fast.to_dict()),
                json.dumps(qs.expert.to_dict()),
                json.dumps(qs.heavy.to_dict()) if qs.heavy else "{}",
                json.dumps(qs.grok_4_3.to_dict()) if qs.grok_4_3 else "{}",
                json.dumps(item.ext),
                rev
            )
            count += 1
        return AccountMutationResult(upserted=count, revision=rev)

    async def patch_accounts(self, patches: list[AccountPatch]) -> AccountMutationResult:
        if not patches:
            return AccountMutationResult()
        await self._ensure_initialized()
        rev = await self._bump_revision()
        ts = now_ms()
        count = 0
        for patch in patches:
            rows = await self._execute("SELECT * FROM accounts WHERE token = ?", patch.token)
            if not rows:
                continue
            record = _row_to_record(rows[0])
            
            updates: Dict[str, Any] = {"updated_at": ts, "revision": rev}
            if patch.pool is not None:
                updates["pool"] = patch.pool
            if patch.status is not None:
                updates["status"] = patch.status.value
            if patch.state_reason is not None:
                updates["state_reason"] = patch.state_reason
            if patch.last_use_at is not None:
                updates["last_use_at"] = patch.last_use_at
            if patch.last_fail_at is not None:
                updates["last_fail_at"] = patch.last_fail_at
            if patch.last_fail_reason is not None:
                updates["last_fail_reason"] = patch.last_fail_reason
            if patch.last_sync_at is not None:
                updates["last_sync_at"] = patch.last_sync_at
            if patch.last_clear_at is not None:
                updates["last_clear_at"] = patch.last_clear_at
            if patch.quota_auto is not None:
                updates["quota_auto"] = json.dumps(patch.quota_auto)
            if patch.quota_fast is not None:
                updates["quota_fast"] = json.dumps(patch.quota_fast)
            if patch.quota_expert is not None:
                updates["quota_expert"] = json.dumps(patch.quota_expert)
            if patch.quota_heavy is not None:
                updates["quota_heavy"] = json.dumps(patch.quota_heavy)
            if patch.quota_grok_4_3 is not None:
                updates["quota_grok_4_3"] = json.dumps(patch.quota_grok_4_3)
            if patch.usage_use_delta is not None:
                updates["usage_use_count"] = max(0, record.usage_use_count + patch.usage_use_delta)
            if patch.usage_fail_delta is not None:
                updates["usage_fail_count"] = max(0, record.usage_fail_count + patch.usage_fail_delta)
            if patch.usage_sync_delta is not None:
                updates["usage_sync_count"] = max(0, record.usage_sync_count + patch.usage_sync_delta)

            tags = list(record.tags)
            if patch.tags is not None:
                tags = patch.tags
            if patch.add_tags:
                for t in patch.add_tags:
                    if t not in tags:
                        tags.append(t)
            if patch.remove_tags:
                tags = [t for t in tags if t not in patch.remove_tags]
            updates["tags"] = json.dumps(tags)

            ext = dict(record.ext)
            if patch.ext_merge:
                ext.update(patch.ext_merge)
            if patch.clear_failures:
                for k in ("cooldown_until", "cooldown_reason", "disabled_at",
                          "disabled_reason", "expired_at", "expired_reason",
                          "forbidden_strikes"):
                    ext.pop(k, None)
                updates["status"] = AccountStatus.ACTIVE.value
                updates["usage_fail_count"] = 0
                updates["last_fail_at"] = None
                updates["last_fail_reason"] = None
                updates["state_reason"] = None
            updates["ext"] = json.dumps(ext)
            
            # Construct UPDATE statement dynamically
            set_parts = []
            vals = []
            for k, v in updates.items():
                set_parts.append(f"{k} = ?")
                vals.append(v)
            vals.append(patch.token)
            
            sql = f"UPDATE accounts SET {', '.join(set_parts)} WHERE token = ?"
            await self._run(sql, *vals)
            count += 1
            
        return AccountMutationResult(patched=count, revision=rev)

    async def delete_accounts(self, tokens: list[str]) -> AccountMutationResult:
        if not tokens:
            return AccountMutationResult()
        await self._ensure_initialized()
        rev = await self._bump_revision()
        ts = now_ms()
        count = 0
        for token in tokens:
            changes = await self._run(
                "UPDATE accounts SET deleted_at = ?, updated_at = ?, revision = ? WHERE token = ? AND deleted_at IS NULL",
                ts, ts, rev, token
            )
            count += changes
        return AccountMutationResult(deleted=count, revision=rev)

    async def get_accounts(self, tokens: list[str]) -> list[AccountRecord]:
        if not tokens:
            return []
        await self._ensure_initialized()
        records = []
        for token in tokens:
            rows = await self._execute("SELECT * FROM accounts WHERE token = ?", token)
            if rows:
                records.append(_row_to_record(rows[0]))
        return records

    async def list_accounts(self, query: ListAccountsQuery) -> AccountPage:
        await self._ensure_initialized()
        
        where_parts = []
        params = []
        
        if not query.include_deleted:
            where_parts.append("deleted_at IS NULL")
        if query.pool:
            where_parts.append("pool = ?")
            params.append(query.pool)
        if query.status:
            where_parts.append("status = ?")
            params.append(query.status.value)
            
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        
        count_rows = await self._execute(f"SELECT COUNT(*) as count FROM accounts {where_clause}", *params)
        total = int(count_rows[0].get("count") or 0) if count_rows else 0
        
        sort_col = query.sort_by if query.sort_by in ("updated_at", "created_at", "pool", "status", "usage_use_count") else "updated_at"
        sort_dir = "DESC" if query.sort_desc else "ASC"
        
        offset = (query.page - 1) * query.page_size
        limit = query.page_size
        
        sql = f"SELECT * FROM accounts {where_clause} ORDER BY {sort_col} {sort_dir} LIMIT ? OFFSET ?"
        rows = await self._execute(sql, *(params + [limit, offset]))
        rev = await self.get_revision()
        
        return AccountPage(
            items=[_row_to_record(r) for r in rows],
            total=total,
            page=query.page,
            page_size=query.page_size,
            total_pages=max(1, (total + query.page_size - 1) // query.page_size),
            revision=rev,
        )

    async def replace_pool(self, command: BulkReplacePoolCommand) -> AccountMutationResult:
        await self._ensure_initialized()
        rev = await self._bump_revision()
        ts = now_ms()
        
        deleted = await self._run(
            "UPDATE accounts SET deleted_at = ?, updated_at = ?, revision = ? WHERE pool = ? AND deleted_at IS NULL",
            ts, ts, rev, command.pool
        )
        
        upserted_result = await self.upsert_accounts(command.upserts)
        return AccountMutationResult(
            upserted=upserted_result.upserted,
            deleted=deleted,
            revision=upserted_result.revision,
        )

    async def close(self) -> None:
        pass
