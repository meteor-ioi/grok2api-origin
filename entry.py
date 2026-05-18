import os
from workers import WorkerEntrypoint
import asgi

_app = None

# The WorkerEntrypoint is the main entry point defined in wrangler.toml.
# It intercepts incoming requests, maps environment variables and bindings,
# and delegates the request execution to our FastAPI application via ASGI.
class Default(WorkerEntrypoint):
    async def fetch(self, request):
        global _app
        try:
            # 1. Map Cloudflare environment variables to os.environ so os.getenv() works
            # across the entire Python codebase without modifying original settings files.
            try:
                from js import Object
                env_keys = Object.keys(self.env)
            except Exception:
                env_keys = dir(self.env)

            for key in env_keys:
                if not key.startswith("_"):
                    try:
                        val = getattr(self.env, key)
                        if isinstance(val, (str, int, float, bool)):
                            os.environ[key] = str(val)
                    except AttributeError:
                        pass

            # Ensure ACCOUNT_STORAGE is set to cloudflare_d1 in the serverless environment
            os.environ["ACCOUNT_STORAGE"] = "cloudflare_d1"
            # Disable file logging dynamically to prevent folder creations in read-only environment
            os.environ["LOG_FILE_ENABLED"] = "false"

            # 2. Inject D1 SQLite Database binding for account & configuration storage
            db_binding = None
            try:
                db_binding = self.env.DB
            except AttributeError:
                pass

            if db_binding is not None:
                from app.control.account.backends.cloudflare_d1 import set_d1_database
                set_d1_database(db_binding)

            # 3. Lazily import and initialize FastAPI app after environment has been mapped.
            # This guarantees that get_repository_backend() correctly reads "cloudflare_d1".
            if _app is None:
                from app.main import app
                
                # Monkeypatch locks and local media cache since filesystem is read-only
                import app.main
                app.main._try_acquire_scheduler_lock = lambda: False
                
                async def dummy_reconcile(*args, **kwargs):
                    pass
                app.main.reconcile_local_media_cache_async = dummy_reconcile
                
                # --- Lazy Startup Initialization for Serverless Environment ---
                from app.control.account.backends.factory import create_repository
                from app.platform.startup import run_startup_migrations
                from app.platform.config.snapshot import config
                
                # A. Initialize and load configuration backend
                await config.load()
                
                # B. Create and initialize repository schema in D1
                repo = create_repository()
                await repo.initialize()
                
                # C. Run first-boot migrations to seed D1 schema/config
                await run_startup_migrations(
                    config_backend=config._get_backend(),
                    account_repo=repo
                )
                
                # D. Populate application state variables
                from app.dataplane.account import get_account_directory
                app.state.repository = repo
                app.state.directory = await get_account_directory(repo)
                
                _app = app

            # 4. Bridge ASGI fetch request and return standard JS Response
            return await asgi.fetch(_app, request, self.env)
        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            from js import Response
            return Response.new(f"Python Worker Exception:\n{tb}", status=500)
