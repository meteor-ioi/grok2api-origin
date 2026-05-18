import os
from workers import WorkerEntrypoint
import asgi

# The WorkerEntrypoint is the main entry point defined in wrangler.toml.
# It intercepts incoming requests, maps environment variables and bindings,
# and delegates the request execution to our FastAPI application via ASGI.
class Default(WorkerEntrypoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = None

    async def fetch(self, request):
        # 1. Map Cloudflare environment variables to os.environ so os.getenv() works
        # across the entire Python codebase without modifying original settings files.
        for key in dir(self.env):
            if not key.startswith("_"):
                val = getattr(self.env, key)
                if isinstance(val, (str, int, float, bool)):
                    os.environ[key] = str(val)

        # Ensure ACCOUNT_STORAGE is set to cloudflare_d1 in the serverless environment
        os.environ["ACCOUNT_STORAGE"] = "cloudflare_d1"

        # 2. Inject D1 SQLite Database binding for account & configuration storage
        if hasattr(self.env, "DB"):
            from app.control.account.backends.cloudflare_d1 import set_d1_database
            set_d1_database(self.env.DB)

        # 3. Lazily import and initialize FastAPI app after environment has been mapped.
        # This guarantees that get_repository_backend() correctly reads "cloudflare_d1".
        if self.app is None:
            from app.main import app
            self.app = app

        # 4. Bridge ASGI fetch request and return standard JS Response
        return await asgi.fetch(self.app, request.js_object, self.env)
