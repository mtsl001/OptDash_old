#!/usr/bin/env python3
"""
run_api.py — Start the FastAPI server.
Run this in a SEPARATE terminal from run_pipeline.py.
"""
import sys
from pathlib import Path

# ── Raise OS file-descriptor limit BEFORE anything opens files ──────────────
# macOS default is 256. DuckDB opens one fd per Parquet file during glob scans.
# By 12:00 IST there are ~80+ intraday .parquet files + sockets + DuckDB internals
# → hits the limit, causing "Too many open files" / InvalidInputException cascade.
# Hard limit on macOS is 65536; raise soft limit to match.
if sys.platform != "win32":
    try:
        import resource
        _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        _target = min(65536, _hard)
        if _soft < _target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (_target, _hard))
            print(f"[run_api] ulimit -n raised: {_soft} → {_target}")
        else:
            print(f"[run_api] ulimit -n already sufficient: {_soft}")
    except Exception as _e:
        print(f"[run_api] WARNING: Could not raise ulimit: {_e}")
else:
    print("[run_api] Windows detected: Skipping ulimit adjustment (not applicable)")
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "api"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "ai"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import uvicorn
import os

if __name__ == "__main__":
    host = os.getenv("API_HOST", "127.0.0.1")
    port = int(os.getenv("API_PORT", "8000"))
    # reload=True in dev; False for stability
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
        workers=1,
        log_level="info",
    )
