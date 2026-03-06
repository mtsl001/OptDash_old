"""
logger.py — Structured logging setup for the pipeline.
Call setup_logging() once at process startup, before any other imports.
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
import os


def setup_logging(log_dir: Path) -> None:
    """
    Configure root logger with:
    - Console handler (stdout, colourised if colorama available)
    - File handler (daily rotating, UTF-8)
    Level is read from LOG_LEVEL environment variable (default INFO).
    """
    try:
        import colorama
        colorama.init(autoreset=True)
    except ImportError:
        pass

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.today().strftime('%Y-%m-%d')}.log"

    fmt = "%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=log_level,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_file), encoding="utf-8"),
        ],
    )

    # Silence noisy third-party loggers
    for noisy in ["google.auth", "google.api_core", "urllib3", "apscheduler.executors"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger("pipeline").info(
        f"Logging initialised. Level={log_level_str}. File={log_file}"
    )
