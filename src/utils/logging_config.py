"""
Structured logging configuration for the Vanguard Marketing Intelligence Platform.
Provides file + console logging with rotation.
"""

import logging
import logging.handlers
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import LOG_DIR, LOG_LEVEL


def setup_logging(name: str = "vanguard_intelligence") -> logging.Logger:
    """
    Configure and return a logger with console + rotating file handlers.
    
    Args:
        name: Logger name (appears in log output)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Formatter
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    # Rotating file handler (5 MB per file, keep 5 backups)
    log_file = LOG_DIR / f"{name}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialized → {log_file}")
    return logger
