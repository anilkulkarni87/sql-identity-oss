import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for logs.
    Captures specific structured fields from the record if present.
    """

    def format(self, record: logging.LogRecord) -> str:
        # Base log object
        data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include specific structural fields if present in extra={}
        structured_keys = [
            "run_id",
            "stage",
            "event",
            "rows_affected",
            "row_count",
            "entities",
            "edges",
            "clusters",
            "duration_seconds",
            "duration_ms",
            "error",
        ]

        for key in structured_keys:
            if hasattr(record, key):
                data[key] = getattr(record, key)

        # Include exception info if present
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)

        return json.dumps(data)


def configure_logging(level: int = logging.INFO, json_format: Optional[bool] = None) -> None:
    """
    Configure the root logger.

    Args:
        level: Logging level (default INFO)
        json_format: Whether to use JSON formatting.
                     If None, checks IDR_JSON_LOGS env var.
    """
    if json_format is None:
        json_format = os.environ.get("IDR_JSON_LOGS", "0").lower() in ("1", "true", "yes")

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates/conflicts
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)

    if json_format:
        handler.setFormatter(JsonFormatter())
    else:
        # Standard human-readable format
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )

    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger."""
    return logging.getLogger(name)
