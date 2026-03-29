import re
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path


def normalize_text(text: str) -> str:
    """Lowercase, strip punctuation except hyphens within words, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", " ", text)   # keep word chars, spaces, hyphens
    text = re.sub(r"-+", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def utc_now_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging(log_dir: Path, *, debug: bool = False) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    level = logging.DEBUG if debug else logging.INFO

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    for h in handlers:
        h.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger("excipient_finder")
    logger.setLevel(level)
    for h in handlers:
        logger.addHandler(h)
    return logger
