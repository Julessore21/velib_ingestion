import logging

from .config import get_settings

_CONFIGURED = False


def configure_logging(log_level: str | None = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    settings = get_settings()
    level = (log_level or settings.log_level).upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
