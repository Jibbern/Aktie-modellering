from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def silence_pdfminer_warnings(enabled: bool = True) -> Iterator[None]:
    if not enabled:
        yield
        return
    logger_names = (
        "pdfminer",
        "pdfminer.pdffont",
        "pdfminer.pdfinterp",
        "pdfminer.converter",
    )
    loggers = [logging.getLogger(name) for name in logger_names]
    prev_levels = [lg.level for lg in loggers]
    try:
        for lg in loggers:
            lg.setLevel(logging.ERROR)
        yield
    finally:
        for lg, level in zip(loggers, prev_levels):
            lg.setLevel(level)
