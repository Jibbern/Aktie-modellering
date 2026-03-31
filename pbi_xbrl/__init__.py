"""Public package exports for the workbook build runtime.

The package-level surface stays intentionally small: callers typically need the
pipeline entrypoints, SEC client helpers, and the top-level ingest command
family. Everything else is organized in submodules so the internal stage layout
can evolve without changing the import contract used by `stock_models.py`,
tests, or small maintenance scripts.
"""

__version__ = "0.4.0"

from .pipeline import PipelineConfig, run_pipeline, write_excel  # noqa: F401,E402
from .sec_xbrl import SecClient, SecConfig, companyfacts_to_df, cik_from_ticker  # noqa: F401,E402
from .sec_ingest import IngestConfig, download_all  # noqa: F401,E402
