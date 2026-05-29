"""
Entry point for the E-commerce Analytics ETL Pipeline.

Initializes all pipeline components - extractors, transformer,
and loader - then executes the full ETL workflow.
"""

import logging

from pipeline.utils import setup_logging, DBConnectionManager
from pipeline.config import settings
from pipeline.extractors import FileExtractor, DbExtractor
from pipeline.transformers import SalesTransformer
from pipeline.loaders import FileLoader
from pipeline.main_pipeline import ETLPipeline


# Configure logging before any pipeline logic runs.
setup_logging()
logger = logging.getLogger(__name__)


def main():
    """
    Assembles and runs the ETL pipeline.

    Creates concrete implementations for each pipeline stage
    and injects them into the orchestrator via constructor (DI).
    """
    # -- Database connection manager (shared across DB extractors) --
    db_manager = DBConnectionManager()

    # -- Build pipeline components --
    file_extractor = FileExtractor()
    db_extractor = DbExtractor(db_manager=db_manager)
    transformer = SalesTransformer()
    loader = FileLoader()

    # -- Assemble and run the pipeline --
    pipeline = ETLPipeline(
        file_extractor=file_extractor,
        db_extractor=db_extractor,
        transformer=transformer,
        loader=loader
    )

    pipeline.run()


if __name__ == '__main__':
    main()