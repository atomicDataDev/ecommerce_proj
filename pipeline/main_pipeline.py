"""
Pipeline orchestrator module (Facade pattern).

Coordinates the full Extract-Transform-Load workflow by delegating
each stage to injected component implementations.
"""

import logging

from pipeline.extractors import IEventsExtractor, IEnrichmentExtractor
from pipeline.transformers import ITransformer
from pipeline.loaders import ILoader


logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    High-level orchestrator that runs the ETL pipeline end-to-end.

    Accepts abstract interfaces via constructor injection, making it
    easy to swap implementations without modifying orchestration logic.

    :param file_extractor: Extractor for event log archives.
    :type file_extractor: IEventsExtractor
    :param db_extractor: Extractor for enrichment data (customers, products).
    :type db_extractor: IEnrichmentExtractor
    :param transformer: Business logic transformer.
    :type transformer: ITransformer
    :param loader: Output writer for the final report.
    :type loader: ILoader
    """

    def __init__(
            self,
            file_extractor: IEventsExtractor,
            db_extractor: IEnrichmentExtractor,
            transformer: ITransformer,
            loader: ILoader
    ):
        self.file_extractor = file_extractor
        self.db_extractor = db_extractor
        self.transformer = transformer
        self.loader = loader

    def run(self) -> None:
        """
        Executes the full ETL workflow sequentially.

        Steps:
        - Extract event logs from nested ZIP archives.
        - Extract customer and product catalogues from PostgreSQL.
        - Transform and aggregate data into a sales report.
        - Load the final report to the configured output destination.
        """
        logger.info("Starting pipeline")

        # -- Extract stage --
        logger.info("Extracting event data from ZIP archives")
        events_df = self.file_extractor.extract()

        logger.info("Extracting customer data from database")
        customers_df = self.db_extractor.extract_customers()

        logger.info("Extracting product data from database")
        products_df = self.db_extractor.extract_products()

        # -- Transform stage --
        logger.info("Transforming and aggregating data")
        report_df = self.transformer.transform(events_df, customers_df, products_df)

        # -- Load stage --
        self.loader.load(report_df)
        logger.info("Pipeline finished successfully")