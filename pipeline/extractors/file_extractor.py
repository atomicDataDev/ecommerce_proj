"""
File-based event extractor for nested ZIP archives.

Handles the three-level archive structure:
master ZIP -> daily ZIP -> JSON part-files.
Supports batch processing to limit peak memory consumption.
"""

import json
import logging
import zipfile
from pathlib import Path
from typing import Generator

import pandas as pd
from pydantic import BaseModel, ValidationError

from pipeline.extractors.interfaces import IEventsExtractor
from pipeline.config import settings

logger = logging.getLogger(__name__)


class EventModel(BaseModel):
    """
    Pydantic model for validating individual event records.

    Events of type ``purchase`` carry a ``quantity`` field;
    other event types (``view_product``, ``add_to_cart``) do not.

    :ivar timestamp: ISO-8601 timestamp of the event.
    :ivar customer_id: Unique customer identifier (e.g., ``c001``).
    :ivar event_type: One of ``view_product``, ``add_to_cart``, ``purchase``.
    :ivar product_id: Unique product identifier (e.g., ``p042``).
    :ivar quantity: Number of units purchased (only for ``purchase`` events).
    """

    timestamp: str
    customer_id: str
    event_type: str
    product_id: str
    quantity: int | None = None


class FileExtractor(IEventsExtractor):
    """
    Extracts event logs from nested ZIP archives into a DataFrame.

    Iterates over all ``*.zip`` files in the data directory,
    unpacks daily archives, reads JSON part-files, validates
    each record via :class:`EventModel`, and assembles the
    results in memory-efficient batches.

    :ivar data_dir: Path to the directory containing master ZIP files.
    :type data_dir: str
    :ivar batch_size: Max number of events to accumulate before
        converting to a DataFrame chunk. Defaults to 50000.
    :type batch_size: int
    """

    def __init__(self):
        self.data_dir = Path(settings.data_dir)
        self.batch_size = settings.batch_size

    def _iter_events(self) -> Generator[dict, None, None]:
        """
        Lazily yields validated event dicts from the nested archive structure.

        Traverses: master ZIP -> daily ZIP -> JSON part-file -> event list.
        Invalid records are logged and skipped.

        :return: Generator of event dictionaries.
        :rtype: Generator[dict, None, None]
        """
        for master_zip_path in self.data_dir.glob("*.zip"):
            with zipfile.ZipFile(master_zip_path, 'r') as master_zip:

                for daily_zip_name in master_zip.namelist():
                    # Skip non-ZIP entries (e.g., metadata files).
                    if not daily_zip_name.endswith(".zip"):
                        continue

                    with master_zip.open(daily_zip_name) as daily_zip_file:
                        with zipfile.ZipFile(daily_zip_file, 'r') as daily_zip:

                            for json_name in daily_zip.namelist():
                                if not json_name.endswith('.json'):
                                    continue

                                with daily_zip.open(json_name) as json_file:
                                    raw_data = json.load(json_file)

                                    for item in raw_data:
                                        try:
                                            event = EventModel(**item)
                                            yield event.model_dump()
                                        except ValidationError as e:
                                            logger.critical(f"Critical error occured: {e}", exc_info=True)
                                            continue

    def extract(self) -> pd.DataFrame:
        """
        Reads all event archives and returns a consolidated DataFrame.

        Events are accumulated in batches of ``batch_size`` to avoid
        holding millions of raw dicts in memory simultaneously.
        Each batch is converted to a DataFrame immediately, then
        all chunks are concatenated at the end.

        :return: DataFrame with columns ``timestamp``, ``customer_id``,
            ``event_type``, ``product_id``, ``quantity``.
        :rtype: pd.DataFrame
        """
        chunks: list[pd.DataFrame] = []
        current_batch: list[dict] = []

        for event in self._iter_events():
            current_batch.append(event)

            # Convert to DataFrame once batch limit is reached.
            if len(current_batch) >= self.batch_size:
                chunks.append(pd.DataFrame(current_batch))
                current_batch = []

        # Flush remaining events that didn't fill a full batch.
        if current_batch:
            chunks.append(pd.DataFrame(current_batch))

        if chunks:
            return pd.concat(chunks, ignore_index=True)
        else:
            # Return empty DataFrame with expected schema if no data found.
            return pd.DataFrame(columns=['timestamp', 'customer_id', 'event_type', 'product_id', 'quantity'])