"""Reader for parsing and extracting financial data from CSV files.

This module defines a CSVReader class which processes CSV files. It extracts,
transforms, and converts CSV data into transaction objects used in Beancount
based workflows.
"""

import datetime
import re
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import petl as etl
from beancount.core.data import Transaction
from beancount.core.number import D
from beangulp import Importer as BaseImporter
from beangulp import cache

from src.readers.reader import Reader


@dataclass
class CSVReaderOptions:
    """Configuration options for CSVReader."""

    max_rounding_error: float
    header_identifier: str
    column_labels_line: str
    date_format: str
    skip_comments: str
    header_map: dict[str, str]
    skip_transaction_types: list[str]
    transaction_type_map: dict[str, str]
    transformation_cb: Callable[[Any], Any]


class CSVReader(Reader):
    """Reader implementation for CSV files."""

    FILE_EXTS: list[str] = ["csv"]

    def __init__(self, config: dict[str, Any], opt: CSVReaderOptions) -> None:
        """Initialize the CSVReader with configuration and options."""
        super().__init__(config)
        self.options = opt

    def initialize_reader(self, file: str) -> None:
        """Identify the file and set readiness status."""
        self.reader_ready = self.deep_identify(file)
        if self.reader_ready:
            self.file_read_done = False
        else:
            print("header_identifier failed---------------:")
            print(self.options.header_identifier, cache.get_file(file).head())

    def deep_identify(self, file: str) -> bool:
        """Match file header using the configured header_identifier pattern."""
        return bool(
            re.match(
                self.options.header_identifier,
                cache.get_file(file).head(
                    encoding=getattr(self, "file_encoding", None)
                ),
            )
        )

    def date(self, file: str) -> datetime.date:
        """Return the latest transaction date found in the file."""
        self.initialize(file)
        self.read_file(file)
        return max(ot.date for ot in self.get_transactions()).date()

    def prepare_raw_file(self, rdr: Any) -> Any:
        """Optionally transform raw table before processing."""
        return rdr

    def fix_column_names(self, rdr: Any) -> Any:
        """Replace special characters in header with underscores."""
        header_map = {k: re.sub(r"[-/ ]", "_", k) for k in rdr.header()}
        return rdr.rename(header_map)

    def prepare_processed_table(self, rdr: Any) -> Any:
        """Further transform the table after conversion."""
        return rdr

    def convert_columns(self, rdr: Any) -> Any:
        """Convert types for known columns such as dates, amounts, etc."""
        if "type" in rdr.header():
            rdr = rdr.convert("type", self.options.transaction_type_map)

        decimals = ["units"]
        for i in decimals:
            if i in rdr.header():
                rdr = rdr.convert(i, D)

        def remove_non_numeric(x: str) -> str:
            return re.sub(r"[^0-9\.-]", "", str(x).strip())

        currencies = getattr(self, "currency_fields", []) + [
            "unit_price",
            "fees",
            "total",
            "amount",
            "balance",
        ]
        for i in currencies:
            if i in rdr.header():
                rdr = rdr.convert(i, remove_non_numeric)
                rdr = rdr.convert(i, D)

        def convert_date(d: str) -> datetime.datetime:
            return datetime.datetime.strptime(
                d.strip(), self.options.date_format
            )

        dates = getattr(self, "date_fields", []) + [
            "date",
            "tradeDate",
            "settleDate",
        ]
        for i in dates:
            if i in rdr.header():
                rdr = rdr.convert(i, convert_date)

        return rdr

    def read_raw(self, file: str) -> Any:
        """Read raw CSV file using PETL."""
        return etl.fromcsv(
            file,
            encoding=getattr(self, "file_encoding", None),
            delimiter=getattr(self, "csv_delimiter", ","),
        )

    def skip_until_main_table(
        self, rdr: Any, col_labels: list[str] | None = None
    ) -> Any:
        """Skip rows until a line contains all column labels."""
        if not col_labels and hasattr(self.options, "column_labels_line"):
            col_labels = self.options.column_labels_line.replace(
                '"', ""
            ).split(getattr(self, "csv_delimiter", ","))
        if not col_labels:
            return rdr

        skip = None
        for n, r in enumerate(rdr):
            if all(i in list(r) for i in col_labels):
                skip = n
        if skip is None:
            print("Error: expected columns not found:")
            print(col_labels)
            sys.exit(1)
        return rdr.skip(skip)

    def extract_table_with_header(
        self, rdr: Any, col_labels: list[str] | None = None
    ) -> Any:
        """Return only the rows of the main table from the CSV file."""
        rdr = self.skip_until_main_table(rdr, col_labels)
        nrows = len(rdr)
        for n, r in enumerate(rdr):
            if not r or all(i == "" for i in r):
                nrows = n - 1
                break
        return rdr.head(nrows)

    def skip_until_row_contains(self, rdr: Any, value: str) -> Any:
        """Skip rows until one contains the target value."""
        start = None
        for n, r in enumerate(rdr):
            if value in r[0]:
                start = n
        if start is None:
            print(f'Error: table is not as expected. "{value}" row not found.')
            sys.exit(1)
        return rdr.rowslice(start, len(rdr))

    def read_file(self, file: str) -> None:
        """Process the CSV file into a structured, converted table."""
        if not getattr(self, "file_read_done", False):
            rdr = self.read_raw(file)
            rdr = self.prepare_raw_file(rdr)
            rdr = rdr.skip(getattr(self, "skip_head_rows", 0))
            rdr = rdr.head(len(rdr) - getattr(self, "skip_tail_rows", 0) - 1)
            rdr = self.extract_table_with_header(rdr)
            if hasattr(self.options, "skip_comments"):
                rdr = rdr.skipcomments(self.options.skip_comments)
            rdr = rdr.rowslice(getattr(self, "skip_data_rows", 0), None)
            rdr = self.options.transformation_cb(rdr)
            rdr = rdr.rename(self.options.header_map)
            rdr = self.convert_columns(rdr)
            rdr = self.fix_column_names(rdr)
            rdr = self.prepare_processed_table(rdr)
            self.rdr = rdr
            self.ifile = file
            self.file_read_done = True

    def get_transactions(self) -> Iterator[Transaction]:
        """Yield valid transactions, skipping those marked to skip."""
        for ot in self.rdr.namedtuples():
            if self.skip_transaction(ot):
                continue
            yield ot

    def skip_transaction(self, row: Transaction) -> bool:
        """Return True if transaction should be skipped based on type."""
        return (
            getattr(row, "type", "NO_TYPE")
            in self.options.skip_transaction_types
        )

    def get_balance_assertion_date(self) -> datetime.date | None:
        """Return the assertion date as one day after the last transaction."""
        if not self.get_max_transaction_date():
            return None
        return self.get_max_transaction_date() + datetime.timedelta(days=1)

    def get_max_transaction_date(self) -> datetime.date | None:
        """Get the latest transaction date."""
        try:
            date = max(
                ot.tradeDate if hasattr(ot, "tradeDate") else ot.date
                for ot in self.get_transactions()
            ).date()
        except Exception:
            return None
        return date

    def get_row_by_label(self, file: str, label: str) -> list[str]:
        """Return the first row whose first column matches the given label.

        Args:
            file: The path to the CSV file.
            label: The label to search for.

        Returns:
            A list representing the matching row.
        """
        rdr = self.read_raw(file)
        rdr = self.prepare_raw_file(rdr)
        return rdr.select(lambda r: r[0] == label)[1]
