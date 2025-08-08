"""Reader for parsing and extracting financial data from CSV files.

This module provides the CSVReader class, which reads, processes, and converts
financial CSV data into Transaction objects for use in Beancount workflows.
It uses the PETL library for table transformations, allowing flexible data
preprocessing, column mapping, and type conversions.

Typical usage example:

    reader = CSVReader(config, options)
    if reader.try_parse(file_path):
        reader.read_file(file_path)
        for transaction in reader.get_transactions():
            process(transaction)
"""

import datetime
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import petl as etl
from beancount.core.data import Transaction
from beancount.core.number import D
from beangulp import cache

from src.readers.reader import Reader


@dataclass
class CSVReaderOptions:
    """Configuration options for CSVReader.

    Attributes:
        max_rounding_error: Allowed rounding error when processing amounts.
        header_identifier: Regex string to identify the header row in the CSV.
        column_labels_line: Expected CSV header line containing column labels.
        date_format: Date format string for parsing date fields.
        skip_comments: String indicating comment lines to skip.
        header_map: Mapping from CSV column names to internal names.
        skip_transaction_types: List of transaction types to skip.
        transaction_type_map: Mapping of raw transaction type strings to normalized types.
        transformation_cb: Callback for applying custom table transformations.
    """

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
    """Reader implementation for CSV files.

    Attributes:
        FILE_EXTS: Supported file extensions for this reader.
        options: Configuration options for CSVReader.
        rdr: Processed PETL table after reading and conversion.
        ifile: Path to the last processed file.
    """

    FILE_EXTS: list[str] = ["csv"]

    def __init__(self, config: dict[str, Any], opt: CSVReaderOptions) -> None:
        """Initialize the CSVReader.

        Args:
            config: Configuration dictionary for the reader.
            opt: CSVReaderOptions instance with parsing and transformation rules.
        """
        super().__init__(config)
        self.options = opt

    def try_parse(self, file: str) -> bool:
        """Check if the file matches the configured header pattern.

        Args:
            file: Path to the file to check.

        Returns:
            True if the file header matches the identifier pattern, False otherwise.
        """
        if re.match(
            self.options.header_identifier,
            cache.get_file(file).head(
                encoding=getattr(self, "file_encoding", None)
            ),
        ):
            return True
        print("header_identifier failed---------------:")
        print(self.options.header_identifier, cache.get_file(file).head())
        return False

    def date(self, file: str) -> datetime.date:
        """Get the latest transaction date from the file.

        Args:
            file: Path to the CSV file.

        Returns:
            The most recent transaction date in the file.
        """
        self.initialize(file)
        self.read_file(file)
        return max(ot.date for ot in self.get_transactions()).date()

    def prepare_raw_file(self, rdr: Any) -> Any:
        """Transform raw table before processing (optional).

        Args:
            rdr: Raw PETL table.

        Returns:
            The transformed PETL table.
        """
        return rdr

    def fix_column_names(self, rdr: Any) -> Any:
        """Normalize column names by replacing certain characters with underscores.

        Args:
            rdr: PETL table.

        Returns:
            PETL table with updated column names.
        """
        header_map = {k: re.sub(r"[-/ ]", "_", k) for k in rdr.header()}
        return rdr.rename(header_map)

    def prepare_processed_table(self, rdr: Any) -> Any:
        """Apply final transformations to the processed table.

        Args:
            rdr: PETL table after column conversion.

        Returns:
            Final transformed PETL table.
        """
        return rdr

    def convert_columns(self, rdr: Any) -> Any:
        """Convert known columns to appropriate data types.

        Args:
            rdr: PETL table.

        Returns:
            PETL table with converted columns.
        """
        if "type" in rdr.header():
            rdr = rdr.convert("type", self.options.transaction_type_map)

        for col in ["units"]:
            if col in rdr.header():
                rdr = rdr.convert(col, D)

        def remove_non_numeric(x: str) -> str:
            return re.sub(r"[^0-9\.-]", "", str(x).strip())

        for col in getattr(self, "currency_fields", []) + [
            "unit_price",
            "fees",
            "total",
            "amount",
            "balance",
        ]:
            if col in rdr.header():
                rdr = rdr.convert(col, remove_non_numeric)
                rdr = rdr.convert(col, D)

        def convert_date(d: str) -> datetime.datetime:
            return datetime.datetime.strptime(
                d.strip(), self.options.date_format
            )

        for col in getattr(self, "date_fields", []) + [
            "date",
            "tradeDate",
            "settleDate",
        ]:
            if col in rdr.header():
                rdr = rdr.convert(col, convert_date)

        return rdr

    def read_raw(self, file: str) -> Any:
        """Read a raw CSV file into a PETL table.

        Args:
            file: Path to the CSV file.

        Returns:
            Raw PETL table.
        """
        return etl.fromcsv(
            file,
            encoding=getattr(self, "file_encoding", None),
            delimiter=getattr(self, "csv_delimiter", ","),
        )

    def skip_until_main_table(
        self, rdr: Any, col_labels: list[str] | None = None
    ) -> Any:
        """Skip rows until the header row is found.

        Args:
            rdr: PETL table.
            col_labels: Optional list of expected column labels.

        Returns:
            PETL table starting from the header row.
        """
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
        """Extract the main table with header and data rows.

        Args:
            rdr: PETL table.
            col_labels: Optional list of expected column labels.

        Returns:
            PETL table with only the relevant data rows.
        """
        rdr = self.skip_until_main_table(rdr, col_labels)
        nrows = len(rdr)
        for n, r in enumerate(rdr):
            if not r or all(i == "" for i in r):
                nrows = n - 1
                break
        return rdr.head(nrows)

    def skip_until_row_contains(self, rdr: Any, value: str) -> Any:
        """Skip rows until a specific value is found.

        Args:
            rdr: PETL table.
            value: String value to search for.

        Returns:
            PETL table starting from the found row.
        """
        start = None
        for n, r in enumerate(rdr):
            if value in r[0]:
                start = n
        if start is None:
            print(f'Error: table is not as expected. "{value}" row not found.')
            sys.exit(1)
        return rdr.rowslice(start, len(rdr))

    def read_file(self, file: str) -> None:
        """Read and fully process the CSV file.

        Args:
            file: Path to the CSV file.
        """
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

    def get_transactions(self) -> Iterator[Transaction]:
        """Yield transactions from the processed table.

        Returns:
            Iterator of Transaction objects.
        """
        for ot in self.rdr.namedtuples():
            if self.skip_transaction(ot):
                continue
            yield ot

    def skip_transaction(self, row: Transaction) -> bool:
        """Determine if a transaction should be skipped.

        Args:
            row: Transaction row.

        Returns:
            True if the transaction type is in the skip list, False otherwise.
        """
        return (
            getattr(row, "type", "NO_TYPE")
            in self.options.skip_transaction_types
        )

    def get_balance_assertion_date(self) -> datetime.date | None:
        """Get the balance assertion date.

        Returns:
            One day after the latest transaction date, or None if no date found.
        """
        if not self.get_max_transaction_date():
            return None
        return self.get_max_transaction_date() + datetime.timedelta(days=1)

    def get_max_transaction_date(self) -> datetime.date | None:
        """Get the latest transaction date in the processed table.

        Returns:
            The latest transaction date, or None if unavailable.
        """
        try:
            date = max(
                ot.tradeDate if hasattr(ot, "tradeDate") else ot.date
                for ot in self.get_transactions()
            ).date()
        except Exception:
            return None
        return date

    def get_row_by_label(self, file: str, label: str) -> list[str]:
        """Find a row where the first column matches a given label.

        Args:
            file: Path to the CSV file.
            label: Label to match in the first column.

        Returns:
            The matching row as a list of strings.
        """
        rdr = self.read_raw(file)
        rdr = self.prepare_raw_file(rdr)
        return rdr.select(lambda r: r[0] == label)[1]
