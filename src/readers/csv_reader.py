"""csv importer module for beancount to be used along with investment/banking/other importer modules in
beancount_reds_importers."""

import datetime
import re
import sys
import traceback
from typing import Callable
from dataclasses import dataclass

import petl as etl
from beancount.core.number import D
from beangulp import Importer as BaseImporter
from beangulp import cache

from src.readers.reader import Reader


@dataclass
class CSVReaderOptions:
    max_rounding_error: float
    header_identifier: str
    column_labels_line: str
    date_format: str
    skip_comments: str
    header_map: dict
    skip_transaction_types: list
    transaction_type_map: dict
    transformation_cb: Callable


class CSVReader(Reader):
    FILE_EXTS = ["csv"]

    def __init__(self, config, opt: CSVReaderOptions):
        super().__init__(config)
        self.options = opt

    def initialize_reader(self, file):
        self.reader_ready = self.deep_identify(file)
        if self.reader_ready:
            self.file_read_done = False
        else:
                print("header_identifier failed---------------:")
                print(self.options.header_identifier, cache.get_file(file).head())

    def deep_identify(self, file):
        return re.match(
            self.options.header_identifier,
            cache.get_file(file).head(encoding=getattr(self, "file_encoding", None)),
        )

    def date(self, file):
        "Get the maximum date from the file."
        self.initialize(file)  # self.date_format gets set via this
        self.read_file(file)
        return max(ot.date for ot in self.get_transactions()).date()

    def prepare_raw_file(self, rdr):
        return rdr

    def fix_column_names(self, rdr):
        header_map = {k: re.sub("[-/ ]", "_", k) for k in rdr.header()}
        rdr = rdr.rename(header_map)
        return rdr

    def prepare_processed_table(self, rdr):
        return rdr

    def convert_columns(self, rdr):
        # convert data in transaction types column
        if "type" in rdr.header():
            rdr = rdr.convert("type", self.options.transaction_type_map)

        # fixup decimals
        decimals = ["units"]
        for i in decimals:
            if i in rdr.header():
                rdr = rdr.convert(i, D)

        # fixup currencies
        def remove_non_numeric(x):
            return re.sub(r"[^0-9\.-]", "", str(x).strip())  # noqa: W605

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

        # fixup dates
        def convert_date(d):
            """Remove spaces and convert to datetime"""
            return datetime.datetime.strptime(d.strip(), self.options.date_format)

        dates = getattr(self, "date_fields", []) + ["date", "tradeDate", "settleDate"]
        for i in dates:
            if i in rdr.header():
                rdr = rdr.convert(i, convert_date)

        return rdr

    def read_raw(self, file):
        return etl.fromcsv(
            file,
            encoding=getattr(self, "file_encoding", None),
            delimiter=getattr(self, "csv_delimiter", ","),
        )

    def skip_until_main_table(self, rdr, col_labels=None):
        """Skip csv lines until the header line is found."""
        # TODO: convert this into an 'extract_table()' method that handles the tail as well
        if not col_labels:
            if hasattr(self.options, "column_labels_line"):
                col_labels = self.options.column_labels_line.replace('"', "").split(
                    getattr(self, "csv_delimiter", ",")
                )
            else:
                return rdr
        skip = None
        for n, r in enumerate(rdr):
            # We only check if each element in col_labels shows up in the line in the file, and not
            # the other way around. This allows additional fields to show up anywhere, case the csv
            # format changes
            if all(i in list(r) for i in col_labels):
                skip = n
        if skip is None:
            print("Error: expected columns not found:")
            print(col_labels)
            sys.exit(1)
        return rdr.skip(skip)

    def extract_table_with_header(self, rdr, col_labels=None):
        rdr = self.skip_until_main_table(rdr, col_labels)
        nrows = len(rdr)
        for n, r in enumerate(rdr):
            if not r or all(i == "" for i in r):
                # blank line, terminate
                nrows = n - 1
                break
        rdr = rdr.head(nrows)
        return rdr

    def skip_until_row_contains(self, rdr, value):
        start = None
        for n, r in enumerate(rdr):
            if value in r[0]:
                start = n
        if start is None:
            print(f'Error: table is not as expected. "{value}" row not found.')
            sys.exit(1)
        return rdr.rowslice(start, len(rdr))

    def read_file(self, file):
        print('aaaa')
        if not getattr(self, "file_read_done", False):
            print('bbbb')
            # read file
            rdr = self.read_raw(file)
            rdr = self.prepare_raw_file(rdr)

            # extract main table
            rdr = rdr.skip(getattr(self, "skip_head_rows", 0))  # chop unwanted header rows
            rdr = rdr.head(
                len(rdr) - getattr(self, "skip_tail_rows", 0) - 1
            )  # chop unwanted footer rows
            rdr = self.extract_table_with_header(rdr)
            if hasattr(self.options, "skip_comments"):
                rdr = rdr.skipcomments(self.options.skip_comments)
            rdr = rdr.rowslice(getattr(self, "skip_data_rows", 0), None)
            print(rdr)
            rdr = self.options.transformation_cb(rdr)
            print(rdr)

            # process table
            rdr = rdr.rename(self.options.header_map)
            rdr = self.convert_columns(rdr)
            rdr = self.fix_column_names(rdr)
            rdr = self.prepare_processed_table(rdr)
            self.rdr = rdr
            self.ifile = file
            self.file_read_done = True

    def get_transactions(self):
        for ot in self.rdr.namedtuples():
            if self.skip_transaction(ot):
                continue
            yield ot

    # TOOD: custom, overridable
    def skip_transaction(self, row):
        return getattr(row, "type", "NO_TYPE") in self.options.skip_transaction_types

    def get_balance_assertion_date(self):
        """
        We add an additional day to get_max_transaction_date(), since Beancount balance
        assertions are defined to occur on the beginning of the assertion date.
        """
        if not self.get_max_transaction_date():
            return None
            
        return self.get_max_transaction_date() + datetime.timedelta(days=1)

    def get_max_transaction_date(self):
        try:
            # date = self.ofx_account.statement.end_date.date() # this is the date of ofx download
            # we find the last transaction's date. If we use the ofx download date (if our source is ofx), we
            # could end up with a gap in time between the last transaction's date and balance assertion.
            # Pending (but not yet downloaded) transactions in this gap will get downloaded the next time we
            # do a download in the future, and cause the balance assertions to be invalid.

            # TODO: clean this up. this probably suffices:
            # return max(ot.date for ot in self.get_transactions()).date()
            date = max(
                ot.tradeDate if hasattr(ot, "tradeDate") else ot.date
                for ot in self.get_transactions()
            ).date()
        except Exception as err:
            #print("ERROR: no end_date. SKIPPING input.")
            #traceback.print_tb(err.__traceback__)
            return None

        return date

    def get_row_by_label(self, file, label):
        """Return a row from file where the first cell (column) matches label. This is a common
        operation in csv files, and is thus provided here as a utility. Eg:
           "Account Statement:,123456,EUR"
        """
        # Read from scratch, as we don't want to throw away headers or footers, which is where our
        # label is likely to be found
        rdr = self.read_raw(file)
        rdr = self.prepare_raw_file(rdr)
        return rdr.select(lambda r: r[0] == label)[1]
