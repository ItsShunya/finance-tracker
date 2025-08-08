"""Reader for parsing and extracting financial data from OFX/QFX files.

This module defines a class for reading OFX/QFX financial statement files and
extracting transactions, balances, positions, and balance assertion dates for
automated bookkeeping pipelines using Beancount.
"""

import datetime
import warnings
from collections import namedtuple
from io import StringIO
from pathlib import Path
from typing import Any, Generator

import ofxparse
from beancount.core.data import Transaction
from bs4 import BeautifulSoup
from bs4.builder import XMLParsedAsHTMLWarning

from .reader import Reader

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class OFXReader(Reader):
    """Reader for OFX and QFX statement files.

    Parses OFX files and extracts relevant financial information including
    transactions, positions, and balances. Designed to integrate with Beancount.

    Attributes:
        FILE_EXTS (list[str]): Supported file extensions for this reader.
        ofx (ofxparse.Ofx): Parsed OFX data.
        ofx_account (Any): Account extracted from parsed OFX data.
        currency (str): Currency code (e.g. 'USD') for the account.
    """

    FILE_EXTS: list[str] = ["ofx", "qfx"]
    ofx: Any
    ofx_account: Any
    currency: str

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the reader with configuration.

        Args:
            config: Configuration dictionary for the reader.
        """
        super().__init__(config)

    def try_parse(self, file: str) -> bool:
        """Parse the file and initialize the reader state.

        Args:
            file: Path to the OFX/QFX file.

        Returns:
            True if the file was successfully parsed, False otherwise.
        """
        self.ofx_account = None
        try:
            self.ofx = self.read_file(file)
        except ofxparse.OfxParserException:
            return False

        for acc in self.ofx.accounts:
            acc_num_field = getattr(self, "account_number_field", "account_id")
            if self.match_account_number(
                getattr(acc, acc_num_field),
                self.config["account_number"],
            ):
                self.ofx_account = acc

        self.currency = self.ofx_account.statement.currency.upper()
        return True

    def match_account_number(
        self, file_account: str, config_account: str
    ) -> bool:
        """Match the account number from file with the configured account.

        Args:
            file_account: Account number from the OFX file.
            config_account: Expected account number from configuration.

        Returns:
            True if the account numbers match.
        """
        return file_account == config_account

    def date(self, file: str) -> datetime.date | None:
        """Return the end date of the statement.

        Args:
            file: Path to the OFX/QFX file.

        Returns:
            Statement end date, or None if not available.
        """
        if not getattr(self, "ofx_account", None):
            self.try_parse(file)
        try:
            return self.ofx_account.statement.end_date
        except AttributeError:
            return None

    def read_file(self, file: str) -> Any:
        """Read and parse an OFX/QFX file.

        Args:
            file: Path to the OFX/QFX file.

        Returns:
            Parsed OFX object.
        """
        with open(file, "r", encoding="utf-8") as fh:
            sgml_content = fh.read()

        soup = BeautifulSoup(sgml_content, "html.parser")
        for tag in soup.find_all():
            if not tag.contents and not tag.attrs:
                tag.extract()

        file_like_object = StringIO(str(soup))
        return ofxparse.OfxParser.parse(file_like_object)

    def get_transactions(self) -> Generator[Transaction, None, None]:
        """Yield all transactions from the parsed statement.

        Returns:
            Generator of Transaction objects.
        """
        yield from self.ofx_account.statement.transactions

    def get_balance_statement(
        self, file: str | None = None
    ) -> Generator[Any, None, None]:
        """Yield account balance for balance assertions.

        Args:
            file: (Unused) Path to the file.

        Returns:
            Generator of namedtuples with balance data.
        """
        if not hasattr(self.ofx_account.statement, "balance"):
            return []

        date = self.get_balance_assertion_date()
        if date:
            Balance = namedtuple("Balance", ["date", "amount"])
            yield Balance(date, self.ofx_account.statement.balance)

    def get_balance_positions(self) -> Generator[Any, None, None]:
        """Yield current investment positions if present.

        Returns:
            Generator of position entries.
        """
        if not hasattr(self.ofx_account.statement, "positions"):
            return []
        yield from self.ofx_account.statement.positions

    def get_available_cash(
        self, settlement_fund_balance: float = 0
    ) -> float | None:
        """Compute available cash after subtracting settlement fund balance.

        Args:
            settlement_fund_balance: Amount to subtract from available cash.

        Returns:
            Available cash amount or None if unavailable.
        """
        available_cash = getattr(
            self.ofx_account.statement, "available_cash", None
        )
        if available_cash is not None:
            return available_cash - settlement_fund_balance
        return None

    def get_ofx_end_date(
        self, field: str = "end_date"
    ) -> datetime.date | None:
        """Return a date from the OFX file (default: statement end date).

        Args:
            field: Attribute name of the date field.

        Returns:
            Date with time stripped, or None if not found.
        """
        end_date = getattr(self.ofx_account.statement, field, None)
        return end_date.date() if end_date else None

    def get_smart_date(self) -> datetime.date | None:
        """Compute a smart balance assertion date with safety margin.

        Returns:
            A computed date with a safety margin for pending transactions.
        """
        fudge = self.config.get("balance_assertion_date_fudge", 2)

        dates = [
            self.get_ofx_end_date("end_date"),
            self.get_max_transaction_date(),
            self._fudged_date("available_balance_date", fudge),
            self._fudged_date("balance_date", fudge),
        ]

        if all(d is None for d in dates[:2]):
            return None

        def safe(x: datetime.date | None) -> datetime.date:
            return x if x else datetime.date.min

        return max(safe(d) for d in dates)

    def get_balance_assertion_date(self) -> datetime.date | None:
        """Determine the date to assert account balance.

        Returns:
            The balance assertion date with +1 day offset (Beancount standard).
        """
        strategy_map = {
            "smart": self.get_smart_date,
            "ofx_date": self.get_ofx_end_date,
            "last_transaction": self.get_max_transaction_date,
            "today": datetime.date.today,
        }

        strategy = self.config.get("balance_assertion_date_type", "smart")
        date = strategy_map[strategy]()
        return date + datetime.timedelta(days=1) if date else None

    def get_max_transaction_date(self) -> datetime.date | None:
        """Return the latest transaction date from the file.

        Returns:
            Latest transaction date or None if not found.
        """
        try:
            return max(
                ot.tradeDate if hasattr(ot, "tradeDate") else ot.date
                for ot in self.get_transactions()
            ).date()
        except (TypeError, ValueError):
            return None

    def _fudged_date(
        self, field: str, fudge_days: int
    ) -> datetime.date | None:
        """Get a date field from the statement and subtract fudge days.

        Args:
            field: The field name to fetch.
            fudge_days: Number of days to subtract.

        Returns:
            Adjusted date or None.
        """
        d = self.get_ofx_end_date(field)
        return d - datetime.timedelta(days=fudge_days) if d else None
