"""Base module for file readers used in transaction importers.

Defines the abstract `Reader` class which serves as a base for implementing
file readers that extract transactions and balances from various formats.
"""

import inspect
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

from beancount.core.data import Transaction


class Reader(ABC):
    """Abstract base class for reader implementations.

    Subclasses should implement file-specific logic to extract transactions
    and account information from bank or brokerage statements.

    Attributes:
        FILE_EXTS (list[str]): Supported file extensions (without dot).
        IMPORTER_NAME (str): Human-readable name of the importer.
        config (dict[str, Any]): Configuration dictionary for the reader.
        currency (str): Currency set by the reader or config.
        filename_pattern (str): Regex pattern to match filenames.
    """

    FILE_EXTS: ClassVar[list[str]] = [""]
    IMPORTER_NAME: ClassVar[str] = "UNKNOWN"

    config: dict[str, Any]
    currency: str
    filename_pattern: str

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the reader with a config dictionary.

        Args:
            config: Configuration dictionary containing settings like
                    currency, filename patterns, and accounts.
        """
        self.config = config
        self.currency = config.get("currency", "CURRENCY_NOT_CONFIGURED")
        self.filename_pattern = config.get("filename_pattern", "^*")

    # ────────────────────────────────
    # Required methods.
    # ────────────────────────────────

    @abstractmethod
    def get_transactions(self) -> list[Transaction]:
        """Return the list of parsed transactions.

        Returns:
            A list of parsed transaction objects.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    @abstractmethod
    def date(self, file: str | Path) -> Any:
        """Return the date associated with the file.

        Args:
            file: Path to the input file.

        Returns:
            A date object or string representing the file's date.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    @abstractmethod
    def read_file(self, file: str | Path) -> None:
        """Read and parse the file contents.

        Args:
            file: Path to the file.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    @abstractmethod
    def try_parse(self, file: str | Path) -> None:
        """Try to parse the file and return the result.

        Args:
            file: Path to the file to be parsed.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    # ────────────────────────────────
    # Beancount utils — do not override.
    # ────────────────────────────────

    def identify(self, file: str | Path) -> bool:
        """Determine whether this reader should process the given file.

        Args:
            file: Path to the input file.

        Returns:
            True if the reader can handle the file, False otherwise.
        """
        file_path = Path(file)

        if file_path.suffix.lower() not in (
            f".{ext.lower()}" for ext in self.FILE_EXTS
        ):
            return False

        if not re.match(self.filename_pattern, file_path.name):
            return False

        if not self.try_parse(file):
            return False

        return True

    def filename(self, file: str | Path) -> str:
        """Return the filename from a given path.

        Args:
            file: Path or filename.

        Returns:
            The name of the file (without parent directories).
        """
        return Path(file).name

    def account(self, file: str | Path) -> str:
        """Return the appropriate account for the given file.

        Args:
            file: Path to the input file.

        Returns:
            Account string.
        """
        if "filing_account" in self.config:
            return self.config["filing_account"]
        return self.config["main_account"]

    # ────────────────────────────────
    # Optional hooks — subclasses may override.
    # ────────────────────────────────

    def get_balance_statement(
        self, file: str | Path | None = None
    ) -> list[Any]:
        """Get balance statement extracted from the file.

        Args:
            file: Optional file path.

        Returns:
            A list representing the balance statement.
        """
        return []

    def get_balance_positions(self) -> list[Any]:
        """Get balance positions reported in the file.

        Returns:
            A list of balance positions.
        """
        return []

    def get_balance_assertion_date(self) -> Any:
        """Get the date of balance assertion, if available.

        Returns:
            The date of the assertion, or None.
        """
        return None

    def get_available_cash(
        self, settlement_fund_balance: float = 0.0
    ) -> float | None:
        """Get available cash, optionally including a settlement fund balance.

        Args:
            settlement_fund_balance: Amount in the settlement fund.

        Returns:
            Available cash value, or None if not applicable.
        """
        return None
