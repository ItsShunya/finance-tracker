"""Importer module for Caixabank OFX files.

This module defines an Importer class that uses an OFXReader
to parse Caixabank OFX exports into Beancount-compatible transactions.
"""

from typing import Any, Sequence

from ...readers.ofx_reader import OFXReader
from ...transactions.banking import BalanceStatement, BankingImporter


class CaixabankImporter(BankingImporter):
    """Caixabank OFX importer.

    Uses OFXReader to parse Caixabank OFX files into transactions.

    Attributes:
        reader (OFXReader): The OFXReader instance used to read files.
    """

    IMPORTER_NAME: str = "Caixabank"
    reader: OFXReader

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the Caixabank importer with configuration.

        Args:
            config: Configuration dictionary.
        """
        super().__init__(config)
        self.reader = OFXReader(config)

    def get_balance_statement(
        self, file: str | None = None
    ) -> Sequence[BalanceStatement]:
        """Return an empty list of balance statements.

        Args:
            file: Optional file path (unused).

        Returns:
            An empty list, as balance statements are not provided.
        """
        return []
