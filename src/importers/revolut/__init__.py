"""Importer module for Revolut bank transactions.

This importer processes CSV exports from Revolut using a CSVReader
configured with transformation rules and header mappings. It produces
Beancount-compatible transactions and balance statements.
"""

from collections.abc import Iterator
from typing import Any

from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BalanceStatement, BankingImporter


class RevolutImporter(BankingImporter):
    """Revolut transaction importer.

    This class configures a CSVReader with specific settings to parse
    and transform CSV exports from Revolut accounts.

    Attributes:
        reader (CSVReader): The configured CSV reader for Revolut data.
    """

    IMPORTER_NAME: str = "Revolut"
    reader: CSVReader

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the Revolut importer with configuration.

        Args:
            config: Dictionary of configuration options.
        """
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line=(
                "Type,Product,Started Date,Completed Date,"
                "Description,Amount,Fee,Currency,State,Balance"
            ),
            date_format="%Y-%m-%d %H:%M:%S",
            skip_comments="# ",
            header_map={
                "Started Date": "date",
                "Currency": "currency",
                "Type": "type",
                "Description": "payee",
                "Balance": "balance",
            },
            skip_transaction_types=[],
            transaction_type_map={
                "TOPUP": "payment",
                "CARD_PAYMENT": "payment",
                "TRANSFER": "payment",
            },
            transformation_cb=self.transformations,
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr: Any) -> Any:
        """Apply transformations to raw CSV data.

        Adds computed 'amount' and empty 'memo' fields to each row.

        Args:
            rdr: Raw PETL table object.

        Returns:
            Transformed PETL table object.
        """
        rdr = rdr.addfield(
            "amount",
            lambda x: f"{float(x['Amount']) - float(x['Fee']):.2f}",
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(
        self, file: str | None = None
    ) -> Iterator[BalanceStatement]:
        """Yield a balance statement from the first CSV row.

        The balance corresponds to the earliest transaction date found.

        Args:
            file: Unused. Included for interface compatibility.

        Yields:
            A single BalanceStatement instance.
        """
        date = self.reader.get_balance_assertion_date()
        if date:
            first = self.reader.rdr.namedtuples()[0]
            yield BalanceStatement(date, first.balance, first.currency)
