"""Importer module for N26 bank CSV files.

This module defines an Importer class to parse N26 CSV exports
into Beancount-compatible transactions and balances using a
configured CSVReader.
"""

from typing import Any

from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BalanceStatement, BankingImporter


class N26Importer(BankingImporter):
    """N26 bank CSV importer.

    Configures a CSVReader to parse N26 exported CSV files
    into Beancount transactions.

    Attributes:
        reader (CSVReader): Configured CSVReader instance.
    """

    IMPORTER_NAME: str = "N26"
    reader: CSVReader

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the N26 importer with configuration.

        Args:
            config: Configuration dictionary.
        """
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line=(
                '"Booking Date","Value Date","Partner Name","Partner Iban",'
                'Type,"Payment Reference","Account Name","Amount (EUR)",'
                '"Original Amount","Original Currency","Exchange Rate"'
            ),
            date_format="%Y-%m-%d",
            skip_comments="# ",
            header_map={
                "Booking Date": "date",
                # "Currency": "currency",  # Not used here
                "Type": "type",
                "Payment Reference": "payee",
                "Amount (EUR)": "amounts",
            },
            skip_transaction_types=[],
            transaction_type_map={
                "Credit Transfer": "payment",
                "Instant Savings": "payment",
                "Debig Transfer": "payment",
            },
            transformation_cb=self.transformations,
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr: Any) -> Any:
        """Apply transformations to the raw CSV data.

        Adds fields: currency, amount, and memo to each row.

        Args:
            rdr: PETL table of parsed CSV rows.

        Returns:
            Transformed PETL table.
        """
        rdr = rdr.addfield("currency", "EUR")
        rdr = rdr.addfield(
            "amount", lambda x: f"{float(x['Amount (EUR)']):.2f}"
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(
        self, file: str | None = None
    ) -> list[BalanceStatement]:
        """Return an empty balance statement list.

        Args:
            file: Optional file path (unused).

        Returns:
            Empty list, as balance statements are not provided.
        """
        return []
