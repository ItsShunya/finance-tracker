"""Importer module for Paypal transactions.

Parses Paypal CSV exports into Beancount-compatible data using a configured
CSVReader with transformation rules and header mappings.
"""

from collections.abc import Iterator
from typing import Any

from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BalanceStatement, BankingImporter


class PaypalImporter(BankingImporter):
    """Paypal transaction importer.

    This class configures a CSVReader to process CSV exports from
    Paypal accounts into Beancount-compatible transactions and balances.

    Attributes:
        reader (CSVReader): The configured CSV reader for Paypal data.
    """

    IMPORTER_NAME: str = "Paypal"
    reader: CSVReader

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the Paypal importer with configuration.

        Args:
            config: Dictionary of configuration options.
        """
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line=(
                '"Date","Time","Time Zone","Description","Currency",'
                '"Gross ","Fee ","Net","Balance","Transaction ID",'
                '"From Email Address","Name","Bank Name","Bank Account",'
                '"Shipping and Handling Amount","Sales Tax","Invoice ID",'
                '"Reference Txn ID"'
            ),
            date_format="%d/%m/%Y",
            skip_comments="# ",
            header_map={
                "Date": "date",
                "From Email Address": "checknum",
                "Currency": "currency",
                "Description": "type",
            },
            skip_transaction_types=[
                "General Authorization - Pending",
                "General Authorization - Completed",
            ],
            transaction_type_map={
                "Website Payment": "payment",
                "PreApproved Payment Bill User Payment": "payment",
                "Express Checkout Payment": "payment",
            },
            transformation_cb=self.transformations,
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr: Any) -> Any:
        """Apply transformations to raw CSV data.

        Adds computed fields: amount, balance, payee, and memo.

        Args:
            rdr: PETL table representing the parsed CSV rows.

        Returns:
            Transformed PETL table object.
        """
        rdr = rdr.addfield(
            "amount",
            lambda x: x["Net"].replace(",", "."),
        )
        rdr = rdr.addfield(
            "balance",
            lambda x: x["Balance"].replace(",", "."),
        )
        rdr = rdr.addfield(
            "payee",
            lambda x: f"{x['Description']}: {x['Name']}",
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
