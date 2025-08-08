"""Generic banking importer for beancount."""

import itertools
from abc import ABC, abstractmethod
from typing import NamedTuple

from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.data import Balance, Transaction, new_metadata
from beangulp import Importer as BaseImporter

from src.transactions.common import create_posting
from src.transactions.transaction_builder import TransactionBuilder


class BalanceStatement(NamedTuple):
    """Balance Statement representation.

    Attributes:
        date: Full timestamp of the statement.
        amount: Number of the statement.
        currency: String representation of the currency.
    """

    date: str
    amount: int
    currency: str


class BankingImporter(BaseImporter, TransactionBuilder):
    """Beangulp Importer abstract class for handling banking transactions.

    This class is non-instantiable and must be used to inherit from when
    creating a top level importer for any banking institution. The child
    importer must implement the `get_balance_statement()` method.

    Attributes:
        config: Dictionary containing custom settings of the importer.
    """

    def __init__(self, config: dict) -> None:
        """Initialize the banking importer with a specific set of options.

        Args:
            config: Dictionary of configurable options.
        """
        self.config = config
        self.reader_ready = False

        # For overriding in custom_init()
        # TODO: move to getters (dataclass?)
        self.get_payee = lambda ot: ot.payee
        self.get_narration = lambda ot: ot.payee

        # REQUIRED_CONFIG = {
        #     'account_number'   : 'account number',
        #     'main_account'     : 'destination of import',
        # }

    @abstractmethod
    def get_balance_statement(self, file: str = None):
        """Abstract method for retrieving balance statement.

        Args:
            file: Path to the transactions file in use.
        """
        pass

    def account(self, file: str) -> str:
        """Return the main account for the importer, handled by the reader.

        Args:
            file: Path to the transactions file in use.
        Returns:
            The main account for the importer.
        """
        return self.reader.account(file)

    def identify(self, file: str) -> bool:
        """Return if the reader is able to parse the given file.

        Args:
            file: Path to the transactions file in use.
        Returns:
            True if the reader is able to parse the file.
        """
        return self.reader.identify(file)

    def initialize(self, file: str) -> None:
        """Initialize the reader.

        TODO: Move to reader's __init__.

        Args:
            file: Path to the transactions file in use.
        """
        self.reader.initialize_reader(file)

    def match_account_number(
        self, file_account: str, config_account: str
    ) -> bool:
        """Return true if the given account matches the expected.

        We many not want to store entire credit card numbers in our config. Or
        a given file may not contain the full account number. Override this
        method to handle these cases.

        Args:
            file_account: Account found on the file.
            config_account: Account configured in the settings.
        Returns:
            True if both accounts match.
        """
        return file_account.endswith(config_account)

    def get_main_account(self, ot: Transaction) -> str:
        """Return the main account of the importer.

        Args:
            ot: Transaction to check.
        Returns:
            A string representing the main account.
        """
        return self.config["main_account"]

    def get_target_account(self, ot: Transaction) -> str:
        """Return the target account of the importer.

        Args:
            ot: Transaction to check.
        Returns:
            A string representing the target account.
        """
        return self.config.get("target_account")

    def extract_balance(self, file: str, counter: int) -> list[Transaction]:
        """Extract the Balance from the file.

        Args:
            file: Path to the transactions file in use.
            counter: Integer
        Returns:
            List of entries containing balance.
        """
        entries = []

        for bal in self.get_balance_statement(file=file):
            if bal:
                metadata = new_metadata(file, next(counter))
                metadata.update(self.build_metadata(file, metatype="balance"))
                balance_entry = Balance(
                    metadata,
                    bal.date,
                    self.config["main_account"],
                    Amount(bal.amount, self.get_currency(bal)),
                    None,
                    None,
                )
                entries.append(balance_entry)
        return entries

    def get_currency(self, ot: Transaction) -> str:
        """Return the currency used in the given Transaction.

        Args:
            ot: Transaction to check.
        Returns:
            A string representing the currency.
        """
        try:
            return ot.currency
        except AttributeError:
            return self.reader.currency

    def extract(
        self, file: str, existing_entries: list[Transaction] = None
    ) -> list[Transaction]:
        """Extract the entries from the given file.

        Args:
            file: Path to the transactions file in use.
            existing_entries: List of previous existing entries.
        Returns
            Number of new entries.
        """
        self.initialize(file)
        counter = itertools.count()
        new_entries = []

        self.reader.read_file(file)
        for ot in self.reader.get_transactions():
            if self.skip_transaction(ot):
                continue
            metadata = new_metadata(file, next(counter))
            # metadata['type'] = ot.type # Optional metadata, debugging #TODO
            metadata.update(
                self.build_metadata(
                    file, metatype="transaction", data={"transaction": ot}
                )
            )

            # With Beancount the grammar is (payee, narration). payee is always
            # optional, narration is mandatory. This is a bit unintuitive and
            # smart_importer relies on narration, so it is important to keep
            # the order unchanged in the call below.

            # Build transaction entry
            entry = Transaction(
                meta=metadata,
                date=ot.date.date(),
                flag=flags.FLAG_OKAY,
                # payee and narration are switched. See the preceding note
                payee=self.get_narration(ot),
                narration=self.get_payee(ot),
                tags=self.get_tags(ot),
                links=self.get_links(ot),
                postings=[],
            )

            main_account = self.get_main_account(ot)

            create_posting(
                entry,
                main_account,
                ot.amount,
                self.get_currency(ot),
                amount_number=ot.foreign_amount
                if hasattr(ot, "foreign_amount")
                else None,
                amount_currency=ot.foreign_currency
                if hasattr(ot, "foreign_currency")
                else None,
            )

            # smart_importer can fill this in if the importer doesn't override
            target_acct = self.get_target_account(ot)
            if target_acct:
                create_posting(entry, target_acct, None, None)

            self.add_custom_postings(entry, ot)
            new_entries.append(entry)

        new_entries += self.extract_balance(file, counter)

        return new_entries
