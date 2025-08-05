"""Generic banking importer for beancount."""

import itertools
from collections import namedtuple

from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.data import Balance, Transaction, new_metadata
from beangulp import Importer as BaseImporter

from src.transactions.transaction_builder import TransactionBuilder
from src.transactions.common import create_posting

BalanceStatement = namedtuple('BalanceStatement', ['date', 'amount', 'currency'])

class BankingImporter(BaseImporter, TransactionBuilder):
    FLAG = flags.FLAG_OKAY

    def __init__(self, config):
        self.config = config
        self.reader_ready = False

        # For overriding in custom_init()
        self.get_payee = lambda ot: ot.payee
        self.get_narration = lambda ot: ot.memo

        # REQUIRED_CONFIG = {
        #     'account_number'   : 'account number',
        #     'main_account'     : 'destination of import',
        # }

    def initialize(self, file):
        if not hasattr(self, "file") or self.file != file:
            self.custom_init()
            self.initialize_reader(file)
            self.file = file

    def build_account_map(self):
        # TODO: Not needed for accounts using smart_importer; make this configurable
        # transaction types: {}
        # self.target_account_map = {
        #         "directdep": 'TODO',
        #         "credit":    'TODO',
        #         "debit":     'TODO',
        # }
        pass

    def match_account_number(self, file_account, config_account):
        return file_account.endswith(config_account)

    def custom_init(self):
        self.max_rounding_error = 0.04

    @staticmethod
    def fields_contain_data(ot, fields):
        return all(hasattr(ot, f) and getattr(ot, f) for f in fields)

    def get_main_account(self, ot):
        """Can be overridden by importer"""
        return self.config["main_account"]

    def get_target_account(self, ot):
        """Can be overridden by importer"""
        return self.config.get("target_account")

    # --------------------------------------------------------------------------------

    def extract_balance(self, file, counter):
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

    def extract_custom_entries(self, file, counter):
        """For custom importers to override"""
        return []

    def get_currency(self, ot):
        try:
            return ot.currency
        except AttributeError:
            return self.currency

    def extract(self, file, existing_entries=None):
        self.initialize(file)
        counter = itertools.count()
        new_entries = []

        self.read_file(file)
        for ot in self.get_transactions():
            if self.skip_transaction(ot):
                continue
            metadata = new_metadata(file, next(counter))
            # metadata['type'] = ot.type # Optional metadata, useful for debugging #TODO
            metadata.update(
                self.build_metadata(file, metatype="transaction", data={"transaction": ot})
            )

            # description fields: With OFX, ot.payee tends to be the "main" description field,
            # while ot.memo is optional
            #
            # With Beancount, the grammar is (payee, narration). payee is optional, narration is
            # mandatory. This is a bit unintuitive. In addition, smart_importer relies on
            # narration, so keeping the order unchanged in the call below is important.

            # Build transaction entry
            entry = Transaction(
                meta=metadata,
                date=ot.date.date(),
                flag=self.FLAG,
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
                amount_number = ot.foreign_amount if hasattr(ot, "foreign_amount") else None,
                amount_currency = ot.foreign_currency if hasattr(ot, "foreign_currency") else None,
            )

            # smart_importer can fill this in if the importer doesn't override self.get_target_acct()
            target_acct = self.get_target_account(ot)
            if target_acct:
                create_posting(entry, target_acct, None, None)

            self.add_custom_postings(entry, ot)
            new_entries.append(entry)

        new_entries += self.extract_balance(file, counter)
        new_entries += self.extract_custom_entries(file, counter)

        return new_entries
