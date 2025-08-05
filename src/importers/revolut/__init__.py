from src.readers.csv_reader import CSVReader
from src.transactions.banking import BankingImporter, BalanceStatement


class Importer(BankingImporter, CSVReader):
    IMPORTER_NAME = "Revolut"

    def custom_init(self):
        self.max_rounding_error = 0.04
        self.header_identifier = ""
        self.column_labels_line = 'Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance'
        self.date_format = "%Y-%m-%d %H:%M:%S"
        self.skip_comments = "# "

        self.header_map = {
            "Started Date":         "date",
            "Currency":             "currency",
            "Type":                 "type",
            "Description":          "payee",
            "Balance":              "balance",
        }

        self.skip_transaction_types = []

        self.transaction_type_map = {
            'TOPUP':           'payment',
            'CARD_PAYMENT':    'payment',
            'TRANSFER':        'payment',
        }


    def prepare_table(self, rdr):
        rdr = rdr.addfield(
            "amount",
            lambda x: f"{float(x['Amount']) - float(x['Fee']):.2f}"
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""

        date = self.get_balance_assertion_date()
        if date:
            yield BalanceStatement(date, self.rdr.namedtuples()[0].balance, self.rdr.namedtuples()[0].currency)
