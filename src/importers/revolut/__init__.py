from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BankingImporter, BalanceStatement

class Importer(BankingImporter):
    IMPORTER_NAME = "Revolut"

    def __init__(self, config):
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line='Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance',
            date_format="%Y-%m-%d %H:%M:%S",
            skip_comments="# ",
            header_map={
                "Started Date":         "date",
                "Currency":             "currency",
                "Type":                 "type",
                "Description":          "payee",
                "Balance":              "balance",
            },
            skip_transaction_types=[],
            transaction_type_map={
                'TOPUP':           'payment',
                'CARD_PAYMENT':    'payment',
                'TRANSFER':        'payment',
            },
            transformation_cb=self.transformations
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr):
        rdr = rdr.addfield(
            "amount",
            lambda x: f"{float(x['Amount']) - float(x['Fee']):.2f}"
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""
        date = self.reader.get_balance_assertion_date()
        if date:
            yield BalanceStatement(date, self.reader.rdr.namedtuples()[0].balance, self.reader.rdr.namedtuples()[0].currency)
