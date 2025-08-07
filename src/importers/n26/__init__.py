from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BankingImporter, BalanceStatement

class Importer(BankingImporter):
    IMPORTER_NAME = "N26"

    def __init__(self, config):
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line='"Booking Date","Value Date","Partner Name","Partner Iban",Type,"Payment Reference","Account Name","Amount (EUR)","Original Amount","Original Currency","Exchange Rate"',
            date_format="%Y-%m-%d",
            skip_comments="# ",
            header_map={
                "Booking Date":         "date",
                #"Currency":             "currency",
                "Type":                 "type",
                "Payment Reference":    "payee",
                "Amount (EUR)":         "amounts",
            },
            skip_transaction_types=[],
            transaction_type_map={
                'Credit Transfer':           'payment',
                'Instant Savings':           'payment',
                'Debig Transfer':            'payment',
            },
            transformation_cb=self.transformations
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr):
        rdr = rdr.addfield(
            "currency","EUR"
        )
        rdr = rdr.addfield(
            "amount",
            lambda x: f"{float(x['Amount (EUR)']):.2f}"
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""
        return []
