from src.readers.csv_reader import CSVReader
from src.transactions.banking import BankingImporter


class Importer(BankingImporter, CSVReader):
    IMPORTER_NAME = "N26"

    def custom_init(self):
        self.max_rounding_error = 0.04
        self.header_identifier = ""
        self.column_labels_line = '"Booking Date","Value Date","Partner Name","Partner Iban",Type,"Payment Reference","Account Name","Amount (EUR)","Original Amount","Original Currency","Exchange Rate"'
        self.date_format = "%Y-%m-%d"
        self.skip_comments = "# "

        self.header_map = {
            "Booking Date":         "date",
            #"Currency":             "currency",
            "Type":                 "type",
            "Payment Reference":    "payee",
            "Amount (EUR)":         "amounts",
        }

        self.skip_transaction_types = []

        self.transaction_type_map = {
            'Credit Transfer':           'payment',
            'Instant Savings':           'payment',
            'Debig Transfer':            'payment',
        }


    def prepare_table(self, rdr):
        rdr = rdr.addfield(
            "currency","EUR"
        )
        rdr = rdr.addfield(
            "amount",
            lambda x: f"{float(x['Amount (EUR)']):.2f}"
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr
