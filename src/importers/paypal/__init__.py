from src.readers import csv_reader
from src.transactions import banking


class Importer(banking.Importer, csv_reader.Importer):
    IMPORTER_NAME = "Paypal"

    def custom_init(self):
        self.max_rounding_error = 0.04
        self.filename_pattern_def = "^Paypal-.*.csv$"
        self.header_identifier = ""
        self.column_labels_line = '"Date","Time","Time Zone","Description","Currency","Gross ","Fee ","Net","Balance","Transaction ID","From Email Address","Name","Bank Name","Bank Account","Shipping and Handling Amount","Sales Tax","Invoice ID","Reference Txn ID"'
        self.date_format = "%d/%m/%Y"
        self.skip_comments = "# "

        self.header_map = {
            "Date":                 "date",
            "From Email Address":   "checknum",
            #"Name":                 "payee",
            "Currency":             "currency",
            "Description":          "type",
        }

        self.skip_transaction_types = [
            "General Authorization - Pending",
            "General Authorization - Completed",
        ]

        self.transaction_type_map = {
            'Website Payment':      'payment',
            'PreApproved Payment Bill User Payment':    'payment',
            'Express Checkout Payment': 'payment',
        }


    def prepare_table(self, rdr):
        # TO-DO: Simplify these fields. e.g. addfieldS()
        rdr = rdr.addfield(
            "amount",
            lambda x: x["Net"].replace(',', '.')
        )
        rdr = rdr.addfield(
            "balance",
            lambda x: x["Balance"].replace(',', '.')
        )
        rdr = rdr.addfield(
            "payee",
            lambda x: f'{x["Description"]}: {x["Name"]}' 
        )
        rdr = rdr.addfield("memo", lambda x: "")
        return rdr

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""

        date = self.get_balance_assertion_date()
        if date:
            yield banking.Balance(date, self.rdr.namedtuples()[0].balance, self.rdr.namedtuples()[0].currency)
