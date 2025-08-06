from src.readers.csv_reader import CSVReader, CSVReaderOptions
from src.transactions.banking import BankingImporter, BalanceStatement

class Importer(BankingImporter):
    IMPORTER_NAME = "Paypal"

    def __init__(self, config):
        super().__init__(config)

        csv_options = CSVReaderOptions(
            max_rounding_error=0.04,
            header_identifier="",
            column_labels_line='"Date","Time","Time Zone","Description","Currency","Gross ","Fee ","Net","Balance","Transaction ID","From Email Address","Name","Bank Name","Bank Account","Shipping and Handling Amount","Sales Tax","Invoice ID","Reference Txn ID"',
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
            transformation_cb=self.transformations
        )

        self.reader = CSVReader(config, csv_options)

    def transformations(self, rdr):
        # TO-DO: Simplify these fields. e.g. addfieldS()
        print("prepare table")
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
        date = self.reader.get_balance_assertion_date()
        if date:
            yield BalanceStatement(date, self.reader.rdr.namedtuples()[0].balance, self.reader.rdr.namedtuples()[0].currency)
