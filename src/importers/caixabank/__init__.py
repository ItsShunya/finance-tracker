from src.readers.ofx_reader import OFXReader
from src.transactions.banking import BankingImporter


class Importer(BankingImporter):
    IMPORTER_NAME: str = "Caixabank"

    def __init__(self, config):
        super().__init__(config)
        self.reader = OFXReader(config)

    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""
        return []
