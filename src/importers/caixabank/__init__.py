from src.readers.ofx_reader import OFXReader
from src.transactions.banking import BankingImporter

class Importer(BankingImporter):
    IMPORTER_NAME = "Caixabank"

    def __init__(self, config):
        super().__init__(config)
        self.reader = OFXReader()

    def account(self):
        return self.reader.account()

    def identify(self, file):
        return self.reader.identify(file)
