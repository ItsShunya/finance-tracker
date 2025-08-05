from src.readers.ofx_reader import OFXReader
from src.transactions.banking import BankingImporter

class Importer(BankingImporter, OFXReader):
    IMPORTER_NAME = "Caixabank"

    def custom_init(self):
        self.max_rounding_error = 0.04
