from src.readers import ofx_reader
from src.transactions import banking


class Importer(banking.Importer, ofx_reader.Importer):
    IMPORTER_NAME = "Caixabank"

    def custom_init(self):
        if not self.custom_init_run:
            self.max_rounding_error = 0.04
            self.filename_pattern_def = "^Caixabank-.*.ofx$"
            self.custom_init_run = True
