import sys
from os import path

import beangulp
from smart_importer import PredictPostings, PredictPayees

# beancount doesn't run from this directory
sys.path.insert(0, path.join(path.dirname(__file__)))

# importers located in the importers directory
from src.importers import caixabank, paypal, revolut, n26

# Setting this variable provides a list of importer instances.
CONFIG = [
    PredictPostings().wrap(
        PredictPayees().wrap(
            caixabank.Importer(
                {
                    "main_account": "Assets:EU:CaixaBank:Checking",
                    "filename_pattern": "^Caixabank-",
                    "account_number": "0101278127",
                }
            )
        )
    ),

    PredictPostings().wrap(
        PredictPayees().wrap(
            paypal.Importer(
                {
                    "main_account": "Assets:Online:Paypal:Checking",
                    "filename_pattern": "^Paypal-",
                }
            )
        )
    ),

    PredictPostings().wrap(
        PredictPayees().wrap(
            revolut.Importer(
                {
                    "main_account": "Assets:EU:Revolut:Checking",
                    "filename_pattern": "^Revolut-",
                }
            )
        )
    ),

    PredictPostings().wrap(
        PredictPayees().wrap(
            n26.Importer(
                {
                    "main_account": "Assets:EU:N26:Checking",
                    "filename_pattern": "^N26-",
                }
            )
        )
    ),
]

HOOKS = [
]

# Override the header on extracted text (if desired).
#extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'


if __name__ == "__main__":
    ingest = beangulp.Ingest(CONFIG, HOOKS)
    ingest()
