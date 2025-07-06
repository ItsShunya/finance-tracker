import sys
from os import path

import beangulp

# beancount doesn't run from this directory
sys.path.insert(0, path.join(path.dirname(__file__)))

# importers located in the importers directory
from importers import caixabank

# Setting this variable provides a list of importer instances.
CONFIG = [
    #paypal.PaypalImporter('Assets:Online:Paypal:Checking'),
    caixabank.Importer(
        {
            "main_account": "Assets:EU:CaixaBank:Checking",
            "account_number": "0101278127",
        }
    )
]

# Override the header on extracted text (if desired).
#extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'


if __name__ == "__main__":
    ingest = beangulp.Ingest(CONFIG)
    ingest()
