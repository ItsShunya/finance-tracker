"""Main entrypoint for using beancount. This is the Python file to be executed.

Example usage (with uv):
- `uv run config.py extract -e ./ledger/main.bean ./documents/`
- `uv run config.py identify ./documents`
"""

import sys
from os import path

import beangulp
from smart_importer import PredictPayees, PredictPostings

from src.importers import caixabank, n26, paypal, revolut

# beancount doesn't run from this directory
sys.path.insert(0, path.join(path.dirname(__file__)))

# The tests in tests/ run these directly, without smart_importer.
IMPORTERS = {
    "caixabank": caixabank.CaixabankImporter(
        {
            "main_account": "Assets:EU:CaixaBank:Checking",
            "filename_pattern": r"^\d{4}-\d{2}-CaixaBank\.ofx$",
            "account_number": "0101278127",
        }
    ),
    "paypal": paypal.PaypalImporter(
        {
            "main_account": "Assets:Online:Paypal:Checking",
            "filename_pattern": r"^\d{4}-\d{2}-Paypal\.csv$",
        }
    ),
    "revolut": revolut.RevolutImporter(
        {
            "main_account": "Assets:EU:Revolut:Checking",
            "filename_pattern": r"^\d{4}-\d{2}-Revolut\.csv$",
        }
    ),
    "n26_checking": n26.N26Importer(
        {
            "main_account": "Assets:EU:N26:Checking",
            "filename_pattern": r"^\d{4}-\d{2}-N26_Checking\.csv$",
        }
    ),
    "n26_saving": n26.N26Importer(
        {
            "main_account": "Assets:EU:N26:Saving",
            "filename_pattern": r"^\d{4}-\d{2}-N26_Saving\.csv$",
        }
    ),
}

CONFIG = [
    PredictPostings().wrap(PredictPayees().wrap(importer))
    for importer in IMPORTERS.values()
]

HOOKS = []

# Override the header on extracted text (if desired).
# extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'


if __name__ == "__main__":
    ingest = beangulp.Ingest(CONFIG, HOOKS)
    ingest()
