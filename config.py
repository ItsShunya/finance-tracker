import os, sys

# beancount doesn't run from this directory
sys.path.append(os.path.dirname(__file__))

# importers located in the importers directory
from importers import paypal, caixabank

# Setting this variable provides a list of importer instances.
CONFIG = [
    paypal.PaypalImporter('Assets:Online:Paypal:Checking'),
    caixabank.CaixaBankImporter('Assets:EU:CaixaBank:Checking')
]

# Override the header on extracted text (if desired).
#extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'