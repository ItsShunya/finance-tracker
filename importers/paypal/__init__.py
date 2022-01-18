import email
from operator import contains
from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.position import Cost

from dateutil.parser import parse

from titlecase import titlecase

import csv
import os
import re

from utilities import strings

class PaypalImporter(importer.ImporterProtocol):
    def __init__(self, account, lastfour):
        self.account = account
        self.lastfour = lastfour

    def _check_common_names(self, name):
        if(name.contains("Uber")):
            return "Uber Eats"
        elif(name.contains("Telepizza")):
            return "Telepizza"
        return ""

    def _check_common_emails(self, email):
        if(email.contains("@uber.com")):
            return "Uber Eats"
        elif(email.contains("@telepizza.com")):
            return "Telepizza"
        return ""

    def _check_account(self, name, email, bank, desc):
        restaurants = ["uber", "telepizza"]
        games = ["terminal3", "cognosphere", "mihoyo"] # BDO, Genshin

        if("caixabank" in bank.lower()):
            return "Assets:EU:CaixaBank:Checking"
        if(any(r in name for r in restaurants) or any(r in email for r in restaurants)):
            return "Expenses:Food:Restaurant"
        elif(any(g in name for g in games) or any(g in email for g in games)):
            return "Expenses:Leisure:Games"
        elif(self._is_payment(desc)):
            return "Income:Sold"
        else:
            return "Expenses:Other"

    def _is_payment(self, desc):
        payment_descriptions = [ "Mobile Payment", "General Payment"]
        return any(p in desc for p in payment_descriptions)

    def identify(self, f):
        return re.match(r'^Paypal-.*\.CSV$', os.path.basename(f.name))

    def extract(self, f):
        entries = []
        with open(f.name, encoding="utf-8") as f:
            for index, row in enumerate(csv.DictReader(f)):
                trans_date = parse(row['Date'], dayfirst=True).date()
                trans_desc = titlecase(strings.remove_accents(row['Description']))
                trans_amt  = row['Net'].replace(',', '.')
                name = row['Name']
                email = row['From Email Address']
                bank = row['Bank Name']

                meta = data.new_metadata(f.name, index)

                txn = data.Transaction(
                    meta=meta,
                    date=trans_date,
                    flag=flags.FLAG_OKAY,
                    payee=trans_desc,
                    narration="",
                    tags=set(),
                    links=set(),
                    postings=[],
                )

                txn.postings.append(
                    data.Posting(self.account, amount.Amount(D(trans_amt),
                        row['Currency']), None, None, None, None)
                )

                if(name.__ne__("") or email.__ne__("") or bank.__ne__("")):
                    txn.postings.append(
                        data.Posting(self._check_account(name, email, bank, trans_desc), -amount.Amount(D(trans_amt),
                        row['Currency']), None, None, None, None)
                )
                
                entries.append(txn)

        return entries