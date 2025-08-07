import re
from abc import ABC, abstractmethod
from pathlib import Path


class Reader(ABC):
    FILE_EXTS = [""]
    IMPORTER_NAME = "UNKNOWN"

    def __init__(self, config):
        self.config = config

    def identify(self, file):
        file_path = Path(file)

        if file_path.suffix.lower() not in (
            f".{ext.lower()}" for ext in self.FILE_EXTS
        ):
            return False

        self.filename_pattern = self.config.get("filename_pattern", "^*")

        if not re.match(self.filename_pattern, file_path.name):
            return False

        self.currency = self.config.get("currency", "CURRENCY_NOT_CONFIGURED")
        self.initialize_reader(file)
        return self.reader_ready

    def set_currency(self):
        """For overriding"""
        self.currency = self.config.get("currency", "CURRENCY_NOT_CONFIGURED")

    def filename(self, file):
        return Path(file).name

    def account(self, file):
        import inspect

        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        if any("predictor" in i.filename for i in calframe):
            if "smart_importer_hack" in self.config:
                return self.config["smart_importer_hack"]

        # Otherwise handle a typical bean-file call
        self.initialize_reader(file)
        if "filing_account" in self.config:
            return self.config["filing_account"]
        return self.config["main_account"]

    def get_balance_statement(self, file=None):
        return []

    def get_balance_positions(self):
        return []

    def get_balance_assertion_date(self):
        return None

    def get_available_cash(self, settlement_fund_balance=0):
        return None

    @abstractmethod
    def get_transactions(self):
        raise NotImplementedError

    @abstractmethod
    def date(self, file):
        raise NotImplementedError

    @abstractmethod
    def read_file(self, file):
        raise NotImplementedError
