"""Base module for transaction builders.

This module provides the abstract `TransactionBuilder` class, which serves as a
base for custom transaction importers such as those for investment, banking,
and paycheck sources. Subclasses can override its methods to define custom
behavior for tagging, linking, skipping, and transforming transactions.
"""

from abc import ABC
from typing import Any

from beancount.core import data
from beancount.core.data import EMPTY_SET, Meta, Transaction


class TransactionBuilder(ABC):
    """Base class for building or transforming transactions in custom importers.

    Subclasses can override the methods defined here to apply custom behavior
    when importing financial transactions, such as filtering entries, tagging,
    linking, and adding metadata or postings.

    Attributes:
        config (dict[str, Any]): A configuration dictionary typically loaded
            by the subclass, used to drive substitution and customization.
    """

    config: dict[str, Any]

    def skip_transaction(self, ot: Transaction) -> bool:
        """Determine whether a transaction should be skipped.

        Args:
            ot: The original transaction object.

        Returns:
            True if the transaction should be skipped, False otherwise.
        """
        return False

    def get_tags(self, ot: Transaction | None = None) -> list[str]:
        """Return a set of tags to attach to a transaction.

        Args:
            ot: Optional original transaction object.

        Returns:
            A set of tags, defaulting to EMPTY_SET.
        """
        return EMPTY_SET

    def get_links(self, ot: Transaction | None = None) -> list[str]:
        """Return a set of links to attach to a transaction.

        Args:
            ot: Optional original transaction object.

        Returns:
            A set of links, defaulting to EMPTY_SET.
        """
        return EMPTY_SET

    @staticmethod
    def remove_empty_subaccounts(acct: str) -> str:
        """Clean account names by removing empty subaccount parts.

        Args:
            acct: A colon-separated account, possibly with empty segments.

        Returns:
            A cleaned account string without empty segments.

        Example:
            'Assets:Foo::Bar' -> 'Assets:Foo:Bar'
        """
        return ":".join(x for x in acct.split(":") if x)

    def set_config_variables(self, substs: dict[str, str]) -> None:
        """Replace placeholders in the config dict using given substitutions.

        Args:
            substs: A dictionary of key-value substitutions. Keys should match
                the placeholders in config strings (e.g., '{currency}').

        Example:
            substs = {
                'currency': 'USD',
                'ticker': '{ticker}',
                'source401k': '{source401k}',
            }
        """
        self.config = {
            k: v.format(**substs) if isinstance(v, str) else v
            for k, v in self.config.items()
        }

        # Derive filing_account if not explicitly defined
        if "filing_account" not in self.config:
            kwargs = {k: "" for k in substs}
            filing_account = self.config["main_account"].format(**kwargs)
            self.config["filing_account"] = self.remove_empty_subaccounts(
                filing_account
            )

    def add_custom_postings(self, entry: Transaction, ot: Transaction) -> None:
        """Add additional postings to a transaction entry.

        This is a hook for importers to inject extra postings as needed.

        Args:
            entry: The beancount transaction entry to modify.
            ot: The original transaction object.
        """
        pass

    def build_metadata(
        self,
        file: str,
        metatype: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> Meta:
        """Build a metadata dictionary to attach to a directive.

        Can be used to add file-level or context-aware metadata to each
        beancount entry (e.g., transactions, balances, etc.).

        Args:
            file: The filename being processed.
            metatype: Optional type of directive ('transaction', 'balance').
            data: Optional dictionary with additional context data.

        Returns:
            A metadata dictionary to attach to the entry.
        """
        data = data or {}

        if self.config.get("emit_filing_account_metadata", True) is not False:
            acct = self.config.get(
                "filing_account", self.config.get("main_account", None)
            )
            return {"filing_account": acct}

        return {}
