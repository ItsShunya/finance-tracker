"""Common utilities to handle Beancount data such as Transactions."""

from beancount.core.amount import Amount
from beancount.core.data import Posting
from beancount.core.number import D, Decimal
from beancount.core.position import Cost

from src.util.errors import CustomException


def create_posting(
    entry: object,
    account: str,
    number: Decimal | str,
    currency: str,
    amount_number: Decimal | str = None,
    amount_currency: str = None,
    is_price: bool = False,
    is_cost: bool = False,
) -> Posting:
    """Create a simple posting on the entry.

    Args:
      entry: The entry instance to add the posting to.
      account: The account to use on the posting.
      number: The number to use in the posting's Amount.
      currency: The currency for the Amount.
      amount_number: The number to use for the posting's cost or price Amount.
      amount_currency: The currency for the cost or price Amount.
      is_price: A boolean indicating whether the posting is a price.
      is_cost: A boolean indicating whether the posting is a cost.

    Returns:
      An instance of Posting, and as a side-effect the entry has had its list
      of postings modified with the new Posting instance.
    """
    units = Amount(D(number), currency)
    amount = Amount(D(amount_number), amount_currency)

    # Determine whether to set cost or price if needed.
    cost = (
        Cost(amount.number, amount.currency, None, None) if is_cost else None
    )
    price = units if is_price else None

    posting = Posting(account, units, cost, price, None, None)

    if entry is not None:
        entry.postings.append(posting)

    return posting
