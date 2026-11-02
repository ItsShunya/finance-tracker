# Getting started

How to turn this repo into your own tracker. You'll need some idea of how [Beancount](https://beancount.github.io/docs/) works.

## 1. Your own ledger and documents

`ledger/` and `documents/` are submodules pointing to my private repos, so you can't clone them. Replace them with your own (private) repos:

```bash
git submodule deinit -f ledger documents
git rm -f ledger documents
rm -rf .git/modules/ledger .git/modules/documents
git submodule add git@github.com:<you>/finance-ledger.git ledger
git submodule add git@github.com:<you>/finance-documents.git documents
```

Or just make them plain folders if you don't want separate repos.

## 2. Documents

One folder per bank, one file per month, named `YYYY-MM-<Bank>.<ext>`:

```
documents/
├── CaixaBank/2026-10-CaixaBank.ofx
├── Paypal/2026-10-Paypal.csv
├── Revolut/2026-10-Revolut.csv
└── N26/
    ├── Checking/2026-10-N26_Checking.csv
    └── Savings/2026-10-N26_Saving.csv
```

The importers only pick up files matching the `filename_pattern` in `config.py`, so keep the names consistent. For a whole year at once, `2025-Paypal.csv` works too.

## 3. Importers

`config.py` has one importer per account. For each one set:

- `main_account`: the Beancount account the transactions go to.
- `filename_pattern`: regex for its files in `documents/`.
- `account_number`: only CaixaBank, the account in the OFX file.

Remove the banks you don't use. For a new bank, copy the closest one in `src/importers/` (CSV or OFX) and change the column mapping. Then add it to `config.py`.

Each importer has an anonymized sample statement in `tests/<importer>/`, next to the `.beancount` file it should produce. `make test` checks them, and CI runs it too. For a new importer, add its key to `IMPORTERS` in `config.py`, drop a sample (fake names, amounts and IDs) in `tests/<key>/`, run `make test-generate` and review the output before committing it.

Each importer is wrapped with smart_importer's `PredictPostings` and `PredictPayees`, which guess the other side of each transaction from your existing ledger. On the first import there's nothing to learn from, so you'll fill in everything by hand.

## 4. Ledger

```
ledger/
├── main.bean        includes everything below
├── options.bean     title, currencies, fava options and plugins
├── accounts.bean    commodities and open directives
├── opening.bean     opening balances
├── budget.bean      fava budgets
└── Caixabank/
    └── 2026/
        ├── 2026-caixabank.bean      includes the months
        ├── 2026-01-caixabank.bean
        └── ...
```

`main.bean` only includes the settings files and each bank's year file:

```beancount
include "options.bean"
include "accounts.bean"
include "budget.bean"
include "opening.bean"

include "Caixabank/2026/2026-caixabank.bean"
include "Paypal/2026/2026-paypal.bean"
```

And each year file includes its months:

```beancount
include "2026-01-caixabank.bean"
include "2026-02-caixabank.bean"
```

In `options.bean`, change the title and currencies. Keep the `fava_dashboards` extension. The zero-sum plugin matches transfers between your own accounts: book both sides to `Assets:Zero-Sum-Accounts:Bank-Account-Transfers` and it pairs them up.

Every account you use must be opened in `accounts.bean`, or `make check` will complain.

## 5. Makefile

The paths for each bank are in the `extract-*` targets of the `Makefile`. If you add or remove a bank, change them there too.

Every month:

1. Download last month's statements into `documents/`, named as above.
2. CaixaBank only exports Norma 43, so convert it: `make caixabank-ofx IN=~/Downloads/<file>`.
3. `make extract`. Banks with no document, or with a `.bean` already there, are skipped. Use `MONTH=2026-10` for another month.
4. Go through the new `.bean` files and fix or add the categories. A transaction looks like this:

   ```beancount
   2026-10-04 * "WWW.AMAZON"
     Assets:EU:CaixaBank:Checking   -27.99 EUR
     Expenses:House:Furniture
   ```

   You can delete the `****` header line and the `filing_account` metadata if you like.
5. Add the months to the year files. In January, create the year file and include it in `main.bean`.
6. `make check` until it's clean, then `make fava` to look at it on http://localhost:5000.
7. Commit in `ledger/` and `documents/`, then commit the submodule bump here.

To redo a month, delete its `.bean` and run `make extract` again.

## 6. Server

The image is built and pushed to `ghcr.io/<you>/personal-finances` when you push to the `release` branch. It runs Fava on `/data/main.bean`, port 5000. Start it on your server:

```bash
docker run -d --name fava -p 5000:5000 --restart unless-stopped ghcr.io/<you>/personal-finances
```

Copy `local.mk.example` to `local.mk` and set `FAVA_SSH`. Then `make deploy` copies the committed ledger into `/data` and the dashboards into `/dashboards`. Fava reloads on its own.

The container doesn't keep the ledger when it's recreated, so run `make deploy` again after that.

## 7. Dashboards

The asset and liability groups at the top of `dashboards/dashboards.js` match my account names. Change the regexes to yours, and `pinnedCategories` to your main expense categories.
