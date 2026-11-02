![finance-tracker](https://socialify.git.ci/ItsShunya/finance-tracker/image?font=Inter&logo=https%3A%2F%2Fbeancount.github.io%2Ffava%2F_static%2Flogo.png&name=1&owner=1&pattern=Plus&theme=Light)

This is the setup I use to keep track of my personal finances: transactions, savings and investments. It has the [Beancount](https://beancount.github.io/) importers for my banks, and [Fava](https://beancount.github.io/fava/) with a couple of custom dashboards to look at everything.

The importers are based on [redstreet's importers](https://github.com/redstreet/beancount_reds_importers). I didn't need most of their features and wanted to customize them, so instead of using them as a dependency I reused the parts I needed.

It uses Beancount v3 and won't work with v1 or v2.

Feel free to fork it. [docs/getting-started.md](docs/getting-started.md) explains how to adapt it to your own banks and ledger.

## Structure

It follows the layout recommended by beangulp: importers, ledger and documents are kept apart. The importers are public, the ledger and documents are private submodules.

- `config.py`: entry point. Sets up one importer per account and runs beangulp.
- `src/`
  - `importers/`: one importer per bank (CaixaBank, Paypal, Revolut, N26).
  - `readers/`: generic CSV and OFX readers the importers build on.
  - `transactions/`: turns the parsed rows into Beancount transactions and balances.
  - `util/`: small helpers.
- `ledger/`: the Beancount ledger, split by bank and year.
- `documents/`: the statements downloaded from each bank, one file per month.
- `dashboards/`: the Fava dashboards.

## Setup

You need Python 3.13+. Clone it and install the dependencies with [uv](https://docs.astral.sh/uv/):

```bash
git clone --recurse-submodules https://github.com/ItsShunya/Personal-Finances.git
cd Personal-Finances
uv sync
```

uv downloads Python 3.13 if you don't have it. If you use Nix, `nix develop` gives you Python, uv, make, git and ssh, and makes uv use that Python instead:

```bash
nix develop
uv sync
```

> [!NOTE]
> Both share the same `.venv`, so stick to one of them. A `.venv` made inside `nix develop` won't work outside of it, because packages like numpy need the libraries the Nix shell provides.

## Usage

Everything goes through `make`; run it without arguments to see the targets. At the start of every month:

1. Download last month's statements into `documents/<Bank>/`, named `YYYY-MM-<Bank>.<ext>`.
2. CaixaBank only exports Norma 43 (CSB43), so convert it to OFX:
   ```bash
   make caixabank-ofx IN=~/Downloads/<file>
   ```
3. Extract the transactions:
   ```bash
   make extract
   ```
   This writes `ledger/<Bank>/<year>/<month>-<bank>.bean` for every bank with a document. Banks with no document, or with a `.bean` already there, are skipped, so it never overwrites a month you've already reviewed. Use `MONTH=YYYY-MM` for another month, or `make extract-<bank>` for a single bank.
4. Review the new files. [smart_importer](https://github.com/beancount/smart_importer) fills in the categories and payees based on the existing ledger, but it gets some wrong. Then include the months in the year's file.
5. Check the ledger and look at it locally:
   ```bash
   make check
   make fava   # http://localhost:5000
   ```
6. Commit in `ledger/` and `documents/`, bump the submodules here and `make deploy`.

`make identify` is handy when a new file isn't picked up: it shows which importer matches each document.

## Dashboards

`dashboards/` has two [fava-dashboards](https://github.com/andreasgerstmayr/fava-dashboards), shown under *Dashboards* in Fava:

- **Overview**: net worth, assets and liabilities over time, income vs. expenses, monthly P&L, savings rate and expenses by category.
- **Monthly**: where the money went in a single month, compared with previous months, plus top payees, largest expenses and budgets. It shows the latest month by default; use the time filter or the links at the top to pick another one.

`dashboards.yaml` has the layout and `dashboards.js` the queries and charts. Settings like the asset groups are at the top of `dashboards.js`.

The extension is enabled in the ledger with:

```
1998-01-01 custom "fava-extension" "fava_dashboards" "{
  'config': '../dashboards/dashboards.yaml'
}"
```

## Deployment

Fava runs in Docker on my home server. Pushing to the `release` branch builds the image and publishes it to `ghcr.io/itsshunya/personal-finances`. It serves `/data/main.bean` on port 5000.

The ledger isn't in the image. Copy `local.mk.example` to `local.mk`, set the server, and `make deploy` copies the committed ledger and dashboards into the container. Fava reloads them on its own. `make restart` and `make shell` are there too.

## Contributing

This is mostly for my own use, but tips and suggestions are welcome. Open an issue if you have any.

## License

[Apache-2.0](./LICENSE). Use and modify it as you like, but please give credit.

## Acknowledgments

Thanks to the Beancount community, and to *redstreet* for the importers this started from.
