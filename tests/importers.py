"""Run beangulp's test or generate command on every importer.

Each importer is checked against the samples in tests/<importer>/:
    uv run python -m tests.importers test
    uv run python -m tests.importers generate
"""

import sys

from beangulp.testing import wrap

from config import IMPORTERS

failed = False
for name, importer in IMPORTERS.items():
    try:
        wrap(importer)([*sys.argv[1:], f"tests/{name}"])
    except SystemExit as e:
        failed |= bool(e.code)

sys.exit(failed)
