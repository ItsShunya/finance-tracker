# Server settings go in local.mk (see local.mk.example).
-include local.mk

FAVA_SSH ?= $(error FAVA_SSH is not set, see local.mk.example)
FAVA_CONTAINER ?= fava
REF ?= HEAD

# Month to import, defaults to last month.
MONTH ?= $(shell date -d "$$(date +%Y-%m-15) -1 month" +%Y-%m)
YEAR = $(word 1,$(subst -, ,$(MONTH)))

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

EXTRACT = uv run config.py extract -e ledger/main.bean

# $(call extract,document,bean): skips if the document is missing or the bean
# already exists, so reviewed files are never overwritten.
define extract
	@if [ ! -e $(1) ]; then echo "skip: no $(1)"; \
	elif [ -e $(2) ]; then echo "skip: $(2) already exists"; \
	else mkdir -p $(dir $(2)) && $(EXTRACT) -o $(2) $(1); fi
endef

.PHONY: help caixabank-ofx identify extract extract-caixabank extract-paypal \
	extract-revolut extract-n26 check fava deploy restart shell

help:
	@echo "make caixabank-ofx IN=file  convert a CaixaBank CSB43 export to OFX"
	@echo "make identify               check the importers find the documents"
	@echo "make extract                extract every bank for MONTH (default $(MONTH))"
	@echo "make extract-<bank>         caixabank, paypal, revolut or n26"
	@echo "make check                  run bean-check on the ledger"
	@echo "make fava                   run fava locally"
	@echo "make deploy [REF=branch]    copy the committed ledger and dashboards to the server"
	@echo "make restart                restart fava on the server"
	@echo "make shell                  open a shell in the container"

caixabank-ofx:
	uv run csb2format -f ofx $(IN) documents/CaixaBank/$(MONTH)-CaixaBank.ofx

identify:
	uv run config.py identify documents

extract: extract-caixabank extract-paypal extract-revolut extract-n26

extract-caixabank:
	$(call extract,documents/CaixaBank/$(MONTH)-CaixaBank.ofx,ledger/Caixabank/$(YEAR)/$(MONTH)-caixabank.bean)

extract-paypal:
	$(call extract,documents/Paypal/$(MONTH)-Paypal.csv,ledger/Paypal/$(YEAR)/$(MONTH)-paypal.bean)

extract-revolut:
	$(call extract,documents/Revolut/$(MONTH)-Revolut.csv,ledger/Revolut/$(YEAR)/$(MONTH)-revolut.bean)

extract-n26:
	$(call extract,documents/N26/Checking/$(MONTH)-N26_Checking.csv,ledger/N26/Checking/$(YEAR)/$(MONTH)-n26.bean)
	$(call extract,documents/N26/Savings/$(MONTH)-N26_Saving.csv,ledger/N26/Saving/$(YEAR)/$(MONTH)-n26.bean)

check:
	uv run bean-check ledger/main.bean

fava:
	uv run fava ledger/main.bean

# The ledger is a submodule, so it's archived from its own repo.
deploy:
	git -C ledger archive HEAD | ssh $(FAVA_SSH) docker exec -i $(FAVA_CONTAINER) tar -x -C /data
	git archive $(REF) dashboards | ssh $(FAVA_SSH) docker exec -i $(FAVA_CONTAINER) tar -x -C /

restart:
	ssh -t $(FAVA_SSH) docker restart $(FAVA_CONTAINER)

shell:
	ssh -t $(FAVA_SSH) docker exec -it $(FAVA_CONTAINER) bash
