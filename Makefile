DOCKER_RUN := . scripts/run_spark_connect_server.sh ./tmp
DOCKER_STOP := docker stop spark-delta && rm -rf ./tmp
DOCKER_RM := docker rm spark-delta
DOCKER_CLEAN := $(DOCKER_STOP) >/dev/null 2>&1; $(DOCKER_RM) >/dev/null 2>&1

docker:
	$(DOCKER_RUN)

stop:
	-$(DOCKER_STOP)
	-$(DOCKER_RM)

test:
	@bash -c '\
		set -e; \
		$(DOCKER_RUN); \
		trap "$(DOCKER_CLEAN)" EXIT; \
		cargo test \
	'

PYSPARK_VERSION ?= 3.5
PYSPARK_REQUIREMENTS := tools/requirements-$(PYSPARK_VERSION).txt
PYSPARK_INVENTORY_OUT := parity/pyspark-$(PYSPARK_VERSION).json

pyspark-inventory:
	@test -d venv || python3 -m venv venv
	@venv/bin/pip install -q -r $(PYSPARK_REQUIREMENTS)
	@mkdir -p $(dir $(PYSPARK_INVENTORY_OUT))
	venv/bin/python tools/pyspark_inventory.py \
		--package pyspark.sql.connect \
		--package pyspark.sql.session \
		--version-from pyspark \
		-o $(PYSPARK_INVENTORY_OUT)
	@echo "Wrote $(PYSPARK_INVENTORY_OUT)"

RUST_INVENTORY_OUT := parity/spark-connect.json
PARITY_REPORT_OUT  := parity/PARITY.md

rust-inventory:
	@mkdir -p $(dir $(RUST_INVENTORY_OUT))
	cargo run -q --bin api_parity_dump > $(RUST_INVENTORY_OUT)
	@echo "Wrote $(RUST_INVENTORY_OUT)"

parity-report: pyspark-inventory rust-inventory
	venv/bin/python tools/parity_report.py \
		--py   $(PYSPARK_INVENTORY_OUT) \
		--rust $(RUST_INVENTORY_OUT) \
		-o     $(PARITY_REPORT_OUT)
	@echo "Wrote $(PARITY_REPORT_OUT)"