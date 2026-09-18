DOCKER_RUN := . scripts/run_spark_connect_server.sh ./tmp
DOCKER_STOP := docker stop spark-delta && rm -rf ./tmp
DOCKER_RM := docker rm spark-delta
DOCKER_CLEAN := $(DOCKER_STOP) >/dev/null 2>&1; $(DOCKER_RM) >/dev/null 2>&1

VENV := . venv/bin/activate &&

# The Spark release this checkout targets: the .proto files the crate is
# generated from, the pyspark the parity reference is walked from, and the test
# suites vendored for porting. Override to move the whole repo at once, e.g.
# `make protos vendor deps parity SPARK_VERSION=4.0.0`.
SPARK_VERSION ?= 3.5.7

# Vendored trees are keyed by release line, not patch: a patch upgrade
# overwrites in place, so `git diff` is the drift report.
SPARK_MINOR := $(basename $(SPARK_VERSION))
SPARK_FEATURE := spark-$(subst .,-,$(SPARK_MINOR))
PROTO_DIR := protobuf/spark-$(SPARK_MINOR)
# Spark 4.0 moved the Connect protos out of connector/.
PROTO_ROOT := $(if $(filter 3,$(word 1,$(subst ., ,$(SPARK_VERSION)))),connector,sql)
PROTO_UPSTREAM := https://api.github.com/repos/apache/spark/contents/$(PROTO_ROOT)/connect/common/src/main/protobuf/spark/connect?ref=v$(SPARK_VERSION)

VENDOR := vendor/spark-$(SPARK_MINOR)
UPSTREAM := https://raw.githubusercontent.com/apache/spark/v$(SPARK_VERSION)/python

# Paths relative to `python/` in the Spark repo - which $(UPSTREAM) already
# points at - mirrored verbatim under $(VENDOR) so a re-fetch is a plain diff
# and each path maps onto its dotted module name. Wheels do not ship these files.
VENDOR_TESTS := \
	pyspark/sql/tests/test_catalog.py \
	pyspark/sql/tests/connect/test_parity_catalog.py

CATALOG_TESTS := $(VENDOR)/pyspark/sql/tests/test_catalog.py

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

# Install the Python tooling and the pyspark the parity reference walks.
deps:
	$(VENV) pip install -r requirements.txt pyspark==$(SPARK_VERSION)

# Fetch the Connect .proto files. example_plugins.proto is an upstream sample
# and is not compiled.
protos:
	@mkdir -p $(PROTO_DIR)/spark/connect
	@curl -sSfL "$(PROTO_UPSTREAM)" \
		| grep -o '"download_url": *"[^"]*\.proto"' | cut -d'"' -f4 \
		| grep -v example_plugins \
		| while read -r url; do \
			echo "  $${url##*/}"; \
			curl -sSfL -o $(PROTO_DIR)/spark/connect/$${url##*/} "$$url"; \
		done
	@echo "$(PROTO_DIR) ready - build with --features $(SPARK_FEATURE)"

# Fetch every vendored test file.
vendor: $(addprefix $(VENDOR)/,$(VENDOR_TESTS))

$(VENDOR)/%.py:
	@mkdir -p $(dir $@)
	curl -sSfL -o $@ $(UPSTREAM)/$*.py

# The walker imports the installed pyspark, so a mismatch would inventory an
# API that is not the one being compared.
check-pyspark:
	@$(VENV) python -c "import pyspark, sys; v = pyspark.__version__; \
		sys.exit(0) if v == '$(SPARK_VERSION)' else \
		sys.exit(f'venv has pyspark {v}, expected $(SPARK_VERSION) - run: make deps')"

parity: parity-api parity-tests

parity-api: check-pyspark
	# Reference
	$(VENV) api-parity-py \
		reference 'pyspark.sql.connect,pyspark.storagelevel' \
		--version-from pyspark \
		-o api-parity/ref.json

	# Port
	cargo run --release --bin api-parity-dump > api-parity/port.json

	# Compare
	$(VENV) api-parity compare \
		api-parity/ref.json api-parity/port.json > api-parity/report.md

# Rust `#[cfg(test)]` functions reach neither producer - rustdoc JSON omits them
# and `inventory` never links them - so the test suites have no port inventory
# to compare against. They are rendered as a checklist instead.
parity-tests: check-pyspark $(CATALOG_TESTS)
	$(VENV) api-parity-py \
		reference pyspark.sql.tests.test_catalog \
		--from-source $(CATALOG_TESTS) \
		--version-from pyspark \
		-o api-parity/ref-tests.json

	$(VENV) python scripts/parity_tests_md.py \
		api-parity/ref-tests.json api-parity/tests.md

.PHONY: docker stop test deps protos vendor check-pyspark parity parity-api parity-tests
