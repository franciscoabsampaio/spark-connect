DOCKER_RUN := bash scripts/run_spark_connect_server.sh
DOCKER_STOP := docker stop spark-delta
DOCKER_RM := docker rm spark-delta
# Tolerates failure of any step: under `set -e` a failing cleanup command would
# otherwise abort the EXIT trap and leave the container behind.
DOCKER_CLEAN := { $(DOCKER_STOP) >/dev/null 2>&1; $(DOCKER_RM) >/dev/null 2>&1; } || true

# apache-spark-connect-proto compiles the Connect protos at build time, so a
# protoc binary has to exist. Prefer the system one; otherwise fetch this
# release into .tools/ once and use that.
PROTOC_VERSION := 36.2
PROTOC_DIR := .tools/protoc-$(PROTOC_VERSION)
SYSTEM_PROTOC := $(shell command -v protoc 2>/dev/null)
PROTOC ?= $(if $(SYSTEM_PROTOC),$(SYSTEM_PROTOC),$(abspath $(PROTOC_DIR)/bin/protoc))

protoc:
	@test -x "$(PROTOC)" || bash scripts/install_protoc.sh $(PROTOC_VERSION) $(PROTOC_DIR)

# For plain cargo invocations: PROTOC=$$(make -s protoc-path) cargo build
protoc-path: protoc
	@echo "$(PROTOC)"

docker:
	$(DOCKER_RUN)

stop:
	-$(DOCKER_STOP)
	-$(DOCKER_RM)

test: protoc
	@PROTOC="$(PROTOC)" bash -c '\
		set -e; \
		$(DOCKER_CLEAN); \
		$(DOCKER_RUN); \
		trap "$(DOCKER_CLEAN)" EXIT; \
		cargo test --all-features \
	'

.PHONY: docker protoc protoc-path stop test
