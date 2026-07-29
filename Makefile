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

parity:
	# Reference
	. venv/bin/activate && api-parity-py \
		reference 'pyspark.sql.connect,pyspark.storagelevel' \
		--version-from pyspark \
		-o api-parity/ref.json

	# Port
	cargo run --release --bin api-parity-dump > api-parity/port.json

	# Compare
	. venv/bin/activate && api-parity compare \
		api-parity/ref.json api-parity/port.json > api-parity/report.md