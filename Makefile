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