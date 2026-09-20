DOCKER_RUN := bash scripts/run_spark_connect_server.sh
DOCKER_STOP := docker stop spark-delta
DOCKER_RM := docker rm spark-delta
# Tolerates failure of any step: under `set -e` a failing cleanup command would
# otherwise abort the EXIT trap and leave the container behind.
DOCKER_CLEAN := { $(DOCKER_STOP) >/dev/null 2>&1; $(DOCKER_RM) >/dev/null 2>&1; } || true

docker:
	$(DOCKER_RUN)

stop:
	-$(DOCKER_STOP)
	-$(DOCKER_RM)

test:
	@bash -c '\
		set -e; \
		$(DOCKER_CLEAN); \
		$(DOCKER_RUN); \
		trap "$(DOCKER_CLEAN)" EXIT; \
		cargo test \
	'

.PHONY: docker stop test
