#!/bin/bash
# Pinned to a Spark 4.0.4 image: 4.1.x servers drop SQL parameters whenever a
# session extension is configured (SPARK-59672), which fails the bind tests.
docker run -d -p 15002:15002 \
    --name spark-delta \
    franciscoabsampaio/spark-connect-server:delta-4.0.1-spark4.0.4-java21-scala2.13

# The gRPC channel is lazy, so tests started before the server accepts
# connections fail with a transport error instead of waiting. Prefer the
# container's own health status, and fall back to the port for images that
# declare no HEALTHCHECK.
ready() {
    local health
    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' spark-delta 2>/dev/null)
    case "$health" in
        healthy) return 0 ;;
        starting|unhealthy) return 1 ;;
        # Probed inside the container: the published port is served by Docker's
        # proxy, which accepts connections before the server itself listens.
        *) docker exec spark-delta bash -c 'exec 3<>/dev/tcp/127.0.0.1/15002' 2>/dev/null ;;
    esac
}

# Only an exited container is fatal: `docker run -d` returns while the
# container is still `created`, which is not yet `running`.
stopped() {
    case "$(docker inspect -f '{{.State.Status}}' spark-delta 2>/dev/null)" in
        exited | dead) return 0 ;;
        *) return 1 ;;
    esac
}

for _ in $(seq 1 120); do
    ready && exit 0
    if stopped; then
        echo "spark-delta stopped before becoming ready:" >&2
        docker logs --tail 20 spark-delta >&2 2>&1
        exit 1
    fi
    sleep 2
done

echo "spark-delta did not become ready within 240s" >&2
exit 1
