#!/bin/sh

set -eu

COMPOSE_FILE="docker-compose.test.yml"

cleanup() {
    docker compose -f "$COMPOSE_FILE" down --volumes
}

# 성공·실패 여부와 관계없이 임시 테스트 DB를 삭제한다.
trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up \
    --build \
    --abort-on-container-exit \
    --exit-code-from tests
