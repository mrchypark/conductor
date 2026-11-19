#!/bin/sh
set -eu

image=${1:-conductor:smoke}
name=conductor-smoke-$$
volume=$name-data
cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then docker logs "$name" >&2 || :; fi
    docker rm -f "$name" >/dev/null 2>&1 || :
    docker volume rm "$volume" >/dev/null 2>&1 || :
    exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

docker volume create "$volume" >/dev/null
start() {
    docker run -d --name "$name" \
        -p 127.0.0.1::8080 -v "$volume:/data" "$image" >/dev/null
    port=$(docker port "$name" 8080/tcp | sed -n '1s/.*://p')
    url=http://127.0.0.1:$port
}
start
ready() {
    attempt=0
    while [ "$attempt" -lt 60 ]; do
        if response=$(curl -fsS --connect-timeout 1 --max-time 2 "$url/v1/workspaces/ws-smoke/handlers" 2>/dev/null) &&
            printf '%s\n' "$response" |
            jq -e 'any(.[]; .handler_key == "payload.echo" and .version == "1")' >/dev/null; then
            return
        fi
        [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ] || return 1
        attempt=$((attempt + 1))
        sleep 1
    done
    echo 'Container readiness timed out' >&2
    return 1
}
ready
response=$(curl -fsS --connect-timeout 2 --max-time 30 -H 'Content-Type: application/json' \
    -d '{"execution_request_id":"er-smoke","handler":"payload.echo","handler_version":"1","operation":"echo","input":{"smoke":true}}' \
    "$url/v1/workspaces/ws-smoke/executions")
printf '%s\n' "$response" |
    jq -e '.id == "exec_er-smoke" and .status == "succeeded" and .output.smoke == true' >/dev/null
docker stop -t 5 "$name" >/dev/null
docker rm "$name" >/dev/null
start
ready
response=$(curl -fsS --connect-timeout 2 --max-time 10 "$url/v1/workspaces/ws-smoke/executions/exec_er-smoke")
printf '%s\n' "$response" |
    jq -e '.status == "succeeded" and .output.smoke == true' >/dev/null
echo 'Container smoke passed (echo execution and persistence after replacement)'
