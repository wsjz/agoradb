#!/usr/bin/env bash
# Copyright 2025 The Oikome Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end check of the network phase (3.1) on the three nodes of
# docker/compose.yml: discovery, TLS pinned to DIDs, a UCAN-gated federated
# query, subscription, revocation cutting access, and Lance search on a
# Space and on a subscribed copy of it. Every step prints what
# was expected and what happened; the first mismatch fails the run.
#
#   E2E_BUILD=1     build the image first (default)
#   E2E_LOG_DIR=d   on failure, write the nodes' logs to d/compose.log

set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE=(docker compose -f docker/compose.yml)
LOG_DIR=${E2E_LOG_DIR:-e2e-logs}

cleanup() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        mkdir -p "$LOG_DIR"
        "${COMPOSE[@]}" logs --no-color >"$LOG_DIR/compose.log" 2>&1 || true
        echo "node logs: $LOG_DIR/compose.log"
    fi
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT

fail() {
    echo "FAIL $*"
    exit 1
}

# Run SQL as the owner of a node; prints rows as tab-separated values.
sql() {
    "${COMPOSE[@]}" exec -T "$1" oikome sql --raw "$2"
}

# Run SQL that must fail; prints the error.
sql_err() {
    local out
    if out=$("${COMPOSE[@]}" exec -T "$1" oikome sql --raw "$2" 2>&1); then
        fail "expected an error from $1: $2 (got: $out)"
    fi
    printf '%s' "$out"
}

check() {
    local what=$1 expected=$2 actual=$3
    if [[ "$actual" == "$expected" ]]; then
        printf 'ok   %s\n     expected: %s\n     actual:   %s\n' "$what" "$expected" "$actual"
    else
        fail "$(printf '%s\n     expected: %s\n     actual:   %s' "$what" "$expected" "$actual")"
    fi
}

check_contains() {
    local what=$1 expected=$2 actual=$3
    if [[ "$actual" == *"$expected"* ]]; then
        printf 'ok   %s\n     expected to contain: %s\n     actual: %s\n' "$what" "$expected" "$actual"
    else
        fail "$(printf '%s\n     expected to contain: %s\n     actual: %s' "$what" "$expected" "$actual")"
    fi
}

# wait_for <description> <seconds> <command...>: retry until it succeeds.
wait_for() {
    local what=$1 seconds=$2
    shift 2
    local deadline=$((SECONDS + seconds))
    until "$@" >/dev/null 2>&1; do
        ((SECONDS < deadline)) || fail "$what (not within ${seconds}s)"
        sleep 0.5
    done
    echo "ok   $what"
}

logs_contain() {
    "${COMPOSE[@]}" logs --no-color "$1" 2>/dev/null | grep -q "$2"
}

knows_peer() {
    sql "$1" "SHOW PEERS" | grep -q "$2"
}

if [[ ${E2E_BUILD:-1} == 1 ]]; then
    "${COMPOSE[@]}" build
fi

echo "== start node-a, then node-b and node-c"
"${COMPOSE[@]}" up -d node-a
wait_for "node-a serves Flight and runs discovery" 120 logs_contain node-a "discovery started"
"${COMPOSE[@]}" up -d node-b node-c
wait_for "node-b is up" 120 logs_contain node-b "discovery started"
wait_for "node-c is up" 120 logs_contain node-c "discovery started"

DID_A=$("${COMPOSE[@]}" exec -T node-a oikome identity)
DID_B=$("${COMPOSE[@]}" exec -T node-b oikome identity)
ENDPOINT_A="https://172.28.0.10:50051"
echo "node-a: $DID_A"
echo "node-b: $DID_B"

echo "== node-a owns blog.posts and grants it to node-b"
sql node-a "CREATE SPACE blog" >/dev/null
sql node-a "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR)" >/dev/null
sql node-a "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello'), (2, 'bob', 'World'), (3, 'alice', 'Again')" >/dev/null
sql node-a "GRANT SELECT ON blog.posts TO \"$DID_B\"" >/dev/null
QUERY=$(sql node-a "ISSUE UCAN ON SPACE blog TO '$DID_B'")
READ=$(sql node-a "ISSUE UCAN ON SPACE blog TO '$DID_B' CAN 'space/read'")
QUERY_ID=$(cut -f1 <<<"$QUERY")
QUERY_TOKEN=$(cut -f4 <<<"$QUERY")
READ_ID=$(cut -f1 <<<"$READ")
READ_TOKEN=$(cut -f4 <<<"$READ")

echo "== node-b has a local transactional Space"
sql node-b "CREATE SPACE orders WITH KIND = 'transactional'" >/dev/null
sql node-b "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount INTEGER)" >/dev/null
sql node-b "INSERT INTO orders.orders VALUES (10, 'alice', 5), (11, 'carol', 7), (12, 'bob', 3)" >/dev/null

echo "== discovery (node-b dialed node-a)"
wait_for "node-b discovers node-a" 60 knows_peer node-b "$DID_A"
check_contains "node-b knows node-a's TLS endpoint" "$DID_A	$ENDPOINT_A" "$(sql node-b "SHOW PEERS")"
wait_for "node-a learns node-b from the same connection" 60 knows_peer node-a "$DID_B"

echo "== UCAN-gated federated query, owner found by DID (no PEER)"
sql node-b "CREATE SPACE pub WITH TOKEN = '$QUERY_TOKEN', REMOTE = 'blog'" >/dev/null
JOIN="SELECT o.id, p.title FROM orders.orders o JOIN pub.posts p ON p.author = o.customer ORDER BY o.id, p.title"
check "local SQLite joined with node-a's Parquet over TLS" \
    "$(printf '10\tAgain\n10\tHello\n12\tWorld')" "$(sql node-b "$JOIN")"

echo "== subscription (space/read): a local copy"
sql node-b "CREATE SPACE copy WITH TOKEN = '$READ_TOKEN', REMOTE = 'blog', MODE = 'subscribe'" >/dev/null
check "rows in node-b's copy" "3" "$(sql node-b "SELECT count(*) FROM copy.posts")"

echo "== revocation cuts access"
sql node-a "REVOKE UCAN '$QUERY_ID'" >/dev/null
sql node-a "REVOKE UCAN '$READ_ID'" >/dev/null
check_contains "remote query after revocation" "permission denied" \
    "$(sql_err node-b "SELECT count(*) FROM pub.posts")"
check_contains "refresh after revocation" "permission denied" \
    "$(sql_err node-b "REFRESH SPACE copy")"
check "the copy already taken stays readable" "3" "$(sql node-b "SELECT count(*) FROM copy.posts")"

echo "== Lance (3.3): node-a searches its Lance Space, node-b a copy of it"
sql node-a "CREATE SPACE notes WITH FORMAT = 'lance'" >/dev/null
sql node-a "CREATE TABLE notes.items (id BIGINT, body VARCHAR, embedding FLOAT[4])" >/dev/null
sql node-a "INSERT INTO notes.items SELECT i, CASE WHEN i % 3 = 0 THEN 'apple banana' ELSE 'cherry date' END || ' note ' || i, [i % 7, i % 5, i % 3, 1.0]::FLOAT[4] FROM (SELECT unnest(range(300)) AS i) t" >/dev/null
sql node-a "CREATE INDEX emb ON notes.items USING IVF_FLAT (embedding) WITH (num_partitions = 2, metric_type = 'l2')" >/dev/null
sql node-a "CREATE INDEX body ON notes.items USING INVERTED (body)" >/dev/null
sql node-a "GRANT SELECT ON notes.items TO \"$DID_B\"" >/dev/null
NEAREST="SELECT id FROM lance_vector_search(%s.items, 'embedding', [0, 0, 0, 1]::FLOAT[4], k => 3) ORDER BY _distance, id"
BANANAS="SELECT count(*) FROM lance_fts(%s.items, 'body', 'banana', k => 5) WHERE id %% 3 = 0"
check "vector search on node-a" "$(printf '0\n105\n210')" "$(sql node-a "$(printf "$NEAREST" notes)")"
check "full-text search on node-a" "5" "$(sql node-a "$(printf "$BANANAS" notes)")"
NOTES_READ=$(sql node-a "ISSUE UCAN ON SPACE notes TO '$DID_B' CAN 'space/read'" | cut -f4)
sql node-b "CREATE SPACE notes_copy WITH TOKEN = '$NOTES_READ', REMOTE = 'notes', MODE = 'subscribe'" >/dev/null
indexes() {
    "${COMPOSE[@]}" exec -T "$1" sh -c "ls $2/items.lance/_indices | sort | tr '\n' ' '"
}
check "node-b's copy holds node-a's indexes (none built on node-b)" \
    "$(indexes node-a /data/.oikome/lance/notes)" \
    "$(indexes node-b /data/.oikome/replicas/notes_copy/lance)"
check "vector search on node-b's copy" "$(printf '0\n105\n210')" "$(sql node-b "$(printf "$NEAREST" notes_copy)")"
check "full-text search on node-b's copy" "5" "$(sql node-b "$(printf "$BANANAS" notes_copy)")"

echo "== mDNS (node-c was given no address)"
wait_for "node-c discovers node-a by mDNS" 60 knows_peer node-c "$DID_A"
check_contains "node-c knows node-a's TLS endpoint" "$DID_A	$ENDPOINT_A" "$(sql node-c "SHOW PEERS")"

echo "== all checks passed"
