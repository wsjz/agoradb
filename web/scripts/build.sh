#!/usr/bin/env bash
# Copyright 2025 The Oikome Authors
# SPDX-License-Identifier: Apache-2.0
#
# Build the browser node: crates/oikome-wasm to WebAssembly, its JS glue
# into web/pkg, and the host page into web/dist.
#
# DataFusion pulls in zstd (C), so a clang with a wasm32 backend is needed:
# Homebrew LLVM on macOS (Apple's clang has none), the distribution clang on
# Linux. Override with CC_wasm32_unknown_unknown / AR_wasm32_unknown_unknown.
#
# PROFILE defaults to `wasm` (Cargo.toml: size-optimized, LTO, stripped);
# PROFILE=release keeps symbol names for readable panic stacks.

set -euo pipefail
cd "$(dirname "$0")/.."
WEB=$PWD
ROOT=$(cd .. && pwd)

if [[ -z ${CC_wasm32_unknown_unknown:-} ]]; then
    for llvm in /opt/homebrew/opt/llvm/bin /usr/local/opt/llvm/bin; do
        if [[ -x $llvm/clang ]]; then
            export CC_wasm32_unknown_unknown=$llvm/clang
            export AR_wasm32_unknown_unknown=$llvm/llvm-ar
            break
        fi
    done
fi

PROFILE=${PROFILE:-wasm}
(cd "$ROOT" && cargo build -p oikome-wasm --target wasm32-unknown-unknown --profile "$PROFILE")
wasm-bindgen --target web --out-dir "$WEB/pkg" \
    "$ROOT/target/wasm32-unknown-unknown/$PROFILE/oikome_wasm.wasm"
npx esbuild src/main.js --bundle --format=esm --outfile=dist/app.js --log-level=warning
ls -l pkg/oikome_wasm_bg.wasm dist/app.js
