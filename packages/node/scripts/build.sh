#!/usr/bin/env bash
# Copyright 2025 The Oikome Authors
# SPDX-License-Identifier: Apache-2.0
#
# Build the browser node: crates/oikome-wasm to WebAssembly and its JS glue
# into packages/node/pkg. The test page is bundled by e2e (`npm run build`
# at the repository root runs both).
#
# DataFusion pulls in zstd (C), so a clang with a wasm32 backend is needed:
# Homebrew LLVM on macOS (Apple's clang has none), the distribution clang on
# Linux. Override with CC_wasm32_unknown_unknown / AR_wasm32_unknown_unknown.
#
# PROFILE defaults to `wasm` (Cargo.toml: size-optimized, LTO, stripped);
# PROFILE=release keeps symbol names for readable panic stacks.

set -euo pipefail
cd "$(dirname "$0")/.."
NODE=$PWD
ROOT=$(cd ../.. && pwd)
# Where cargo writes: CARGO_TARGET_DIR (relative to the workspace root, where
# cargo runs below) or the workspace's target/.
TARGET=${CARGO_TARGET_DIR:-target}
[[ $TARGET == /* ]] || TARGET=$ROOT/$TARGET

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
wasm-bindgen --target web --out-dir "$NODE/pkg" \
    "$TARGET/wasm32-unknown-unknown/$PROFILE/oikome_wasm.wasm"
ls -l pkg/oikome_wasm_bg.wasm
