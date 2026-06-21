#!/usr/bin/env sh
set -eu

cargo +nightly build --release --target x86_64-unknown-linux-gnu
mkdir -p dist
cp target/x86_64-unknown-linux-gnu/release/libgmsv_rhttp.so dist/gmsv_rhttp_linux64.dll
