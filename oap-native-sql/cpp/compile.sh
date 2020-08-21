#!/usr/bin/env bash

set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make

set +eu

