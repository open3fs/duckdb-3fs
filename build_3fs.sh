#!/bin/bash
set -e

cd 3fs
# Update and initialize submodules
echo "Updating git submodules..."
git submodule update --init --recursive

# Apply patches
echo "Applying patches..."
./patches/apply.sh

# Create build directory
echo "Building 3fs..."
cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build -j 8

cd ..
echo "Build completed successfully!" 