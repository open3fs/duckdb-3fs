#!/bin/bash

# Script to create all directories required for DuckDB 3FS extension tests
echo "Creating directories needed for tests..."

# Ensure 3FS mount point exists
mkdir -p /3fs

# Create 3FS test directories
mkdir -p /3fs/test_threefs/integration_test/gender=M
mkdir -p /3fs/test_threefs/integration_test/gender=F
mkdir -p /3fs/test_threefs/basic_test/dir1
mkdir -p /3fs/test_threefs/basic_test/dir2
mkdir -p /3fs/test_threefs/io_test
mkdir -p /3fs/test_threefs/concurrency_test
mkdir -p /3fs/test_threefs/error_test/special@#chars
mkdir -p /3fs/test_threefs/error_test/very_long_directory_name_to_test_path_length_limits_in_3fs_filesystem_implementation
mkdir -p /3fs/test_threefs/perf_test

echo "All test directories have been created successfully!"
echo "You can now run the tests." 