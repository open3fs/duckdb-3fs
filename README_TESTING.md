# DuckDB 3FS Extension Testing Guide

## Prerequisites

Before running the tests for the DuckDB 3FS extension, you need to ensure that all required directories exist. The tests expect certain directory structures to be in place.

## Preparing Test Environment

To prepare your environment for testing, follow these steps:

1. Make the preparation script executable:
   ```bash
   chmod +x prepare_test_dirs.sh
   ```

2. Run the script as a user with sufficient permissions (typically root):
   ```bash
   sudo ./prepare_test_dirs.sh
   ```

   This script will create all necessary directories under `/3fs/test_threefs/` with appropriate permissions.

3. If you're running in a containerized environment, ensure that the container has access to the `/3fs` mount point.

## Directory Structure

The script creates the following directory structure:

```
/3fs/
└── test_threefs/
    ├── integration_test/
    │   ├── gender=M/
    │   └── gender=F/
    ├── basic_test/
    │   ├── dir1/
    │   └── dir2/
    ├── io_test/
    ├── concurrency_test/
    ├── error_test/
    │   └── special@#chars/
    └── perf_test/
```

## Running the Tests

After preparing the directories, you can run the tests using various methods:

### Running All Tests

To run the entire test suite:

```bash
# Using the make command
make test

# Using the unittest executable directly
./build/debug/test/unittest
```

### Running Specific Test Groups

To run only tests related to the 3FS extension:

```bash
make TEST_GROUP=threefs test
```

### Running Individual Test Files

To run a specific test file:

```bash
# Using the make command
make TEST_FILE=test/sql/threefs_basic.test test

# Using the unittest executable directly
./build/debug/test/unittest "/root/duckdb-3fs/test/sql/threefs_integration.test"
```

### Manual Testing with DuckDB CLI

You can also run tests manually with the DuckDB CLI:

```bash
# Using the release build
build/release/duckdb -unsigned < test/sql/threefs_basic.test

# Using the debug build
build/debug/duckdb -unsigned < test/sql/threefs_basic.test
```

### Test Categories

The DuckDB 3FS extension test suite includes several categories of tests:

1. **Basic Tests** (`threefs_basic.test`): Core file and directory operations
2. **I/O Tests** (`threefs_io.test`): File reading and writing performance
3. **Concurrency Tests** (`threefs_concurrency.test`): Simulated concurrent access
4. **Error Handling** (`threefs_errors.test`): Error conditions and edge cases
5. **Performance Tests** (`threefs_performance.test`): Benchmark different operations
6. **Integration Tests** (`threefs_integration.test`): Integration with DuckDB features

### Testing in Different Environments

For CI/CD environments, you may want to run tests with different configurations:

```bash
# Run tests with debug build
DEBUG=1 make test

# Run tests with release build and optimizations
RELEASE=1 OPTIMIZE=1 make test
```

## Troubleshooting

If you encounter errors like:

```
No files found that match the pattern
```

It likely means that the required directories don't exist or have incorrect permissions. Re-run the `prepare_test_dirs.sh` script.

For other issues, check the logs and ensure that the 3FS extension is properly configured with appropriate environment variables.

## Note

The test framework doesn't support direct system commands (like `system mkdir`) within test files, which is why this separate preparation script is necessary. 