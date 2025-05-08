# DuckDB 3FS Extension

The DuckDB 3FS extension enables DuckDB to seamlessly interact with the 3FS distributed file system, providing high-performance data access and manipulation capabilities directly from SQL.

## Overview

The 3FS extension integrates DuckDB with the high-performance 3FS distributed file system, allowing users to:

- Read and write data directly to and from 3FS using standard SQL
- Perform SQL queries over data stored in 3FS without copying to local storage
- Support for various file formats including CSV, Parquet, and JSON
- Take advantage of 3FS's high-throughput I/O and USRBIO capabilities
- Manage 3FS files and directories directly from SQL queries

## Features

- **Direct Data Access**: Access data stored in 3FS using the `3fs://` URI prefix
- **File Operations**: Create, read, write, append, delete, copy, and move files
- **Directory Operations**: Create, list, and delete directories
- **Format Support**: Native support for multiple formats, including:
  - CSV
  - Parquet
  - JSON
- **Advanced Features**:
  - Parallel reading and writing
  - Buffer size optimization
  - USRBIO support for enhanced performance
  - Integration with other DuckDB functionality (views, joins, etc.)

## Requirements

- DuckDB (latest recommended)
- 3FS installation with properly configured mountpoints
- CMake 3.12 or later
- C++17 compatible compiler

## Building the Extension

### Prerequisites

Ensure you have the following prerequisites installed:
- CMake (3.12 or newer)
- C++17 compatible compiler (e.g., GCC 7+, Clang 5+)
- DuckDB development environment
- 3FS libraries and headers

### Building from Source

1. Clone the repository with submodules:
   ```bash
   git clone --recurse-submodules https://github.com/xxxx/duckdb-3fs.git
   cd duckdb-3fs
   ```

2. Build 3FS libraries (if not using pre-built versions):
   ```bash
   ./build_3fs.sh
   ```

3. Build the DuckDB 3FS extension:
   ```bash
   GEN=ninja make -j$(nproc)
   ```

   This will compile both the statically linked extension and the loadable extension.

4. The built extension will be located at:
   ```
   build/release/extension/threefs/threefs.duckdb_extension
   ```

### Building with Custom 3FS Installation

If you have 3FS installed in a custom location, modify `CMakeLists.txt` to point to your installation:

```cmake
set(DEEPSEEK_3FS_INCLUDE_DIR "/path/to/3fs/include")
set(DEEPSEEK_3FS_LIB_DIR "/path/to/3fs/lib")
```

Or pass the paths during the build:

```bash
make DEEPSEEK_3FS_INCLUDE_DIR=/path/to/3fs/include DEEPSEEK_3FS_LIB_DIR=/path/to/3fs/lib
```

### Building Python Duckdb

If you want to use python duckdb with 3FS extension, you can build and install:
```bash
cd /path/duckdb-3fs/
GEN=ninja make -j$(nproc) BUILD_PYTHON=1 CORE_EXTENSIONS="httpfs"
cd /path/duckdb-3fs/duckdb/tools/pythonpkg
python3 -m pip install .
```


## Using the Extension

### Loading the Extension

Load the extension in DuckDB:

```sql
-- Install the extension (if not installed already)
INSTALL 'path/to/threefs.duckdb_extension';

-- Load the extension
LOAD threefs;
```

### Configuration

Configure the extension with your 3FS cluster details:

```sql
-- Set 3FS cluster name
SET threefs_cluster='my_cluster';

-- Set 3FS mount root (if needed)
SET threefs_mount_root='/path/to/3fs/mount';

-- Enable USRBIO mode for optimal performance (if supported)
SET threefs_use_usrbio=true;

-- Set custom buffer size (optional)
SET threefs_iov_size=16384;
```

### Basic Usage

Query data directly from 3FS:

```sql
-- Read CSV file from 3FS
SELECT * FROM read_csv_auto('3fs://path/to/data.csv');

-- Read Parquet file from 3FS
SELECT * FROM read_parquet('3fs://path/to/data.parquet');

-- Write query results to 3FS
COPY (SELECT * FROM my_table) TO '3fs://path/to/output.parquet' (FORMAT PARQUET);
```

## Using the Extension in Python
```python
import duckdb
con = duckdb.connect(config={'allow_unsigned_extensions': True})
con.install_extension('/root/duckdb-3fs/build/release/extension/threefs/threefs.duckdb_extension')
con.load_extension('threefs')
con.execute("SET threefs_cluster='open3fs';")
con.execute("SET threefs_mount_root='/3fs/';")
con.execute("SET threefs_use_usrbio=true;")
con.execute("SET threefs_iov_size=16384;")
con.execute("SET threefs_enable_debug_logging=true;")
data = con.sql("SELECT * FROM read_parquet('/3fs/duckdb/prices.parquet')")
print(data)
con.sql("COPY (SELECT * FROM read_parquet('/3fs/duckdb/prices.parquet')) TO '/3fs/duckdb/output.parquet' (FORMAT PARQUET);")
data = con.sql("SELECT * FROM read_parquet('3fs://3fs/duckdb/prices.parquet')")
print(data)
```


## Testing

The extension includes comprehensive test suites covering different aspects of functionality:

### Running All Tests

To run all tests:

```bash
make test
```

### Running Specific Test Groups

To run only 3FS extension tests:

```bash
make TEST_GROUP=threefs test
```

### Running Individual Tests

To run a specific test file:

```bash
make TEST_FILE=test/sql/threefs_basic.test test
```

### Manual Testing

You can also run tests manually with the DuckDB CLI:

```bash
build/release/duckdb -unsigned < test/sql/threefs_basic.test
```

### Test Categories

The test suite includes:

1. **Basic Tests** (`threefs_basic.test`): Core file and directory operations
2. **I/O Tests** (`threefs_io.test`): File reading and writing performance
3. **Concurrency Tests** (`threefs_concurrency.test`): Simulated concurrent access
4. **Error Handling** (`threefs_errors.test`): Error conditions and edge cases
5. **Performance Tests** (`threefs_performance.test`): Benchmark different operations
6. **Integration Tests** (`threefs_integration.test`): Integration with DuckDB features

### Creating Your Own Tests

You can create additional tests by following the DuckDB test format. Test files should be placed in the `test/sql/` directory with a `.test` extension.

## Troubleshooting

- If the extension fails to load with errors about missing symbols, ensure all dependencies are properly installed and linked.
- For I/O performance issues, try adjusting the buffer size with `SET threefs_iov_size=<size>`.
- If experiencing permission errors, check that your 3FS mount point has appropriate permissions.

## License

This project is licensed under [LICENSE INFORMATION].

## Acknowledgments

This extension integrates DuckDB with the 3FS distributed file system developed by [ORGANIZATION]. 
