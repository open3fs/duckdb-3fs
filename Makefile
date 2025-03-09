# Set project directory
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Extension name
EXT_NAME=threefs

# Extension configuration file
EXT_CONFIG=$(PROJ_DIR)extension_config.cmake

# Include DuckDB extension makefile
include $(PROJ_DIR)extension-ci-tools/makefiles/duckdb_extension.Makefile