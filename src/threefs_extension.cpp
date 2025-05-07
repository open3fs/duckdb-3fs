#define DUCKDB_EXTENSION_MAIN
#include "threefs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

void InitializeThreeFS();
void DeinitializeThreeFS();

static void LoadInternal(DatabaseInstance &instance) {
    auto &fs = instance.GetFileSystem();
    // Register 3FS filesystem
    auto &buffer_manager = instance.GetBufferManager();
    try {
        // Verify 3FS library is available and working
        ThreeFSFileSystem::Verify();
        
        // Register the file system with DuckDB
        fs.RegisterSubSystem(make_uniq<ThreeFSFileSystem>(buffer_manager));
        
        // Log success message
        //fprintf(stderr, "Successfully registered 3FS extension\n");
    } catch (std::exception &e) {
        throw IOException("Failed to initialize 3FS extension: %s", e.what());
    }
}

void ThreeFSExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
    
    // 初始化3FS资源
    InitializeThreeFS();
    
    // Register extension parameter options
    auto &config = DBConfig::GetConfig(*db.instance);

    // Register 3FS debug parameters
    config.AddExtensionOption("threefs_enable_debug_logging", "Enable verbose debug logging for 3FS operations to standard error", LogicalType::BOOLEAN);

    // Register 3FS connection parameters
    config.AddExtensionOption("threefs_cluster", "Specifies the 3FS cluster name", LogicalType::VARCHAR);
    config.AddExtensionOption("threefs_mount_root", "Specifies the mount root path for 3FS", LogicalType::VARCHAR);
    
    // Register USRBIO parameters
    config.AddExtensionOption("threefs_use_usrbio", "Whether to use USRBIO when possible", LogicalType::BOOLEAN);
    config.AddExtensionOption("threefs_iov_size", "Size of the shared memory buffer for USRBIO (bytes)", LogicalType::UBIGINT);
    config.AddExtensionOption("threefs_ior_entries", "Maximum number of IO requests that can be submitted", LogicalType::INTEGER);
    config.AddExtensionOption("threefs_io_depth", "IO depth for batching", LogicalType::INTEGER);
    config.AddExtensionOption("threefs_ior_timeout", "Timeout for IO operations (ms)", LogicalType::INTEGER);
}

std::string ThreeFSExtension::Name() {
    return "3fs";
}

ThreeFSExtension::~ThreeFSExtension() {
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void threefs_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ThreeFSExtension>();
}

DUCKDB_EXTENSION_API const char *threefs_version() {
    return duckdb::DuckDB::LibraryVersion();
}
} 

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif