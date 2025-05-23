#pragma once

#include <string>

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

void InitializeThreeFS();
void DeinitializeThreeFS();

struct ThreeFSParams {
    // 3FS cluster related parameters
    string cluster_name;        // Cluster name
    string mount_root = "/";    // Mount point root directory
    bool enable_debug_logging = false; // Enable debug logging
    
    // USRBIO related parameters
    bool use_usrbio = true;                // Whether to use USRBIO API
    size_t iov_size = 1024 * 1024;         // Shared memory size (1MB)
    size_t ior_entries = 1024;             // Maximum number of requests in IO ring
    //`0` for no control with I/O depth.
    // If greater than 0, then only when `io_depth` I/O requests are in queue, they will be issued to server as a batch.
    // If smaller than 0, then USRBIO will wait for at most `-io_depth` I/O requests are in queue and issue them in one batch. 
    // If io_depth is 0, then USRBIO will issue all the prepared I/O requests to server ASAP.
    size_t io_depth = 0;                  // IO batch processing depth
    int ior_timeout = 0;                   // IO timeout (milliseconds)
    
    // Read parameters from file opener
    static ThreeFSParams ReadFrom(optional_ptr<FileOpener> opener);
};


class ThreeFSFileSystem : public FileSystem {
 public:
  explicit ThreeFSFileSystem(BufferManager &) {
    // Initialize global default parameters
    params.cluster_name = "";
    params.mount_root = "/";
    params.use_usrbio = true;
  }

  // Verify that 3FS library is available
  static void Verify() {
    // Add verification logic here to ensure 3FS library is available
    // Throw an exception if there's a problem
  }

  unique_ptr<FileHandle> OpenFile(
      const string &path, FileOpenFlags flags,
      optional_ptr<FileOpener> opener = nullptr) override;

  //! Read exactly nr_bytes from the specified location in the file. Fails if
  //! nr_bytes could not be read. This is equivalent to calling
  //! SetFilePointer(location) followed by calling Read().
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  //! Write exactly nr_bytes to the specified location in the file. Fails if
  //! nr_bytes could not be written. This is equivalent to calling
  //! SetFilePointer(location) followed by calling Write().
  void Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
             idx_t location) override;
  //! Read nr_bytes from the specified file into the buffer, moving the file
  //! pointer forward by nr_bytes. Returns the amount of bytes read.
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  //! Write nr_bytes from the buffer into the file, moving the file pointer
  //! forward by nr_bytes.
  int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  //! Excise a range of the file. The file-system is free to deallocate this
  //! range (sparse file support). Reads to the range will succeed but will
  //! return undefined data.
  bool Trim(FileHandle &handle, idx_t offset_bytes,
            idx_t length_bytes) override;

  //! Returns the file size of a file handle, returns -1 on error
  int64_t GetFileSize(FileHandle &handle) override;
  //! Returns the file last modified time of a file handle, returns timespec
  //! with zero on all attributes on error
  time_t GetLastModifiedTime(FileHandle &handle) override;
  //! Returns the file last modified time of a file handle, returns timespec
  //! with zero on all attributes on error
  FileType GetFileType(FileHandle &handle) override;
  //! Truncate a file to a maximum size of new_size, new_size should be smaller
  //! than or equal to the current size of the file
  void Truncate(FileHandle &handle, int64_t new_size) override;

  //! Check if a directory exists
  bool DirectoryExists(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;
  //! Create a directory if it does not exist
  void CreateDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;
  //! Recursively remove a directory and all files in it
  void RemoveDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;
  //! List files in a directory, invoking the callback method for each one with
  //! (filename, is_dir)
  bool ListFiles(const string &directory,
                 const std::function<void(const string &, bool)> &callback,
                 FileOpener *opener = nullptr) override;
  //! Move a file from source path to the target, StorageManager relies on this
  //! being an atomic action for ACID properties
  void MoveFile(const string &source, const string &target,
                optional_ptr<FileOpener> opener = nullptr) override;
  //! Check if a file exists
  bool FileExists(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override;

  //! Check if path is a pipe
  bool IsPipe(const string &filename,
              optional_ptr<FileOpener> opener = nullptr) override;
  //! Remove a file from disk
  void RemoveFile(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override;
  //! Sync a file handle to disk
  void FileSync(FileHandle &handle) override;

  //! Runs a glob on the file system, returning a list of matching files
  vector<string> Glob(const string &path,
                      FileOpener *opener = nullptr) override;

  bool CanHandleFile(const string &fpath) override;

  //! Set the file pointer of a file handle to a specified location. Reads and
  //! writes will happen from this location
  void Seek(FileHandle &handle, idx_t location) override;
  //! Return the current seek posiiton in the file.
  idx_t SeekPosition(FileHandle &handle) override;

  //! Whether or not we can seek into the file
  bool CanSeek() override;
  //! Whether or not the FS handles plain files on disk. This is relevant for
  //! certain optimizations, as random reads in a file on-disk are much cheaper
  //! than e.g. random reads in a file over the network
  bool OnDiskFile(FileHandle &handle) override;

  std::string GetName() const override { return "3fs"; }

  //! Returns the last Win32 error, in string format. Returns an empty string if
  //! there is no error, or on non-Windows systems.
  static std::string GetLastErrorAsString();

  //! Checks a file is private (checks for 600 on linux/macos, TODO: currently
  //! always returns true on windows)
  static bool IsPrivateFile(const string &path_p, FileOpener *opener);

  // returns a C-string of the path that trims any file:/ prefix
  static const char *NormalizeLocalPath(const string &path);

 private:
  //! Set the file pointer of a file handle to a specified location. Reads and
  //! writes will happen from this location
  void SetFilePointer(FileHandle &handle, idx_t location);
  idx_t GetFilePointer(FileHandle &handle);

  vector<string> FetchFileWithoutGlob(const string &path, FileOpener *opener,
                                      bool absolute_path);
  int64_t ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);

  ThreeFSParams params;
};

// Define ThreeFSExtension class
class ThreeFSExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
  ~ThreeFSExtension() override;
};

}  // namespace duckdb