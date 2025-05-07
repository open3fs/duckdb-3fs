#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h> // For time()

// Include 3fs headers
#include "lib/api/hf3fs_usrbio.h"

// macOS uses sys/mount.h instead of sys/vfs.h
#if defined(__APPLE__)
#include <sys/mount.h>
#define HAVE_SYS_MOUNT_H
#endif

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

// includes for giving a better error message on lock conflicts
#if defined(__linux__) || defined(__APPLE__)
#include <pwd.h>
#endif

#if defined(__linux__)
// See https://man7.org/linux/man-pages/man2/fallocate.2.html
#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* See feature_test_macros(7) */
#endif
#include <fcntl.h>
#include <libgen.h>
// See e.g.:
// https://opensource.apple.com/source/CarbonHeaders/CarbonHeaders-18.1/TargetConditionals.h.auto.html
#elif defined(__APPLE__)
#include <TargetConditionals.h>
#include <libproc.h>
#endif

#include "include/threefs.hpp"
#include "lib/api/hf3fs.h"

// Define HAVE_SYS_MOUNT_H macro before including 3FS headers so that 3FS headers can properly handle macOS
#if defined(__APPLE__)
#ifndef HAVE_SYS_MOUNT_H
#define HAVE_SYS_MOUNT_H
#endif
#endif

// Simplified implementation of Glob function
// Referenced from DuckDB's Glob function implementation
// Placed in the global namespace so that it can be called from other places
static bool Glob(const char *string, duckdb::idx_t slen, const char *pattern, duckdb::idx_t plen) {
    // Check special cases
    if (plen == 0) {
        return slen == 0;
    }

    if (pattern[0] == '*') {
        // Handle '*' wildcard
        pattern++, plen--;
        // Try to skip the current character and see if the rest matches
        for (duckdb::idx_t i = 0; i <= slen; i++) {
            if (Glob(string + i, slen - i, pattern, plen)) {
                return true;
            }
        }
        return false;
    } else {
        // Handle regular character matching
        if (slen == 0) {
            return false;
        }
        if (*string != *pattern) {
            return false;
        }
        return Glob(string + 1, slen - 1, pattern + 1, plen - 1);
    }
}

namespace duckdb {

ThreeFSParams ThreeFSParams::ReadFrom(optional_ptr<FileOpener> opener)
{
  ThreeFSParams params;

  if (!opener) {
    return params;
  }

  // Set cluster name
  Value value;
  if (opener->TryGetCurrentSetting("threefs_cluster", value)) {
    params.cluster_name = value.ToString();
  }

  // Set mount root directory
  if (opener->TryGetCurrentSetting("threefs_mount_root", value)) {
    params.mount_root = value.ToString();
  }

  // Set enable debug logging
  if (opener->TryGetCurrentSetting("threefs_enable_debug_logging", value)) {
    params.enable_debug_logging = value.GetValue<bool>();
  }

  // Set whether to use USRBIO
  if (opener->TryGetCurrentSetting("threefs_use_usrbio", value)) {
    params.use_usrbio = value.GetValue<bool>();
  }

  // Set shared memory size
  if (opener->TryGetCurrentSetting("threefs_iov_size", value)) {
    params.iov_size = value.GetValue<size_t>();
  }

  // Set maximum number of IO ring requests
  if (opener->TryGetCurrentSetting("threefs_ior_entries", value)) {
    params.ior_entries = value.GetValue<size_t>();
  }

  // Set IO batch processing depth
  if (opener->TryGetCurrentSetting("threefs_io_depth", value)) {
    params.io_depth = value.GetValue<size_t>();
  }

  // Set IO timeout
  if (opener->TryGetCurrentSetting("threefs_ior_timeout", value)) {
    params.ior_timeout = value.GetValue<int>();
  }

  return params;
}

class USRBIOResourceManager {
 public:
  // Singleton pattern to get instance
  static USRBIOResourceManager *instance;

  USRBIOResourceManager() {}
  // Get current thread's USRBIO resources
  struct ThreadUSRBIOResource *GetThreadResource(const ThreeFSParams &params);

  // Global resource cleanup
  ~USRBIOResourceManager();

 private:
  USRBIOResourceManager(const USRBIOResourceManager &) = delete;
  USRBIOResourceManager &operator=(const USRBIOResourceManager &) = delete;

  // Thread resources map protection lock
  std::mutex resource_map_mutex;

  // ThreadID to resource mapping
  std::unordered_map<std::thread::id, struct ThreadUSRBIOResource *>
      thread_resources;
};

// Thread level USRBIO resource structure
struct ThreadUSRBIOResource {
  // USRBIO resources
  struct hf3fs_iov iov;
  struct hf3fs_ior ior_read;
  struct hf3fs_ior ior_write;

  // Resource initialization status
  bool initialized;

  // Resource belongs to parameters
  ThreeFSParams params;

  ThreadUSRBIOResource() : initialized(false) {}

  // Initialize resource
  bool Initialize(const ThreeFSParams &params);

  // Cleanup resource
  void Cleanup();

  ~ThreadUSRBIOResource() { Cleanup(); }
};

// threefs.cpp 中添加
bool ThreadUSRBIOResource::Initialize(const ThreeFSParams &params) {
  if (initialized) {
    return true;
  }

  this->params = params;

  // Create shared memory
  int ret =
      hf3fs_iovcreate(&iov, params.mount_root.c_str(), params.iov_size, 0, -1);
  if (ret < 0) {
    return false;
  }

  // Create read I/O ring
  ret =
      hf3fs_iorcreate4(&ior_read, params.mount_root.c_str(), params.ior_entries,
                       true, params.io_depth, params.ior_timeout, -1, 0);
  if (ret < 0) {
    hf3fs_iovdestroy(&iov);
    return false;
  }

  // Create write I/O ring
  ret = hf3fs_iorcreate4(&ior_write, params.mount_root.c_str(),
                         params.ior_entries, false, params.io_depth,
                         params.ior_timeout, -1, 0);
  if (ret < 0) {
    hf3fs_iordestroy(&ior_read);
    hf3fs_iovdestroy(&iov);
    return false;
  }

  initialized = true;
  return true;
}

void ThreadUSRBIOResource::Cleanup() {
  if (!initialized) {
    return;
  }

  // Destroy USRBIO resources
  hf3fs_iordestroy(&ior_write);
  hf3fs_iordestroy(&ior_read);
  hf3fs_iovdestroy(&iov);

  initialized = false;
}

// Resource manager implementation
struct ThreadUSRBIOResource *USRBIOResourceManager::GetThreadResource(
    const ThreeFSParams &params) {
  std::thread::id thread_id = std::this_thread::get_id();

  {
    std::lock_guard<std::mutex> lock(resource_map_mutex);

    // Find if current thread already has resources
    auto it = thread_resources.find(thread_id);
    if (it != thread_resources.end()) {
      return it->second;
    }

    // Create new thread resources
    ThreadUSRBIOResource *resource = new ThreadUSRBIOResource();
    if (!resource->Initialize(params)) {
      delete resource;
      return nullptr;
    }

    // Store resource mapping
    thread_resources[thread_id] = resource;
    return resource;
  }
}

USRBIOResourceManager::~USRBIOResourceManager() {
  // Clean up all thread resources
  for (auto &pair : thread_resources) {
    delete pair.second;
  }
  thread_resources.clear();
}

// 添加静态成员变量的定义
USRBIOResourceManager* USRBIOResourceManager::instance = nullptr;

struct ThreeFSFileHandle : public FileHandle {
 public:
  ThreeFSFileHandle(FileSystem &file_system, string &path, int fd,
                    FileOpenFlags flags, ThreeFSParams params, bool append)
      : FileHandle(file_system, std::move(path), flags),
        fd(fd),
        params(params),
        is_append(append) {}
  ~ThreeFSFileHandle() override { ThreeFSFileHandle::Close(); }

  int fd;
  idx_t current_pos = 0;
  bool is_append = false;
  ThreeFSParams params;

  void Close() override {
    if (fd != -1) {
      if (params.enable_debug_logging) {
        fprintf(stderr, "Closing file handle: %s, fd: %d\n", path.c_str(), fd);
      }
      hf3fs_dereg_fd(fd);
      close(fd);
      fd = -1;
    }
  };
};


bool ThreeFSFileSystem::FileExists(const string &filename,
                                   optional_ptr<FileOpener> opener) {
  if (!filename.empty()) {
    auto normalized_file = NormalizeLocalPath(filename);
    if (access(normalized_file, 0) == 0) {
      struct stat status;
      stat(normalized_file, &status);
      if (S_ISREG(status.st_mode)) {
        return true;
      }
    }
  }
  // if any condition fails
  return false;
}

bool ThreeFSFileSystem::IsPipe(const string &filename,
                               optional_ptr<FileOpener> opener) {
  if (!filename.empty()) {
    auto normalized_file = NormalizeLocalPath(filename);
    if (access(normalized_file, 0) == 0) {
      struct stat status;
      stat(normalized_file, &status);
      if (S_ISFIFO(status.st_mode)) {
        return true;
      }
    }
  }
  // if any condition fails
  return false;
}


static FileType GetFileTypeInternal(int fd) {  // LCOV_EXCL_START
  struct stat s;
  if (fstat(fd, &s) == -1) {
    return FileType::FILE_TYPE_INVALID;
  }
  switch (s.st_mode & S_IFMT) {
    case S_IFBLK:
      return FileType::FILE_TYPE_BLOCKDEV;
    case S_IFCHR:
      return FileType::FILE_TYPE_CHARDEV;
    case S_IFIFO:
      return FileType::FILE_TYPE_FIFO;
    case S_IFDIR:
      return FileType::FILE_TYPE_DIR;
    case S_IFLNK:
      return FileType::FILE_TYPE_LINK;
    case S_IFREG:
      return FileType::FILE_TYPE_REGULAR;
    case S_IFSOCK:
      return FileType::FILE_TYPE_SOCKET;
    default:
      return FileType::FILE_TYPE_INVALID;
  }
}  // LCOV_EXCL_STOP

bool ThreeFSFileSystem::IsPrivateFile(const string &path_p,
                                      FileOpener *opener) {
  auto path = FileSystem::ExpandPath(path_p, opener);
  auto normalized_path = NormalizeLocalPath(path);

  struct stat st;

  if (lstat(normalized_path, &st) != 0) {
    throw IOException(
        "Failed to stat '%s' when checking file permissions, file may be "
        "missing or have incorrect permissions",
        path.c_str());
  }

  // If group or other have any permission, the file is not private
  if (st.st_mode &
      (S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH)) {
    return false;
  }

  return true;
}

unique_ptr<FileHandle> ThreeFSFileSystem::OpenFile(
    const string &path_p, FileOpenFlags flags,
    optional_ptr<FileOpener> opener) {
  auto path = FileSystem::ExpandPath(path_p, opener);
  auto normalized_path = NormalizeLocalPath(path);
  if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
    throw NotImplementedException(
        "Unsupported compression type for default file system");
  }

  flags.Verify();

  int open_flags = 0;
  int rc;
  bool open_read = flags.OpenForReading();
  bool open_write = flags.OpenForWriting();
  if (open_read && open_write) {
    open_flags = O_RDWR;
  } else if (open_read) {
    open_flags = O_RDONLY;
  } else if (open_write) {
    open_flags = O_WRONLY;
  } else {
    throw InternalException(
        "READ, WRITE or both should be specified when opening a file");
  }
  if (open_write) {
    // need Read or Write
    D_ASSERT(flags.OpenForWriting());
    open_flags |= O_CLOEXEC;
    if (flags.CreateFileIfNotExists()) {
      open_flags |= O_CREAT;
    } else if (flags.OverwriteExistingFile()) {
      open_flags |= O_CREAT | O_TRUNC;
    }
    if (flags.OpenForAppending()) {
      open_flags |= O_APPEND;
    }
  }
  if (flags.DirectIO()) {
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
    // OSX does not have O_DIRECT, instead we need to use fcntl afterwards to
    // support direct IO
#else
    open_flags |= O_DIRECT;
#endif
  }

  // Determine permissions
  mode_t filesec;
  if (flags.CreatePrivateFile()) {
    open_flags |= O_EXCL;  // Ensure we error on existing files or the
                           // permissions may not set
    filesec = 0600;
  } else {
    filesec = 0666;
  }

  if (flags.ExclusiveCreate()) {
    open_flags |= O_EXCL;
  }

  // Open the file
  int fd = open(normalized_path, open_flags, filesec);

  if (fd == -1) {
    if (flags.ReturnNullIfNotExists() && errno == ENOENT) {
      return nullptr;
    }
    if (flags.ReturnNullIfExists() && errno == EEXIST) {
      return nullptr;
    }
    throw IOException("Cannot open file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, path,
                      strerror(errno));
  }

#if defined(__DARWIN__) || defined(__APPLE__)
  if (flags.DirectIO()) {
    // OSX requires fcntl for Direct IO
    rc = fcntl(fd, F_NOCACHE, 1);
    if (rc == -1) {
      throw IOException("Could not enable direct IO for file \"%s\": %s", path,
                        strerror(errno));
    }
  }
#endif

  if (flags.Lock() != FileLockType::NO_LOCK) {
    // set lock on file
    // but only if it is not an input/output stream
    auto file_type = GetFileTypeInternal(fd);
    if (file_type != FileType::FILE_TYPE_FIFO &&
        file_type != FileType::FILE_TYPE_SOCKET) {
      struct flock fl;
      memset(&fl, 0, sizeof fl);
      fl.l_type = flags.Lock() == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
      fl.l_whence = SEEK_SET;
      fl.l_start = 0;
      fl.l_len = 0;
      rc = fcntl(fd, F_SETLK, &fl);
      // Retain the original error.
      int retained_errno = errno;
      bool has_error = rc == -1;
      string extended_error;
      if (has_error) {
        if (retained_errno == ENOTSUP) {
          // file lock not supported for this file system
          if (flags.Lock() == FileLockType::READ_LOCK) {
            // for read-only, we ignore not-supported errors
            has_error = false;
            errno = 0;
          } else {
            extended_error =
                "File locks are not supported for this file system, cannot "
                "open the file in "
                "read-write mode. Try opening the file in read-only mode";
          }
        }
      }
      if (has_error) {
        if (extended_error.empty()) {
          // try to find out who is holding the lock using F_GETLK
          rc = fcntl(fd, F_GETLK, &fl);
          if (rc == -1) {  // fnctl does not want to help us
            extended_error = strerror(errno);
          }
          if (flags.Lock() == FileLockType::WRITE_LOCK) {
            // maybe we can get a read lock instead and tell this to the user.
            fl.l_type = F_RDLCK;
            rc = fcntl(fd, F_SETLK, &fl);
            if (rc != -1) {  // success!
              extended_error +=
                  ". However, you would be able to open this database in "
                  "read-only mode, e.g. by "
                  "using the -readonly parameter in the CLI";
            }
          }
        }
        rc = close(fd);
        if (rc == -1) {
          extended_error += ". Also, failed closing file";
        }
        extended_error +=
            ". See also https://duckdb.org/docs/connect/concurrency";
        throw IOException("Could not set lock on file \"%s\": %s",
                          {{"errno", std::to_string(retained_errno)}}, path,
                          extended_error);
      }
    }
  }

  // Register file descriptor
  rc = hf3fs_reg_fd(fd, 0);
  // hf3fs_reg_fd indicates if the file descriptor less than 0, it means the file descriptor
  // is registered
  if (rc > 0) {
    throw IOException("Failed to register file descriptor: " + std::to_string(fd),
                      {{"errno", std::to_string(rc)}});
  }

  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "OpenFile: File handle: %s, fd: %d, is_append: %d\n", path.c_str(), fd, open_flags & O_APPEND);
  }
  return make_uniq<ThreeFSFileHandle>(*this, path, fd, flags, params, open_flags & O_APPEND);
}

void ThreeFSFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  off_t offset = lseek(fd, UnsafeNumericCast<off_t>(location), SEEK_SET);
  if (offset == (off_t)-1) {
    throw IOException("Could not seek to location %lu for file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, location, handle.path,
                      strerror(errno));
  }
}

idx_t ThreeFSFileSystem::GetFilePointer(FileHandle &handle) {
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  off_t position = lseek(fd, 0, SEEK_CUR);
  if (position == (off_t)-1) {
    throw IOException("Could not get file position file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, handle.path,
                      strerror(errno));
  }
  return UnsafeNumericCast<idx_t>(position);
}

// ReadImpl is the internal implementation of Read, it's used to handle the case when the file is read from a specific location
// This funcation won't modify the file descriptor position
int64_t ThreeFSFileSystem::ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();
  // Get current thread's USRBIO resources
  ThreadUSRBIOResource *resource =
      USRBIOResourceManager::instance->GetThreadResource(
          threefs_handle.params);
  if (!resource || !resource->initialized) {
    throw IOException("Read: Failed to initialize USRBIO for thread");
  }

  uint8_t *buf_ptr = static_cast<uint8_t *>(buffer);
  int64_t bytes_remaining = nr_bytes;
  idx_t current_offset = location;
  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "Read: File handle: %s, location: %lu, nr_bytes: %ld\n", threefs_handle.path.c_str(), current_offset, nr_bytes);
  }

  // Block processing large data
  while (bytes_remaining > 0) {
    // Determine current block size
    size_t current_chunk_size =
        std::min<size_t>(bytes_remaining, resource->params.iov_size);

    // Prepare I/O request
    if (threefs_handle.params.enable_debug_logging) {
      fprintf(stderr, "Prepare read I/O request for file %s, location: %lu, nr_bytes: %ld\n", threefs_handle.path.c_str(), current_offset, current_chunk_size);
    }
    int ret =
        hf3fs_prep_io(&resource->ior_read, &resource->iov,
                      true,                // Read operation
                      resource->iov.base,  // Use shared memory
                      threefs_handle.fd, current_offset, current_chunk_size,
                      nullptr  // User data
        );

    if (ret < 0) {
      throw IOException("Failed to prepare read I/O: " + std::to_string(-ret));
    }

    // Submit I/O request
    ret = hf3fs_submit_ios(&resource->ior_read);
    if (ret < 0) {
      throw IOException("Failed to submit read I/O: " + std::to_string(-ret));
    }

    // Wait for I/O to complete
    struct hf3fs_cqe cqes[1];
    ret = hf3fs_wait_for_ios(&resource->ior_read, cqes, 1, 1, nullptr);
    if (ret < 0) {
      throw IOException("Failed to wait for read I/O: " + std::to_string(-ret));
    }

    // Check completion status
    if (cqes[0].result < 0) {
      throw IOException("Read I/O failed: " + std::to_string(-cqes[0].result));
    }

    // Get actual read bytes
    size_t bytes_read = cqes[0].result;
    if (bytes_read == 0 && bytes_remaining > 0) {
      // Reached end of file
      break;
    }

    // Copy data from shared memory to user buffer
    memcpy(buf_ptr, resource->iov.base, bytes_read);

    // Update pointer and count
    buf_ptr += bytes_read;
    bytes_remaining -= bytes_read;
    current_offset += bytes_read;

    // If read bytes are less than requested bytes, it means end of file
    if (bytes_read < current_chunk_size) {
      break;
    }
  }

  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "Successfully read %ld bytes from offset %lu of file %s\n", nr_bytes - bytes_remaining, location, threefs_handle.path.c_str());
  }
  return nr_bytes - bytes_remaining;
}

int64_t ThreeFSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();
  idx_t location = threefs_handle.current_pos;
  try {
    auto bytes_read = ReadImpl(threefs_handle, buffer, nr_bytes, location);
    threefs_handle.current_pos += bytes_read;
    return bytes_read;
  } catch (const IOException &e) {
    throw;
  }
}

void ThreeFSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();

  try {
    ReadImpl(threefs_handle, buffer, nr_bytes, location);
  } catch (const IOException &e) {
    throw;
  }
}


// This funcation won't modify the file descriptor position
void ThreeFSFileSystem::Write(FileHandle &handle, void *buffer,
                              int64_t nr_bytes, idx_t location) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();

  // Get current thread's USRBIO resources
  ThreadUSRBIOResource *resource =
      USRBIOResourceManager::instance->GetThreadResource(
          threefs_handle.params);
  if (!resource || !resource->initialized) {
    throw IOException("Write: Failed to initialize USRBIO for thread");
  }

  const uint8_t *buf_ptr = static_cast<const uint8_t *>(buffer);
  int64_t bytes_remaining = nr_bytes;
  idx_t current_offset = location;
  if (threefs_handle.is_append) {
    current_offset = GetFileSize(handle);
  }

  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "Write: File handle: %s, location: %lu, nr_bytes: %ld\n", threefs_handle.path.c_str(), current_offset, nr_bytes);
  }

  // Block processing large data
  while (bytes_remaining > 0) {
    // Determine current block size
    size_t current_chunk_size =
        std::min<size_t>(bytes_remaining, resource->params.iov_size);

    // Copy data to shared memory
    memcpy(resource->iov.base, buf_ptr, current_chunk_size);

    // Prepare I/O request
    if (threefs_handle.params.enable_debug_logging) {
      fprintf(stderr, "Prepare write I/O request for file %s, location: %lu, nr_bytes: %ld\n", threefs_handle.path.c_str(), current_offset, current_chunk_size);
    }

    int ret =
        hf3fs_prep_io(&resource->ior_write, &resource->iov,
                      false,               // Write operation
                      resource->iov.base,  // Use shared memory
                      threefs_handle.fd, current_offset, current_chunk_size,
                      nullptr  // User data
        );

    if (ret < 0) {
      throw IOException("Failed to prepare write I/O: " + std::to_string(-ret));
    }

    // Submit I/O request
    ret = hf3fs_submit_ios(&resource->ior_write);

    if (ret < 0) {
      throw IOException("Failed to submit write I/O: " + std::to_string(-ret));
    }

    // Wait for I/O to complete
    struct hf3fs_cqe cqes[1];
    ret = hf3fs_wait_for_ios(&resource->ior_write, cqes, 1, 1, nullptr);
    if (ret < 0) {
      throw IOException("Failed to wait for write I/O: " +
                        std::to_string(-ret));
    }

    // Check completion status
    if (cqes[0].result < 0) {
      throw IOException("Write I/O failed: " + std::to_string(-cqes[0].result));
    }

    // Get actual written bytes
    size_t bytes_written = cqes[0].result;
    if (bytes_written != current_chunk_size) {
      throw IOException("Could not write all bytes to file \"" +
                        threefs_handle.path + "\": wrote " +
                        std::to_string(bytes_written) + "/" +
                        std::to_string(current_chunk_size) + " bytes");
    }

    // Update pointer and count
    buf_ptr += bytes_written;
    bytes_remaining -= bytes_written;
    current_offset += bytes_written;
  }

  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "Successfully written %ld bytes to offset %lu of file %s\n", nr_bytes - bytes_remaining, location, threefs_handle.path.c_str());
  }
}

int64_t ThreeFSFileSystem::Write(FileHandle &handle, void *buffer,
                                 int64_t nr_bytes) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();

  Write(handle, buffer, nr_bytes, threefs_handle.current_pos);
  // Follow posix standard, if the file is opened for appending, the file pointer won't be updated
  if (!threefs_handle.is_append) {
    threefs_handle.current_pos += nr_bytes;
  }

  // Return actual written bytes
  return nr_bytes;
}

bool ThreeFSFileSystem::Trim(FileHandle &handle, idx_t offset_bytes,
                             idx_t length_bytes) {
#if defined(__linux__)
  // FALLOC_FL_PUNCH_HOLE requires glibc 2.18 or up
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 18)
  return false;
#else
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  int res = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                      UnsafeNumericCast<int64_t>(offset_bytes),
                      UnsafeNumericCast<int64_t>(length_bytes));
  return res == 0;
#endif
#else
  return false;
#endif
}

int64_t ThreeFSFileSystem::GetFileSize(FileHandle &handle) {
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  struct stat s;
  if (fstat(fd, &s) == -1) {
    throw IOException("Failed to get file size for file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, handle.path,
                      strerror(errno));
  }
  return s.st_size;
}

time_t ThreeFSFileSystem::GetLastModifiedTime(FileHandle &handle) {
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  struct stat s;
  if (fstat(fd, &s) == -1) {
    throw IOException("Failed to get last modified time for file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, handle.path,
                      strerror(errno));
  }
  return s.st_mtime;
}

FileType ThreeFSFileSystem::GetFileType(FileHandle &handle) {
  int fd = handle.Cast<ThreeFSFileHandle>().fd;
  return GetFileTypeInternal(fd);
}

void ThreeFSFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();
  int fd = threefs_handle.fd;
  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "Truncate: File handle: %s, new_size: %ld\n", handle.path.c_str(), new_size);
  }
  if (ftruncate(fd, new_size) != 0) {
    throw IOException("Could not truncate file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, handle.path,
                      strerror(errno));
  }
}

bool ThreeFSFileSystem::DirectoryExists(const string &directory,
                                      optional_ptr<FileOpener> opener) {
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "DirectoryExists: directory: %s\n", directory.c_str());
  }
  if (!directory.empty()) {
    auto normalized_dir = NormalizeLocalPath(directory);
    if (access(normalized_dir, 0) == 0) {
      struct stat status;
      stat(normalized_dir, &status);
      if (status.st_mode & S_IFDIR) {
        return true;
      }
    }
  }
  // if any condition fails
  return false;
}

void ThreeFSFileSystem::CreateDirectory(const string &directory,
                                        optional_ptr<FileOpener> opener) {
  struct stat st;
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "CreateDirectory: directory: %s\n", directory.c_str());
  }
  auto normalized_dir = NormalizeLocalPath(directory);
  if (stat(normalized_dir, &st) != 0) {
    /* Directory does not exist. EEXIST for race condition */
    if (mkdir(normalized_dir, 0755) != 0 && errno != EEXIST) {
      throw IOException("Failed to create directory \"%s\": %s",
                        {{"errno", std::to_string(errno)}}, directory,
                        strerror(errno));
    }
  } else if (!S_ISDIR(st.st_mode)) {
    throw IOException(
        "Failed to create directory \"%s\": path exists but is not a "
        "directory!",
        {{"errno", std::to_string(errno)}}, directory);
  }
}

int RemoveDirectoryFastOrRecursive(const char *path,
                                   optional_ptr<FileOpener> opener) {
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "RemoveDirectoryFastOrRecursive: path: %s\n", path);
  }
  // Check if the path is on a 3fs filesystem
  char hf3fs_mount_point[256];
  
  // Try to extract the mount point
  int extract_result = hf3fs_extract_mount_point(hf3fs_mount_point, sizeof(hf3fs_mount_point), path);
  if (extract_result > 0) {
    // Path is on 3fs filesystem, use the efficient deletion method
    try {
      // Create a symlink in the rm-rf directory with a timestamped name
      char timestamp[32];
      snprintf(timestamp, sizeof(timestamp), "%ld", static_cast<long>(time(nullptr)));
      
      // Create the link path: <mountpoint>/3fs-virt/rm-rf/<basename>-<timestamp>
      std::string basename = std::string(path).substr(std::string(path).find_last_of("/\\") + 1);
      std::string link_path = std::string(hf3fs_mount_point) + "/3fs-virt/rm-rf/" + basename + "-" + timestamp;
      
      // Create the symlink
      if (symlink(path, link_path.c_str()) == 0) {
        return 0; // Successfully created symlink for background deletion
      }
    } catch (const std::exception &e) {
      // We'll fall back to regular recursive deletion on any failure
    }
  }
  
  // If not on 3fs or the 3fs method failed, use regular recursive deletion
  DIR *d = opendir(path);
  idx_t path_len = (idx_t)strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;
    r = 0;
    while (!r && (p = readdir(d))) {
      int r2 = -1;
      char *buf;
      idx_t len;
      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
        continue;
      }
      len = path_len + (idx_t)strlen(p->d_name) + 2;
      buf = new (std::nothrow) char[len];
      if (buf) {
        struct stat statbuf;
        snprintf(buf, len, "%s/%s", path, p->d_name);
        if (!stat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode)) {
            r2 = RemoveDirectoryFastOrRecursive(buf, opener);
          } else {
            r2 = unlink(buf);
          }
        }
        delete[] buf;
      }
      r = r2;
    }
    closedir(d);
  }
  if (!r) {
    r = rmdir(path);
  }
  return r;
}

void ThreeFSFileSystem::RemoveDirectory(const string &directory,
                                        optional_ptr<FileOpener> opener) {
  auto normalized_dir = NormalizeLocalPath(directory);
  RemoveDirectoryFastOrRecursive(normalized_dir, opener);
}

void ThreeFSFileSystem::RemoveFile(const string &filename,
                                   optional_ptr<FileOpener> opener) {
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "RemoveFile: filename: %s\n", filename.c_str());
  }
  auto normalized_file = NormalizeLocalPath(filename);
  if (std::remove(normalized_file) != 0) {
    throw IOException("Could not remove file \"%s\": %s",
                      {{"errno", std::to_string(errno)}}, filename,
                      strerror(errno));
  }
}

bool ThreeFSFileSystem::ListFiles(
    const string &directory,
    const std::function<void(const string &, bool)> &callback,
    FileOpener *opener) {
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "ListFiles: directory: %s\n", directory.c_str());
  }
  auto normalized_dir = NormalizeLocalPath(directory);
  auto dir = opendir(normalized_dir);
  if (!dir) {
    return false;
  }

  // RAII wrapper around DIR to automatically free on exceptions in callback
  std::unique_ptr<DIR, std::function<void(DIR *)>> dir_unique_ptr(
      dir, [](DIR *d) { closedir(d); });

  struct dirent *ent;
  // loop over all files in the directory
  while ((ent = readdir(dir)) != nullptr) {
    string name = string(ent->d_name);
    // skip . .. and empty files
    if (name.empty() || name == "." || name == "..") {
      continue;
    }
    // now stat the file to figure out if it is a regular file or directory
    string full_path = JoinPath(normalized_dir, name);
    struct stat status;
    auto res = stat(full_path.c_str(), &status);
    if (res != 0) {
      continue;
    }
    if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
      // not a file or directory: skip
      continue;
    }
    // invoke callback
    callback(name, status.st_mode & S_IFDIR);
  }

  return true;
}

void ThreeFSFileSystem::FileSync(FileHandle &handle) {
  auto &threefs_handle = handle.Cast<ThreeFSFileHandle>();
  int fd = threefs_handle.fd;
  if (threefs_handle.params.enable_debug_logging) {
    fprintf(stderr, "FileSync: File handle: %s, fd: %d\n", handle.path.c_str(), fd);
  }
  if (fsync(fd) != 0) {
    throw FatalException("fsync failed!");
  }
}

void ThreeFSFileSystem::MoveFile(const string &source, const string &target,
                                 optional_ptr<FileOpener> opener) {
  auto normalized_source = NormalizeLocalPath(source);
  auto normalized_target = NormalizeLocalPath(target);
  ThreeFSParams params = ThreeFSParams::ReadFrom(opener);
  if (params.enable_debug_logging) {
    fprintf(stderr, "MoveFile: source: %s, target: %s\n", normalized_source, normalized_target);
  }
  //! FIXME: rename does not guarantee atomicity or overwriting target file if
  //! it exists
  if (rename(normalized_source, normalized_target) != 0) {
    throw IOException("Could not rename file!",
                      {{"errno", std::to_string(errno)}});
  }
}

std::string ThreeFSFileSystem::GetLastErrorAsString() { return string(); }

bool ThreeFSFileSystem::CanSeek() { return true; }

bool ThreeFSFileSystem::OnDiskFile(FileHandle &handle) { return true; }

void ThreeFSFileSystem::Seek(FileHandle &handle, idx_t location) {
  if (!CanSeek()) {
    throw IOException("Cannot seek in files of this type");
  }
  SetFilePointer(handle, location);
}

idx_t ThreeFSFileSystem::SeekPosition(FileHandle &handle) {
  if (!CanSeek()) {
    throw IOException("Cannot seek in files of this type");
  }
  return GetFilePointer(handle);
}

static bool IsCrawl(const string &glob) {
  // glob must match exactly
  return glob == "**";
}
static bool HasMultipleCrawl(const vector<string> &splits) {
  return std::count(splits.begin(), splits.end(), "**") > 1;
}
static bool IsSymbolicLink(const string &path) {
  auto normalized_path = ThreeFSFileSystem::NormalizeLocalPath(path);
  struct stat status;
  return (lstat(normalized_path, &status) != -1 && S_ISLNK(status.st_mode));
}

static void RecursiveGlobDirectories(FileSystem &fs, const string &path,
                                     vector<string> &result,
                                     bool match_directory, bool join_path) {
  fs.ListFiles(path, [&](const string &fname, bool is_directory) {
    string concat;
    if (join_path) {
      concat = fs.JoinPath(path, fname);
    } else {
      concat = fname;
    }
    if (IsSymbolicLink(concat)) {
      return;
    }
    if (is_directory == match_directory) {
      result.push_back(concat);
    }
    if (is_directory) {
      RecursiveGlobDirectories(fs, concat, result, match_directory, true);
    }
  });
}

static void GlobFilesInternal(FileSystem &fs, const string &path,
                              const string &glob, bool match_directory,
                              vector<string> &result, bool join_path) {
  fs.ListFiles(path, [&](const string &fname, bool is_directory) {
    if (is_directory != match_directory) {
      return;
    }
    if (Glob(fname.c_str(), fname.size(), glob.c_str(), glob.size())) {
      if (join_path) {
        result.push_back(fs.JoinPath(path, fname));
      } else {
        result.push_back(fname);
      }
    }
  });
}

vector<string> ThreeFSFileSystem::FetchFileWithoutGlob(const string &path,
                                                       FileOpener *opener,
                                                       bool absolute_path) {
  vector<string> result;
  if (FileExists(path, opener) || IsPipe(path, opener)) {
    result.push_back(path);
  } else if (!absolute_path) {
    Value value;
    if (opener && opener->TryGetCurrentSetting("file_search_path", value)) {
      auto search_paths_str = value.ToString();
      vector<std::string> search_paths =
          StringUtil::Split(search_paths_str, ',');
      for (const auto &search_path : search_paths) {
        auto joined_path = JoinPath(search_path, path);
        if (FileExists(joined_path, opener) || IsPipe(joined_path, opener)) {
          result.push_back(joined_path);
        }
      }
    }
  }
  return result;
}

// Helper function to handle 3fs:/ URLs
static idx_t GetFileUrlOffset(const string &path) {
  if (!StringUtil::StartsWith(path, "3fs:/")) {
    return 0;
  }

  // Url without host: 3fs:/some/path
  if (path[6] != '/') {
    return 5;
  }

  // Url with empty host: 3fs:///some/path
  if (path[7] == '/') {
    return 7;
  }

  // Url with localhost: 3fs://localhost/some/path
  if (path.compare(7, 10, "localhost/") == 0) {
    return 16;
  }

  // unkown 3fs:/ url format
  return 0;
}

const char *ThreeFSFileSystem::NormalizeLocalPath(const string &path) {
  return path.c_str() + GetFileUrlOffset(path);
}

vector<string> ThreeFSFileSystem::Glob(const string &path, FileOpener *opener) {
  if (path.empty()) {
    return vector<string>();
  }
  // split up the path into separate chunks
  vector<string> splits;

  bool is_file_url = StringUtil::StartsWith(path, "file:/");
  idx_t file_url_path_offset = GetFileUrlOffset(path);

  idx_t last_pos = 0;
  for (idx_t i = file_url_path_offset; i < path.size(); i++) {
    if (path[i] == '\\' || path[i] == '/') {
      if (i == last_pos) {
        // empty: skip this position
        last_pos = i + 1;
        continue;
      }
      if (splits.empty()) {
        //				splits.push_back(path.substr(file_url_path_offset,
        // i-file_url_path_offset));
        splits.push_back(path.substr(0, i));
      } else {
        splits.push_back(path.substr(last_pos, i - last_pos));
      }
      last_pos = i + 1;
    }
  }
  splits.push_back(path.substr(last_pos, path.size() - last_pos));
  // handle absolute paths
  bool absolute_path = false;
  if (IsPathAbsolute(path)) {
    // first character is a slash -  unix absolute path
    absolute_path = true;
  } else if (StringUtil::Contains(splits[0],
                                  ":")) {  // TODO: this is weird? shouldn't
                                           // IsPathAbsolute handle this?
    // first split has a colon -  windows absolute path
    absolute_path = true;
  } else if (splits[0] == "~") {
    // starts with home directory
    auto home_directory = GetHomeDirectory(opener);
    if (!home_directory.empty()) {
      absolute_path = true;
      splits[0] = home_directory;
      D_ASSERT(path[0] == '~');
      if (!HasGlob(path)) {
        return Glob(home_directory + path.substr(1));
      }
    }
  }
  // Check if the path has a glob at all
  if (!HasGlob(path)) {
    // no glob: return only the file (if it exists or is a pipe)
    return FetchFileWithoutGlob(path, opener, absolute_path);
  }
  vector<string> previous_directories;
  if (absolute_path) {
    // for absolute paths, we don't start by scanning the current directory
    previous_directories.push_back(splits[0]);
  } else {
    // If file_search_path is set, use those paths as the first glob elements
    Value value;
    if (opener && opener->TryGetCurrentSetting("file_search_path", value)) {
      auto search_paths_str = value.ToString();
      vector<std::string> search_paths =
          StringUtil::Split(search_paths_str, ',');
      for (const auto &search_path : search_paths) {
        previous_directories.push_back(search_path);
      }
    }
  }

  if (HasMultipleCrawl(splits)) {
    throw IOException("Cannot use multiple \'**\' in one path");
  }

  idx_t start_index;
  if (is_file_url) {
    start_index = 1;
  } else if (absolute_path) {
    start_index = 1;
  } else {
    start_index = 0;
  }

  for (idx_t i = start_index ? 1 : 0; i < splits.size(); i++) {
    bool is_last_chunk = i + 1 == splits.size();
    bool has_glob = HasGlob(splits[i]);
    // if it's the last chunk we need to find files, otherwise we find
    // directories not the last chunk: gather a list of all directories that
    // match the glob pattern
    vector<string> result;
    if (!has_glob) {
      // no glob, just append as-is
      if (previous_directories.empty()) {
        result.push_back(splits[i]);
      } else {
        if (is_last_chunk) {
          for (auto &prev_directory : previous_directories) {
            const string filename = JoinPath(prev_directory, splits[i]);
            if (FileExists(filename, opener) ||
                DirectoryExists(filename, opener)) {
              result.push_back(filename);
            }
          }
        } else {
          for (auto &prev_directory : previous_directories) {
            result.push_back(JoinPath(prev_directory, splits[i]));
          }
        }
      }
    } else {
      if (IsCrawl(splits[i])) {
        if (!is_last_chunk) {
          result = previous_directories;
        }
        if (previous_directories.empty()) {
          RecursiveGlobDirectories(*this, ".", result, !is_last_chunk, false);
        } else {
          for (auto &prev_dir : previous_directories) {
            RecursiveGlobDirectories(*this, prev_dir, result, !is_last_chunk,
                                     true);
          }
        }
      } else {
        if (previous_directories.empty()) {
          // no previous directories: list in the current path
          GlobFilesInternal(*this, ".", splits[i], !is_last_chunk, result,
                            false);
        } else {
          // previous directories
          // we iterate over each of the previous directories, and apply the
          // glob of the current directory
          for (auto &prev_directory : previous_directories) {
            GlobFilesInternal(*this, prev_directory, splits[i], !is_last_chunk,
                              result, true);
          }
        }
      }
    }
    if (result.empty()) {
      // no result found that matches the glob
      // last ditch effort: search the path as a string literal
      return FetchFileWithoutGlob(path, opener, absolute_path);
    }
    if (is_last_chunk) {
      return result;
    }
    previous_directories = std::move(result);
  }
  return vector<string>();
}

// Initialize 3FS resources
void InitializeThreeFS() {
  //fprintf(stderr, "InitializeThreeFS\n");
  USRBIOResourceManager::instance = new USRBIOResourceManager();
}

void DeinitializeThreeFS() {
  //fprintf(stderr, "DeinitializeThreeFS\n");
  if (USRBIOResourceManager::instance) {
    delete USRBIOResourceManager::instance;
    USRBIOResourceManager::instance = nullptr;
  }
}

bool ThreeFSFileSystem::CanHandleFile(const string &fpath) {
  // Check if the path starts with "3fs://" or "/3fs/"
  return (StringUtil::StartsWith(fpath, "3fs://") || StringUtil::StartsWith(fpath, "/3fs/"));
}

}  // namespace duckdb
