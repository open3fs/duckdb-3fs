#!/bin/bash

# 检查 /3fs 挂载点是否存在
if [ ! -d "/3fs" ]; then
    echo -e "\033[1;31mERROR: /3fs mount point not found!\033[0m"
    echo -e "\033[1;31mPlease mount 3fs filesystem before running tests.\033[0m"
    echo -e "\033[1;31mExample: mount -t 3fs <options> /3fs\033[0m"
    exit 1
fi

# 检查 /3fs 是否确实是一个挂载点，而不仅仅是一个目录
if ! mountpoint -q /3fs; then
    echo -e "\033[1;31mERROR: /3fs exists but is not a mount point!\033[0m"
    echo -e "\033[1;31mPlease mount 3fs filesystem before running tests.\033[0m"
    echo -e "\033[1;31mExample: mount -t 3fs <options> /3fs\033[0m"
    exit 1
fi

# 清理测试目录的函数
cleanup_test_dirs() {
    echo -e "\033[1;33mCleaning up test directories...\033[0m"
    rm -rf /3fs/test_threefs
}

# 注册退出时的清理函数
#trap cleanup_test_dirs EXIT

# 创建测试目录结构
mkdir -p /3fs/test_threefs
mkdir -p /3fs/test_threefs/basic_test/dir1
mkdir -p /3fs/test_threefs/basic_test/dir2
mkdir -p /3fs/test_threefs/io_test
mkdir -p /3fs/test_threefs/error_test
mkdir -p /3fs/test_threefs/error_test/special@#$chars
mkdir -p /3fs/test_threefs/perf_test
mkdir -p /3fs/test_threefs/concurrency_test
mkdir -p /3fs/test_threefs/integration_test
mkdir -p /3fs/test_threefs/integration_test/partitioned/gender=M
mkdir -p /3fs/test_threefs/integration_test/partitioned/gender=F

echo -e "\033[1;32m✓ 3FS mount point found at /3fs, proceeding with tests\033[0m"
exit 0 