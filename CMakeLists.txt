# CMakeLists.txt for the ThreeFS extension

# Set minimum required version of CMake
cmake_minimum_required(VERSION 3.12)

# Set C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set extension name
set(TARGET_NAME threefs)

# Set extension description
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

add_extension_definitions()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

# 3FS build options
option(BUILD_3FS_LIB "Build 3FS library from source" ON)

# Custom command to build 3FS library
if(BUILD_3FS_LIB)
  # Set 3FS source path
  set(3FS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/3fs")
  
  # Set 3FS build directory
  set(3FS_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/3fs_build")
  
  # Create build directory
  file(MAKE_DIRECTORY ${3FS_BUILD_DIR})
  
  # Add custom command to build 3FS library
  add_custom_command(
    OUTPUT ${3FS_BUILD_DIR}/src/lib/api/libhf3fs_api.a
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DENABLE_FUSE_APPLICATION=OFF -DOVERRIDE_CXX_NEW_DELETE=OFF ${3FS_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --parallel 8
    WORKING_DIRECTORY ${3FS_BUILD_DIR}
    COMMENT "Building 3FS library..."
    VERBATIM
  )
  
  # Add custom target
  add_custom_target(build_3fs DEPENDS ${3FS_BUILD_DIR}/src/lib/api/libhf3fs_api.a)
  
  # Set 3FS header and library directories
  set(DEEPSEEK_3FS_INCLUDE_DIR "${3FS_SOURCE_DIR}/src" CACHE PATH "Path to deepseek 3fs include directory" FORCE)
  set(DEEPSEEK_3FS_LIB_DIR "${3FS_BUILD_DIR}/src/lib/api" CACHE PATH "Path to deepseek 3fs library directory" FORCE)
else()
  # If not building from source, use provided paths
  set(DEEPSEEK_3FS_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/3fs/src" CACHE PATH "Path to deepseek 3fs include directory")
  set(DEEPSEEK_3FS_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/3fs/build/src/lib/api" CACHE PATH "Path to deepseek 3fs library directory")
endif()

# Include deepseek 3fs headers
if(NOT DEEPSEEK_3FS_INCLUDE_DIR STREQUAL "")
  include_directories(${DEEPSEEK_3FS_INCLUDE_DIR})
  # Add 3fs root directory to include paths
  include_directories(${CMAKE_CURRENT_SOURCE_DIR}/3fs)
endif()

# Set library directories
if(NOT DEEPSEEK_3FS_LIB_DIR STREQUAL "")
  # Set library path
  link_directories(${DEEPSEEK_3FS_LIB_DIR})
  
  # Add -Wl,--copy-dt-needed-entries link flag to include secondary dependencies
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--copy-dt-needed-entries")
endif()

# Add source files
set(EXTENSION_SOURCES 
    src/threefs_extension.cpp
    src/threefs.cpp
)

# Build static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Set extension information
set_target_properties(${EXTENSION_NAME} PROPERTIES
    EXPORT_NAME ${TARGET_NAME}
)

# Add dependencies to ensure 3FS library is built before the extension
if(BUILD_3FS_LIB)
  add_dependencies(${EXTENSION_NAME} build_3fs)
  add_dependencies(${LOADABLE_EXTENSION_NAME} build_3fs)
endif()

# Link 3FS libraries and dependencies
if(NOT DEEPSEEK_3FS_LIB_DIR STREQUAL "")
  # Static library only links API library
  target_link_libraries(${EXTENSION_NAME} hf3fs_api)
  
  # Loadable extension links shared library version of API to include all symbols
  target_link_libraries(${LOADABLE_EXTENSION_NAME} hf3fs_api_shared)
endif()

# Install targets
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}") 