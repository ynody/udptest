# 承認済みヘッダオンリー依存ライブラリを third_party/ に取得する。
# 実行方法: cmake -P scripts/download_dependencies.cmake

get_filename_component(PROJECT_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(THIRD_PARTY_ROOT "${PROJECT_ROOT}/third_party")
set(DOWNLOAD_CACHE "${THIRD_PARTY_ROOT}/.cache")
set(EXTRACT_ROOT "${THIRD_PARTY_ROOT}/.extract")

function(fetch_include_tree dependency version url archive_root archive_sha256)
  set(destination "${THIRD_PARTY_ROOT}/${dependency}")
  set(version_file "${destination}/.version")

  set(installation_complete TRUE)
  foreach(license_file IN LISTS ARGN)
    if(NOT EXISTS "${destination}/${license_file}")
      set(installation_complete FALSE)
    endif()
  endforeach()

  if(EXISTS "${version_file}")
    file(READ "${version_file}" installed_version)
    string(STRIP "${installed_version}" installed_version)
    if(installed_version STREQUAL version AND installation_complete)
      message(STATUS "${dependency} ${version} is already installed")
      return()
    endif()
  endif()

  file(MAKE_DIRECTORY "${DOWNLOAD_CACHE}")
  file(MAKE_DIRECTORY "${EXTRACT_ROOT}")

  set(archive "${DOWNLOAD_CACHE}/${dependency}-${version}.tar.gz")
  set(archive_is_valid FALSE)
  if(EXISTS "${archive}")
    file(SIZE "${archive}" archive_size)
    if(NOT archive_size EQUAL 0)
      file(SHA256 "${archive}" actual_sha256)
      if(actual_sha256 STREQUAL archive_sha256)
        set(archive_is_valid TRUE)
      endif()
    endif()
    if(NOT archive_is_valid)
      file(REMOVE "${archive}")
    endif()
  endif()
  if(archive_is_valid)
    message(STATUS "Using cached archive for ${dependency} ${version}")
  else()
    message(STATUS "Downloading ${dependency} ${version}")
    file(DOWNLOAD "${url}" "${archive}"
         STATUS download_status
         EXPECTED_HASH "SHA256=${archive_sha256}"
         TLS_VERIFY ON
         SHOW_PROGRESS)
    list(GET download_status 0 download_code)
    list(GET download_status 1 download_message)
    if(NOT download_code EQUAL 0)
      file(REMOVE "${archive}")
      message(FATAL_ERROR "Failed to download ${dependency}: ${download_message}")
    endif()
  endif()

  set(extract_directory "${EXTRACT_ROOT}/${dependency}-${version}")
  file(REMOVE_RECURSE "${extract_directory}")
  file(MAKE_DIRECTORY "${extract_directory}")
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xzf "${archive}"
    WORKING_DIRECTORY "${extract_directory}"
    RESULT_VARIABLE extract_result
  )
  if(NOT extract_result EQUAL 0)
    message(FATAL_ERROR "Failed to extract ${dependency} ${version}")
  endif()

  set(include_source "${extract_directory}/${archive_root}/include")
  if(NOT IS_DIRECTORY "${include_source}")
    message(FATAL_ERROR "The ${dependency} archive does not contain the expected include directory")
  endif()

  file(REMOVE_RECURSE "${destination}")
  file(MAKE_DIRECTORY "${destination}")
  file(COPY "${include_source}/" DESTINATION "${destination}/include")
  foreach(license_file IN LISTS ARGN)
    file(COPY "${extract_directory}/${archive_root}/${license_file}"
         DESTINATION "${destination}")
  endforeach()
  file(WRITE "${version_file}" "${version}\n")
  message(STATUS "Installed ${dependency} ${version} in ${destination}")
endfunction()

fetch_include_tree(
  asio
  1.38.2
  "https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-38-2.tar.gz"
  "asio-asio-1-38-2"
  "9f2648fa483e58a6bf848d970ee0ea650ca19ed7769dfa520ed4f7b8d27af1db"
  "LICENSE_1_0.txt"
  "COPYING"
)

fetch_include_tree(
  nlohmann_json
  3.12.0
  "https://github.com/nlohmann/json/archive/refs/tags/v3.12.0.tar.gz"
  "json-3.12.0"
  "4b92eb0c06d10683f7447ce9406cb97cd4b453be18d7279320f7b2f025c10187"
  "LICENSE.MIT"
)

fetch_include_tree(
  spdlog
  1.17.0
  "https://github.com/gabime/spdlog/archive/refs/tags/v1.17.0.tar.gz"
  "spdlog-1.17.0"
  "d8862955c6d74e5846b3f580b1605d2428b11d97a410d86e2fb13e857cd3a744"
  "LICENSE"
)
