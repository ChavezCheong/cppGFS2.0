#include "src/client/client_cache_manager.h"

using google::protobuf::util::DeadlineExceededError;
using google::protobuf::util::InvalidArgumentError;
using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace client {

google::protobuf::util::StatusOr<std::string> CacheManager::GetChunkHandle(
    const std::string& filename, uint32_t chunk_index) const {
  if (!file_chunk_handle_.contains(filename)) {
    return NotFoundError("Filename does not exist: " + filename);
  }

  if (!file_chunk_handle_.at(filename).contains(chunk_index)) {
    return NotFoundError("Chunk index " + std::to_string(chunk_index) +
                         " does not exist in file: " + filename);
  }

  return file_chunk_handle_.at(filename).at(chunk_index);
}

google::protobuf::util::Status CacheManager::SetChunkHandle(
    const std::string& filename, uint32_t chunk_index,
    const std::string& chunk_handle) {
  auto original_chunk_or(GetChunkHandle(filename, chunk_index));
  if (original_chunk_or.ok()) {
    // If there already exists a mapping for this but we change the
    // chunk_handle, something is wrong because master assigns unique
    // chunk_hanle. (We can extend / relax this rule if we decide to
    // support the case of delete-then-create a file).
    if (original_chunk_or.value() != chunk_handle) {
      return InvalidArgumentError("Reassigning a chunk handle for " + filename +
                                  " at chunk_index " +
                                  std::to_string(chunk_index) + "from " +
                                  original_chunk_or.value() + " to" +
                                  chunk_handle + " not allowed.");
    }
    return OkStatus();
  }

  file_chunk_handle_[filename][chunk_index] = chunk_handle;
  valid_chunk_handle_.insert(chunk_handle);
  return OkStatus();
}

google::protobuf::util::StatusOr<uint32_t> CacheManager::GetChunkVersion(
    const std::string& chunk_handle) const {
  if (!chunk_handle_version_.contains(chunk_handle)) {
    return NotFoundError("Chunk handle not found: " + chunk_handle);
  }
  return chunk_handle_version_.at(chunk_handle);
}

google::protobuf::util::Status CacheManager::SetChunkVersion(
    const std::string& chunk_handle, uint32_t version) {
  if (!valid_chunk_handle_.contains(chunk_handle)) {
    return InvalidArgumentError("Invalid chunk handle " + chunk_handle);
  }
  chunk_handle_version_[chunk_handle] = version;
  return OkStatus();
}

google::protobuf::util::StatusOr<CacheManager::ChunkServerLocationEntry>
CacheManager::GetChunkServerLocation(const std::string& chunk_handle) const {
  if (!chunk_server_location_.contains(chunk_handle)) {
    return NotFoundError("Chunk handle not found: " + chunk_handle);
  }

  auto entry(chunk_server_location_.at(chunk_handle));
  const absl::Time entryTimestamp(entry.timestamp);
  const absl::Time now(absl::Now());

  // If this entry timesout, we return error. The application is supposed
  // to refresh the chunk server location for this chunk by contacting
  // the master again.
  if (now - entryTimestamp > timeout_) {
    return DeadlineExceededError("Chunk server location info for " +
                                 chunk_handle + "timed out");
  }

  return chunk_server_location_.at(chunk_handle);
}

google::protobuf::util::Status CacheManager::SetChunkServerLocation(
    const std::string& chunk_handle,
    const CacheManager::ChunkServerLocationEntry& entry) {
  if (!valid_chunk_handle_.contains(chunk_handle)) {
    return InvalidArgumentError("Invalid chunk handle " + chunk_handle);
  }
  chunk_server_location_[chunk_handle] = entry;
  return OkStatus();
}

CacheManager* CacheManager::ConstructCacheManager(
    const absl::Duration timeout) {
  return new CacheManager(timeout);
}

google::protobuf::util::StatusOr<std::string> CacheManager::GetPrimaryMaster() {
  if (primary_master_.empty()) {
    return std::string("master_server_01");
  }

  return primary_master_;
}

google::protobuf::util::Status CacheManager::SetPrimaryMaster(
    std::string primary_master_name) {
  primary_master_ = primary_master_name;
  return OkStatus();
}
}  // namespace client
}  // namespace gfs
