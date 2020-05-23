#ifndef GFS_CLIENT_CLIENT_CACHE_MANAGER_H_
#define GFS_CLIENT_CLIENT_CACHE_MANAGER_H_

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/time.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/protos/chunk_server.pb.h"

namespace gfs {
namespace client {

// The CacheManager on the client side manages the following information that
// are essential to a client:
//
// 1) mapping from (filename, chunk_index) => (chunk_handle)
// 2) mapping from (chunk_handle) => (version)
// 3) mapping from (chunk_handle) => (primary_chunk_location, 
//                                    all chunk_locations)
// 
// The first mapping is unique and would stay unchanged during time (as the 
// master node assigns unique chunk_handle to each chunk).
//
// The second & third mapping are transient, and in particular, the third 
// mapping should be associated with a timestamp. Any read from this mapping 
// should check if the entry has timed-out.
//
// Example: To instantiate a cache manager use:
//   CacheManager* manager = CacheManager::ConstructCacheManager(timeout);
// 
// Note that for each client process there should be only one cache manager,
// this is managed by another layer.
//
// When client receives a chunk_handle generated by master, it inserts it
// to this cache unit by 
//   manager->SetChunkHandle(filename, chunk_index, chunk_handle);
//
// Client can retrieve / update chunk's version number by 
//   manager->SetChunkVersion(chunk_handle, version);
//   manager->GetChunkVersion(chunk_handle);
//
// Note that the chunk_handle have been inserted to the cache unit via the 
// SetChunkHandle call, otherwise you get an error. 
//
// The GetChunkServerLocation returns an error if the cache is found to be 
// aged. The client code should check this and updates the entry via:
//   CacheManager::ChunkServerLocationEntry entry;
//   entry.primary_location = ...  
//   entry.locations.push_back(...)
//   cacheManager->SetChunkServerLocation(chunk_handle, entry);
// which construct a new entry and update the cache manager
//
// TODO(Xi): This is a simple design in which cache entry does not get wiped 
// out. when a read request comes in for chunk server location, one simply 
// compares the timestamp and decide whether to return the entry or not. 
// To extend this design, one should start a separate thread that 
// periodically scans the cache entry and removes those that expires. The 
// current design leaves room for this extension, as all you have to add is
// an internal lock for the data structures and the implement a separate
// thread for the cleanup. 

class CacheManager {
 public:
  // Cache entry struct for chunk server locations
  struct ChunkServerLocationEntry {
    protos::ChunkServerLocation primary_location;
    std::vector<protos::ChunkServerLocation> locations;
    absl::Time timestamp;

    // Initialize the timestamp when this entry is constructed
    ChunkServerLocationEntry() : timestamp(absl::Now()) {}
    ChunkServerLocationEntry(
        const protos::ChunkServerLocation& _primary_location,
        const std::vector<protos::ChunkServerLocation>& _locations) :
            primary_location(_primary_location),
            locations(_locations),
            timestamp(absl::Now()) {}
  };

  // Returns the chunk handle for a filename at chunk_index. Returns
  // error if no chunk_handle has been assigned
  google::protobuf::util::StatusOr<std::string> 
      GetChunkHandle(const std::string& filename, uint32_t chunk_index) const;

  // Add a (filename, chunk_index) => chunk_handle mapping to the cache
  // Return error if there already exists a different mapping
  google::protobuf::util::Status
      SetChunkHandle(const std::string& filename, uint32_t chunk_index,
                     const std::string& chunk_handle);

  // Retrieve the version number for a given chunk handle. Return error 
  // if chunk not found
  google::protobuf::util::StatusOr<uint32_t>
      GetChunkVersion(const std::string& chunk_handle) const;

  // Setting version number for a chunk. Return error if chunk handle
  // is invalid (not known to client)
  google::protobuf::util::Status 
      SetChunkVersion(const std::string& chunk_handle, uint32_t version);

  // Retrieve the Chunk server loation entry for a given chunk handle.
  // Return error if this entry has timed-out
  google::protobuf::util::StatusOr<ChunkServerLocationEntry>
      GetChunkServerLocation(const std::string& chunk_handle) const;

  // Set the chunk server location for a given chunk handle. Return error
  // if chunk handle is invalid
  google::protobuf::util::Status 
      SetChunkServerLocation(const std::string& chunk_handle,
                             const ChunkServerLocationEntry& entry);

  // Return an initialized CacheManager with a configured timeout
  static CacheManager* ConstructCacheManager(const absl::Duration timeout);

 private:
  CacheManager() = default;
  CacheManager(const absl::Duration timeout) : timeout_(timeout) {}

  // Timeout duration for the location information to expire. 
  absl::Duration timeout_;

  // Map from (filename, chunk_index) to a chunk_handle
  absl::flat_hash_map<std::string, 
      absl::flat_hash_map<uint32_t, std::string>> file_chunk_handle_;

  // A set of valid chunk_handle (added when a mpping is created)
  // This is to detect invalid chunk_handle given by the SetChunkVersion
  // and SetChunkServerLocation function. 
  absl::flat_hash_set<std::string> valid_chunk_handle_;

  // Map from (chunk_handle) to its version number
  absl::flat_hash_map<std::string, uint32_t> chunk_handle_version_;

  // Map from (chunk_handle) to chunk server locations
  absl::flat_hash_map<std::string, ChunkServerLocationEntry> 
      chunk_server_location_;
};

} // namespace client
} // namespace gfs

#endif  // GFS_CLIENT_CLIENT_CACHE_MANAGER_H_