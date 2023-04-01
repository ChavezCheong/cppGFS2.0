#ifndef GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_
#define GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_

#include <list>
#include <memory>
#include <string>

#include "google/protobuf/stubs/statusor.h"
#include "leveldb/db.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"

namespace gfs {

namespace server {


using google::protobuf::util::StatusOr;
using google::protobuf::util::Status;
using protos::grpc::LogEntry;

class RaftServiceLogManager {
    public:

    static RaftServiceLogManager* GetInstance();

    RaftServiceLogManager(const RaftServiceLogManager&) = delete;

    void operator=(const RaftServiceLogManager&) = delete;

    void Initialize(const std::string& raft_service_log_database_name);

    StatusOr<int> GetCurrentTerm();
    
    StatusOr<int> GetVotedFor();

    StatusOr<std::vector<LogEntry>> GetLogEntries();
    
    Status UpdateCurrentTerm(int new_term);
    
    Status UpdateVotedFor(int voted_for);

    Status AppendLogEntries(const std::vector<LogEntry> &log_entries);

    Status DeleteLogEntries(int new_size);

    std::unique_ptr<leveldb::DB> raft_service_log_database_;

    RaftServiceLogManager();
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_