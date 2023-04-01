#include "src/server/master_server/raft_service_log_manager.h"

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "leveldb/write_batch.h"
#include "src/common/system_logger.h"

namespace gfs {
namespace server {

using google::protobuf::util::AlreadyExistsError;
using google::protobuf::util::InternalError;
using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;
using google::protobuf::util::OutOfRangeError;
using google::protobuf::util::UnimplementedError;
using google::protobuf::util::UnknownError;
    
RaftServiceLogManager::RaftServiceLogManager()
    : raft_service_log_database_(nullptr) {}

RaftServiceLogManager* RaftServiceLogManager::GetInstance() {
    static RaftServiceLogManager instance;

    return &instance;
}

void RaftServiceLogManager::Initialize(const std::string& raft_service_log_database_name) {

    leveldb::DB* database;

    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status =
      leveldb::DB::Open(options, raft_service_log_database_name, &database);

    LOG_ASSERT(status.ok()) << status.ToString();

    this->raft_service_log_database_ = std::unique_ptr<leveldb::DB>(database);
    
    write_options_.sync = true;
    raft_service_log_database_->Put(write_options_, "current_term", "0");
}

StatusOr<int>RaftServiceLogManager::GetCurrentTerm() {
    
    std::string existing_data;
    leveldb::Status status = this->raft_service_log_database_->Get(read_options_, "current_term")
}

} 

}