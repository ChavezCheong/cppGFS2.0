#include "src/server/master_server/raft_service_log_manager.h"

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "leveldb/write_batch.h"
#include "src/common/system_logger.h"

namespace gfs
{
    namespace service
    {

        using google::protobuf::util::AlreadyExistsError;
        using google::protobuf::util::InternalError;
        using google::protobuf::util::NotFoundError;
        using google::protobuf::util::OkStatus;
        using google::protobuf::util::OutOfRangeError;
        using google::protobuf::util::UnimplementedError;
        using google::protobuf::util::UnknownError;

        RaftServiceLogManager::RaftServiceLogManager()
            : raft_service_log_database_(nullptr) {}

        RaftServiceLogManager *RaftServiceLogManager::GetInstance()
        {
            static RaftServiceLogManager instance;

            return &instance;
        }

        void RaftServiceLogManager::Initialize(const std::string &raft_service_log_database_name)
        {

            leveldb::DB *database;

            leveldb::Options options;
            options.create_if_missing = true;

            leveldb::Status status =
                leveldb::DB::Open(options, raft_service_log_database_name, &database);

            LOG_ASSERT(status.ok()) << status.ToString();

            this->raft_service_log_database_ = std::unique_ptr<leveldb::DB>(database);

            write_options_ = leveldb::WriteOptions();
            read_options_ = leveldb::ReadOptions();
            write_options_.sync = true;

            // raft_service_log_database_->Put(write_options_, "current_term", "0");
        }

        StatusOr<int> RaftServiceLogManager::GetCurrentTerm()
        {
            std::string placeholder;
            leveldb::Status status =
                raft_service_log_database_->Get(read_options_, "current_term", &placeholder);

            if (!status.ok())
            {
                // not found
                return NotFoundError(
                    absl::StrCat("Current term not found, Status: ",
                                 status.ToString()));
            }

            return std::stoi(placeholder);
        }

        Status RaftServiceLogManager::UpdateCurrentTerm(int new_term)
        {
            leveldb::Status status =
                raft_service_log_database_->Put(write_options_, "current_term",
                                                std::to_string(new_term));
            if (!status.ok())
            {
                return UnknownError(
                    absl::StrCat("Failed while updating current term. Status: ",
                                 status.ToString()));
            }

            return OkStatus();
        }

        StatusOr<int> RaftServiceLogManager::GetVotedFor()
        {
            std::string placeholder;
            leveldb::Status status =
                raft_service_log_database_->Get(read_options_, "current_voted_for", &placeholder);

            if (!status.ok())
            {
                // not found
                return NotFoundError(
                    absl::StrCat("CurrentVotedFor not found, Status: ",
                                 status.ToString()));
            }

            return std::stoi(placeholder);
        }

        Status RaftServiceLogManager::UpdateVotedFor(int voted_for)
        {
            leveldb::Status status =
                raft_service_log_database_->Put(write_options_, "current_voted_for",
                                                std::to_string(voted_for));
            if (!status.ok())
            {
                return UnknownError(
                    absl::StrCat("Failed while updating current voted for. Status: ",
                                 status.ToString()));
            }

            return OkStatus();
        }

        StatusOr<std::vector<LogEntry>> RaftServiceLogManager::GetLogEntries()
        {
            std::string value;
            leveldb::Status status =
                raft_service_log_database_->Get(read_options_, "log_entries", &value);

            if (!status.ok())
            {
                return NotFoundError(
                    absl::StrCat("Log not found, Status: ", status.ToString()));
            }

            // Deserialize the vector of LogEntry protos
            std::vector<LogEntry> log_entries;
            std::istringstream iss(value);
            std::string tmp;
            while (std::getline(iss, tmp, '|'))
            {
                LogEntry log_entry;
                log_entry.ParseFromString(tmp);
                log_entries.push_back(log_entry);
            }

            return log_entries;
        }

        Status RaftServiceLogManager::AppendLogEntries(const std::vector<LogEntry> &log_entries)
        {
            // Get the current log entries
            StatusOr<std::vector<LogEntry>> result = GetLogEntries();
            std::vector<LogEntry> updated_log_entries;
            if (result.ok())
            {
                updated_log_entries = result.value();
            }
            // Append the new log entries to the current ones
            updated_log_entries.insert(updated_log_entries.end(), log_entries.begin(), log_entries.end());

            // Serialize the vector of log entries
            std::string serialized_log_entries;
            for (const auto &log_entry : updated_log_entries)
            {
                std::string serialized_log_entry = log_entry.SerializeAsString();
                serialized_log_entries += serialized_log_entry + '|';
            }

            // Store the serialized log entries in the database
            leveldb::Status status = raft_service_log_database_->Put(write_options_, "log_entries", serialized_log_entries);
            if (!status.ok())
            {
                return UnknownError(
                    absl::StrCat("Failed while storing log entries. Status: ", status.ToString()));
            }

            return OkStatus();
        }

        Status RaftServiceLogManager::DeleteLogEntries(int new_size)
        {
            // Get the current log entries
            StatusOr<std::vector<LogEntry>> result = GetLogEntries();

            // if there was nothing to delete, just return success since 
            // it's idempotent
            if (!result.ok())
            {
                return OkStatus();
            }

            std::vector<LogEntry> updated_log_entries = result.value();

            // if the new size is somehow bigger than the old size, then
            // return success without doing anything
            if (updated_log_entries.size() <= new_size) {
                return OkStatus();
            }

            // Resize to new size and update levelDB
            updated_log_entries.resize(new_size);
            std::string serialized_log_entries;
            for (const auto &log_entry : updated_log_entries)
            {
                std::string serialized_log_entry = log_entry.SerializeAsString();
                serialized_log_entries += serialized_log_entry + '|';
            }

            // Store the serialized log entries in the database
            leveldb::Status status = raft_service_log_database_->Put(write_options_, "log_entries", serialized_log_entries);
            if (!status.ok())
            {
                return UnknownError(
                    absl::StrCat("Failed while storing log entries. Status: ", status.ToString()));
            }

            return OkStatus();
        }

    } // namespace service
} // namespace gfs