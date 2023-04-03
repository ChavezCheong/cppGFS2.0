#ifndef GFS_SERVER_MASTER_SERVER_RAFT_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_RAFT_SERVICE_IMPL_H_

#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/raft_service_client.h"
#include "src/server/master_server/raft_service_log_manager.h"
#include "absl/container/flat_hash_map.h"
#include "src/common/utils.h"

using protos::grpc::LogEntry;

namespace gfs{
namespace service{

// Implementation for handling RaftService requests

class RaftServiceImpl final 
    : public protos::grpc::RaftService::Service {
public:
    RaftServiceImpl(common::ConfigManager* config_manager) : config_manager_(config_manager) {};
    enum State {Follower, Candidate, Leader};
    void AlarmCallback();
    void Initialize(std::string master_name);

private:
    // Handle AppendEntries request sent by Raft server
    grpc::Status AppendEntries(grpc::ServerContext* context,
                               const protos::grpc::AppendEntriesRequest* request,
                               protos::grpc::AppendEntriesReply* reply) override;

    // Handle RequestVote request sent by Raft server
    grpc::Status RequestVote(grpc::ServerContext* context,
                               const protos::grpc::RequestVoteRequest* request,
                               protos::grpc::RequestVoteReply* reply) override;

    void SendRequestVote(); // TODO: change this later
    void SendAppendEntries(); //TODO: change this later

    void ConvertToFollower();
    void ConvertToCandidate();
    void ConvertToLeader();

    State GetCurrentState();

    void SetAlarm(int after_ms);
    

    void reset_election_timeout();

    protos::grpc::AppendEntriesRequest createAppendEntriesRequest(std::string server_name);

    common::ConfigManager* config_manager_;

    std::vector<std::string> all_servers;
    gfs::common::thread_safe_flat_hash_map<std::string, std::shared_ptr<gfs::service::RaftServiceClient>> masterServerClients;

    // persistent state
    int currentTerm, votedFor;
    std::vector<LogEntry> log_;

    const int numServers = 3;

    // volatile state on all servers
    int commitIndex, lastApplied, currLeader, numVotes;
    State currState;

    int serverId;

    // volatile state on leaders
    gfs::common::thread_safe_flat_hash_map<std::string, int> nextIndex, matchIndex;

    // persistent storage for raft service log
    RaftServiceLogManager* raft_service_log_manager_;

};

}
}


#endif 