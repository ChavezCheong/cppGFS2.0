#ifndef GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_
#define GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_

#include "src/protos/grpc/raft_service.grpc.pb.h"
using protos::grpc::LogEntry;

namespace gfs{
namespace service{

// Implementation for handling RaftService requests

class RaftServiceImpl final 
    : public protos::grpc::RaftService::Service {
public:
    RaftServiceImpl();


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

    // persistent state
    int currentTerm, votedFor;
    std::vector<LogEntry> log_;

    // volatile state on all servers
    int commitIndex, lastApplied;
    enum State {Follower, Candidate, Leader};
    State currState;


    // volatile state on leaders
    std::vector<int> nextIndex, matchIndex;

};

}
}


#endif 