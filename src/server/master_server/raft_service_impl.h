#ifndef GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_
#define GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_

#include "src/protos/grpc/raft_service.grpc.pb.h"

namespace gfs{
namespace server{

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


};

}
}


#endif 