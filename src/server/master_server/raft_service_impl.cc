namespace gfs{
namespace service{

#include "src/protos/grpc/raft_service.grpc.pb.h"

grpc::Status RequestVote(grpc::ServerContext* context,
    const protos::grpc::RequestVoteRequest* request,
    protos::grpc::RequestVoteReply* reply){

    
    // TODO: implement logic here

    return grpc::Status::OK;
}

grpc::Status AppendEntries(grpc::ServerContext* context,
    const protos::grpc::AppendEntriesRequest* request,
    protos::grpc::AppendEntriesReply* reply){

    
    // TODO: implement logic here
    return grpc::Status::OK;
}

}
}