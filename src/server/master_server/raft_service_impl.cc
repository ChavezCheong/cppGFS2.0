#include "raft_service_impl.h"

#include "src/protos/grpc/raft_service.grpc.pb.h"


namespace gfs{
namespace service{


grpc::Status RequestVote(grpc::ServerContext* context,
    const protos::grpc::RequestVoteRequest* request,
    protos::grpc::RequestVoteReply* reply){
    // TODO: implement logic here

    response->set_term(currentTerm);
    response->set_votegranted(false);

    // reply false if term < currentTerm
    if (request->term() < currentTerm){
        return grpc::Status::OK;
    }
    // increment term if RPC contains higher term and convert to follower
    else if(request->term() > currentTerm){
        // TODO: add some way to get current servers address for logging purposes
        LOG(INFO) << "Server converting to follower ";
        currentTerm = request->term();
        ConvertToFollower();
    }

    // if votedFor is null or candidateId, and candidates 
    // log is at least as up to date as receiver's log, grant vote
    if((votedFor == -1 || votedFor == request->candidateid())
       && (log.empty() || 
       ((log.back().term() < request->lastlogterm()) || 
       (log.back().term() == request->lastlogterm() && log.size() - 1 <= request->lastlogindex())))){
        votedFor = request->candidateid();
        response->set_votegranted(true);
        // TODO: set votedFor in persistent storage and currentTerm
        // TODO: add some way to get current server id for logging
        LOG(INFO) << "Server voted for " << request->candidateid();
    }

    return grpc::Status::OK;
}

grpc::Status AppendEntries(grpc::ServerContext* context,
    const protos::grpc::AppendEntriesRequest* request,
    protos::grpc::AppendEntriesReply* reply){

    
    // TODO: implement logic here
    return grpc::Status::OK;
}


void ConvertToFollower(){
    currState = State::Follower;
}

// TODO: implement logic here
void ConvertToCandidate(){

}

// TODO: implement logic here
void ConvertToLeader(){

}


}
}