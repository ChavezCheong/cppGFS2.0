#include "raft_service_impl.h"

#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/common/system_logger.h"

namespace gfs{
namespace service{


grpc::Status RequestVote(grpc::ServerContext* context,
    const protos::grpc::RequestVoteRequest* request,
    protos::grpc::RequestVoteReply* reply){
    // TODO: implement logic here

    reply->set_term(currentTerm);
    reply->set_votegranted(false);

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

    int prev_log_index = request->prevlogindex();
    int prev_log_term = request->prevlogterm();


    // reject the request if leader term is less than current term
    if(request->term() < currentTerm){
        // currentTerm = request->term();
        reply->set_term(currentTerm);
        reply->set_success(false);
        return grpc::Status::OK; // might need to change this
    }

    // TODO: handle election timeout


    // If the log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reject the request

    if(prev_log_index >= log.size() || log[prev_log_index].term() != prev_log_term){
        reply->set_term(currentTerm);
        reply->set_success(false);
        return grpc::Status::OK; // might need to change this
    }

    // iterate over entry and append to the log

    for(const auto& entry : request->entries()){
        int index = entry.index();
        int term = entry.term();

        // TODO: implement append entries logic here
        // might need to fix

        if(index < log.size() && log[index].term() != term){
            // if the entry conflicts with the log, i.e has the same index but different term, delete entries 
            log.resize(index);
        }

        if(index >= log.size()){
            // add log entry to the log, not sure how to do this efficiently
        }

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

        if(request->leadercommit() > commitIndex){
            // log.size() - 1 is the index of last entry, might change depend on implementation
            commitIndex = std::min(request->leadercommit(), log.size() - 1);

        }


        return grpc::Status::OK;


        // TODO: Leader might send AppendEntries RPC after voting to convert Candidates to Follower
        // TODO: Follower: If election timeout elapses without receiving AppendEntries
        // RPC from current leader or granting vote to candidate: convert to candidate
        // TODO: Follower: Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts 
        // TODO: If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
        /*• If successful: update nextIndex and matchIndex for
        follower (§5.3)
        • If AppendEntries fails because of log inconsistency:
        decrement nextIndex and retry
        */
        
    }

    // If the leader's commit index is greater than ours, update our commit index


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