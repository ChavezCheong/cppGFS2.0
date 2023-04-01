#include "raft_service_impl.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/common/system_logger.h"
#include <csignal>
#include <random>

using protos::grpc::RequestVoteRequest;
using protos::grpc::AppendEntriesRequest;

namespace gfs{
namespace service{
RaftServiceImpl* alarmHandlerServer;

void HandleSignal(int signum) {
    alarmHandlerServer->AlarmCallback();
}

void RaftServiceImpl::Initialize(){
    signal(SIGALRM, &HandleSignal);
    alarmHandlerServer = this;
    std::vector<std::string> all_servers = config_manager_->GetAllMasterServers();
    for(auto server_name : all_servers){
        auto server_address = config_manager_->GetServerAddress(server_name,
                                          /*resolve_hostname=*/true);
        masterServerClients[server_name] =         
        std::make_shared<RaftServiceClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));
    }
}


void RaftServiceImpl::AlarmCallback() {
    // TODO: Consider the state of the master and call appropriate function:

    // - Candidate: election timeout -> resend RV and reset election timeout
    // - Follower: If election timeout elapses without receiving AppendEntries
        // RPC from current leader or granting vote to candidate: convert to candidate

    if(currState == State::Candidate or currState == State::Follower){
        ConvertToCandidate();
    }
    if(currState == State::Leader){
        
    }
}

void RaftServiceImpl::SetAlarm(int after_ms) {
    struct itimerval timer;
    timer.it_value.tv_sec = after_ms / 1000;
    timer.it_value.tv_usec = 1000 * (after_ms % 1000); // microseconds
    timer.it_interval = timer.it_value;
    setitimer(ITIMER_REAL, &timer, nullptr);
    return;
}




grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* context,
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
       && (log_.empty() || 
       ((log_.back().term() < request->lastlogterm()) || 
       (log_.back().term() == request->lastlogterm() && log_.size() - 1 <= request->lastlogindex())))){
        votedFor = request->candidateid();
        reply->set_votegranted(true);
        // TODO: set votedFor in persistent storage and currentTerm
        // TODO: add some way to get current server id for logging
        LOG(INFO) << "Server voted for " << request->candidateid();
    }

    // TODO: add timer for election to timeout when necessary

    // reset election when 
    reset_election_timeout();

    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* context,
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
    // increment term if RPC contains higher term and convert to follower
    else if(request->term() > currentTerm){
        // TODO: add some way to get current servers address for logging purposes
        LOG(INFO) << "Server converting to follower ";
        currentTerm = request->term();
        ConvertToFollower();
    }


    // TODO: handle election timeout


    // If the log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reject the request

    if(prev_log_index >= log_.size() || log_[prev_log_index].term() != prev_log_term){
        reply->set_term(currentTerm);
        reply->set_success(false);
        return grpc::Status::OK; // might need to change this
    }

    // iterate over entry and append to the log
    int index = prev_log_index + 1;

    for(const auto& entry : request->entries()){
        int term = entry.term();

        // TODO: implement append entries logic here
        // might need to fix

        if(index < log_.size() && log_[index].term() != term){
            // if the entry conflicts with the log, i.e has the same index but different term, delete entries 
            log_.resize(index);
        }

        if(index >= log_.size()){
            log_.push_back(entry);
        }

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if(request->leadercommit() > commitIndex){
            // log.size() - 1 is the index of last entry, might change depend on implementation
            commitIndex = std::min(request->leadercommit(), (uint32_t) log_.size() - 1);

        }
    }

    reply->set_success(true);
    ConvertToFollower();
    currLeader = request->leaderid();


    // TODO: Follower: If election timeout elapses without receiving AppendEntries
    // RPC from current leader or granting vote to candidate: convert to candidate
    // TODO: Follower: Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts 
    // TODO: If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    /*• If successful: update nextIndex and matchIndex for
    follower (§5.3)
    • If AppendEntries fails because of log inconsistency:
    decrement nextIndex and retry
    */


    

    // If the leader's commit index is greater than ours, update our commit index


    return grpc::Status::OK;
}


void RaftServiceImpl::ConvertToFollower(){
    currState = State::Follower;
}

// TODO: implement logic here
void RaftServiceImpl::ConvertToCandidate(){
    // Once a server is converted to candidate, we increase the current term
    currentTerm++;
    numVotes = 0;
    votedFor = serverId;

    reset_election_timeout();

    std::vector<std::string> all_servers = config_manager_->GetAllMasterServers();

    for(auto server_name : all_servers){

        // TODO: use config manager to get the master address, then send a request vote RPC to it

        RequestVoteRequest request;

        request.set_term(currentTerm);
        request.set_candidateid(votedFor);
        request.set_lastlogterm(log_.back().term());
        request.set_lastlogindex(log_.back().index());

        // send request vote to the server

        // SendRequest(request, server);

        // TODO: create a client to communicate with the masters

    }

    // count the votes:

    if(numVotes >= 2){
        ConvertToLeader();
    }
}

RaftServiceImpl::State RaftServiceImpl::GetCurrentState(){
    return currState;
}


void RaftServiceImpl::reset_election_timeout(){
    // TODO: add a Timer here

    int ELECTION_TIMEOUT_LOW = 150;
    int ELECTION_TIMEOUT_HIGH = 500;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);

    float election_timeout_ = dis(gen);

    SetAlarm(election_timeout_);
}

// TODO: implement logic here
void RaftServiceImpl::ConvertToLeader(){
    // Upon election, send empty AppendEntries RPC to all other servers

    std::vector<std::string> all_servers = config_manager_->GetAllMasterServers();
    AppendEntriesRequest request;

    request.set_term(currentTerm);
    request.set_leaderid(currLeader);
    request.set_prevlogindex(log_.back().index());
    request.set_prevlogterm(log_.back().term());
    request.set_leadercommit(log_.back().index());

    for(int server_id = 0; server_id < numServers; server_id++){
        if(server_id == serverId){
            continue;
        }
        // send request to server_id
    }
}

}
}