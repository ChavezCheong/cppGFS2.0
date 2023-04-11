#include "raft_service_impl.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/common/system_logger.h"
#include <csignal>
#include <random>
#include <future>
#include <utility>
#include <vector>
#include <deque>

using protos::grpc::RequestVoteRequest;
using protos::grpc::AppendEntriesRequest;
using protos::grpc::RequestVoteReply;
using protos::grpc::AppendEntriesReply;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::LogEntry;



namespace gfs{
namespace service{
RaftServiceImpl* alarmHandlerServer;

void HandleSignal(int signum) {
    alarmHandlerServer->AlarmCallback();
}

void RaftServiceImpl::Initialize(std::string master_name, bool resolve_hostname){
    signal(SIGALRM, &HandleSignal);
    alarmHandlerServer = this;
    this->resolve_hostname_ =  resolve_hostname;

    // get all servers that are not ourselves
    std::vector<std::string> all_master_servers = config_manager_->GetAllMasterServers();
    for(auto server: all_master_servers){
        if(server != master_name){
            all_servers.push_back(server);
        }
    }

    for(auto server_name : all_servers){
        auto server_address = config_manager_->GetServerAddress(server_name, resolve_hostname_);
        LOG(INFO) << "Establishing new connection to server:"
              << server_address;
        masterServerClients[server_name] =         
        std::make_shared<RaftServiceClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));
    }
    // Set up raft service log manager for use
    raft_service_log_manager_ = RaftServiceLogManager::GetInstance();
    LOG(INFO) << "Starting Raft Service";
    currState = State::Follower;
    votedFor = -1;
    currentTerm = 0;
    SetAlarm(150); // TODO: change this setup 
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
        SendAppendEntries();
        reset_election_timeout();
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

    LOG(INFO) << "Handle Request Vote RPC from " << request->candidateid();

    reply->set_term(currentTerm);
    reply->set_votegranted(0);

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

    if (currState == State::Leader) return grpc::Status::OK;

    // if votedFor is null or candidateId, and candidates 
    // log is at least as up to date as receiver's log, grant vote
    if((votedFor == -1 || votedFor == request->candidateid())
       && (log_.empty() || 
       ((log_.back().term() < request->lastlogterm()) || 
       (log_.back().term() == request->lastlogterm() && log_.size() - 1 <= request->lastlogindex())))){
        votedFor = request->candidateid();
        reply->set_votegranted(1);
        // TODO: set votedFor in persistent storage and currentTerm
        // TODO: add some way to get current server id for logging
        LOG(INFO) << "Server voted for " << request->candidateid();
        reset_election_timeout();
    }
    else{
        return grpc::Status::OK;
    }

    // TODO: add timer for election to timeout when necessary

    // reset election when 
    

    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* context,
    const protos::grpc::AppendEntriesRequest* request,
    protos::grpc::AppendEntriesReply* reply){

    LOG(INFO) << "Handle Append Entries RPC from: " << request->leaderid();
    
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
    reset_election_timeout();


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
    votedFor = -1;
}

// TODO: implement logic here
void RaftServiceImpl::ConvertToCandidate(){
    currState = State::Candidate;
    // Once a server is converted to candidate, we increase the current term
    currentTerm++;
    numVotes = 0;
    votedFor = serverId;

    LOG(INFO) << "Server convert to candidate";

    reset_election_timeout();

    // Send the requests in parallel
    std::vector<
        std::future<std::pair<std::string, StatusOr<RequestVoteReply>>>>
        request_vote_results;

    for(auto server_name : all_servers){        
        LOG(INFO) << "Sending request vote RPC to server " << server_name;
        request_vote_results.push_back(
            std::async(std::launch::async, [&, server_name](){
                RequestVoteRequest request;

                request.set_term(currentTerm);
                request.set_candidateid(votedFor);
                if(log_.size() == 0){
                    request.set_lastlogterm(0);
                    request.set_lastlogindex(0);
                }
                else{
                    request.set_lastlogterm(log_.back().term());
                    request.set_lastlogindex(log_.back().index());
                }
                
                auto client = masterServerClients[server_name];

                auto request_vote_reply = client->SendRequest(request);

                return std::pair<std::string, StatusOr<RequestVoteReply>>(
                    server_name, request_vote_reply);
        }));
    }

    // count the votes
    for(int i = 0; i < all_servers.size(); ++i){
        // TODO: abstract this into configurable variable
        // wait for future with a timeout time 
        std::future_status status = request_vote_results[i].wait_for(std::chrono::milliseconds(50));

        // check if future has resolved
        if (status == std::future_status::ready){
            LOG(INFO) << "Future instance successfully executed";
            auto request_vote_result = request_vote_results[i].get();
            auto server_name = request_vote_result.first;
            auto request_vote_reply = request_vote_result.second;

            // logic to handle votes
            if (request_vote_reply.ok()){
                auto reply = request_vote_reply.value();
                // if outdated term, convert to follower

                LOG(INFO) << "Grant vote? " << reply.votegranted();
                LOG(INFO) << "Term of reply " << reply.term();
                LOG(INFO) << "Current term " << currentTerm;
                if(reply.term() > currentTerm){
                    LOG(INFO) << "Server converting to follower ";
                    currentTerm = reply.term();
                    ConvertToFollower();
                    return;
                }
                else if (reply.votegranted() == 1 and reply.term() == currentTerm){
                    numVotes++;
                    LOG(INFO) << "Term incremented correctly. " << numVotes;
                }
            }
        }
        else {
            LOG(INFO) << "THE FUNCTION TIMED OUT";
        }
    }


    LOG(INFO) << "Numbers of votes are " << numVotes;

    // TODO: CHANGE THIS TO CALCULATED QUORUM
    if(numVotes >= 2){
        ConvertToLeader();
    }
}

RaftServiceImpl::State RaftServiceImpl::GetCurrentState(){
    return currState;
}


void RaftServiceImpl::reset_election_timeout(){
    // TODO: add a Timer here

    int ELECTION_TIMEOUT_LOW = 1000;
    int ELECTION_TIMEOUT_HIGH = 2000;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);

    float election_timeout_ = dis(gen);

    SetAlarm(election_timeout_);
}

// TODO: implement logic here
void RaftServiceImpl::ConvertToLeader(){
    currState = State::Leader;

    // initialize nextIndex and matchIndex
    for(std::string server_name : all_servers){
        nextIndex[server_name] = log_.size();
        matchIndex[server_name] = 0;
    }


    // Upon election, send empty AppendEntries RPC to all other servers
    SendAppendEntries();
    // TODO: make a function that resets heartbeat timeout
    reset_election_timeout();
    LOG(INFO) << "Server converts to leader";
}


void RaftServiceImpl::SendAppendEntries(){
    std::deque<
        std::future<std::pair<std::string, StatusOr<AppendEntriesReply>>>>
        append_entries_results;

    for(auto server_name : all_servers){
        LOG(INFO) << "Sending an empty AppendEntries RPC upon election to server" << server_name;
        append_entries_results.push_back(
            std::async(std::launch::async, [&, server_name](){
                // create reply and send
                    AppendEntriesRequest request = createAppendEntriesRequest(server_name);
                auto client = masterServerClients[server_name];
                auto append_entries_reply = client->SendRequest(request);

                return std::pair<std::string, StatusOr<AppendEntriesReply>>(
                    server_name, append_entries_reply);
        }));
    }

    return; 

    while(!append_entries_results.empty()){
        auto response_future = std::move(append_entries_results.front());
        append_entries_results.pop_front();

        // TODO: abstract this into configurable variable
        // wait for future with a timeout time 
        std::future_status status = response_future.wait_for(std::chrono::seconds(1));

        // check if the future has resolved
        if (status == std::future_status::ready) {
            std::pair<std::string, StatusOr<AppendEntriesReply>> append_entries_result = response_future.get();
            std::string server_name = append_entries_result.first;
            StatusOr<AppendEntriesReply> append_entries_reply = append_entries_result.second;

            if (append_entries_reply.ok()){
                AppendEntriesReply reply = append_entries_reply.value();
                // TODO: add logic to resend appendentries if needed
            }
            else{

            }
        }

        
    }

}

protos::grpc::AppendEntriesRequest RaftServiceImpl::createAppendEntriesRequest(std::string server_name){
    AppendEntriesRequest request;
    request.set_term(currentTerm);
    request.set_leaderid(currLeader);

    int prev_log_index = nextIndex[server_name] - 1;

    //index of log entry immediately preceding new ones
    request.set_prevlogindex(prev_log_index);

    // term of prevLogIndex entry
    request.set_prevlogterm(log_[prev_log_index].term());

    request.set_leadercommit(commitIndex);

    // log entries to store
    for(int j = prev_log_index + 1; j < log_.size(); j++){
        LogEntry* entry = request.add_entries();
    }

    return request;
}


}
}