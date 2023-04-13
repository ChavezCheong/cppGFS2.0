#include "raft_service_impl.h"
#include "absl/synchronization/mutex.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/common/system_logger.h"
#include <csignal>
#include <random>
#include <future>
#include <utility>
#include <vector>
#include <deque>
#include <thread>

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ServerCompletionQueue;
using protos::grpc::AppendEntriesReply;
using protos::grpc::AppendEntriesRequest;
using protos::grpc::LogEntry;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;
using protos::grpc::RequestVoteReply;
using protos::grpc::RequestVoteRequest;

namespace gfs
{
    namespace service
    {
        RaftServiceImpl *alarmHandlerServer;

        void HandleSignal(int signum)
        {
            // Create a new thread and run the AlarmCallback method in it.
            std::thread t([]()
                          { alarmHandlerServer->AlarmCallback(); });

            // Detach the new thread so it can run independently.
            t.detach();
        }

        void RaftServiceImpl::Initialize(std::string master_name, bool resolve_hostname)
        {
            signal(SIGALRM, &HandleSignal);
            alarmHandlerServer = this;
            this->resolve_hostname_ = resolve_hostname;

            // set up the lock
            absl::MutexLock l(&lock_);

            // get all servers that are not ourselves
            std::vector<std::string> all_master_servers = config_manager_->GetAllMasterServers();
            for (auto server : all_master_servers)
            {
                if (server != master_name)
                {
                    all_servers.push_back(server);
                }
            }

            for (auto server_name : all_servers)
            {
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
            auto vote = raft_service_log_manager_->GetVotedFor();
            if (vote.ok())
            {
                votedFor = vote.value();
            }
            else
            {
                votedFor = 0;
            }

            auto curr_term = raft_service_log_manager_->GetCurrentTerm();

            if (curr_term.ok())
            {
                currentTerm = curr_term.value();
            }
            else
            {
                currentTerm = 0;
            }

            auto get_log = raft_service_log_manager_->GetLogEntries();

            if (get_log.ok())
            {
                log_ = get_log.value();
            }
            else
            {
                log_ = std::vector<LogEntry>(0);
            }

            reset_election_timeout();
        }

        void RaftServiceImpl::AlarmCallback()
        {
            // TODO: Consider the state of the master and call appropriate function:

            // - Candidate: election timeout -> resend RV and reset election timeout
            // - Follower: If election timeout elapses without receiving AppendEntries
            // RPC from current leader or granting vote to candidate: convert to candidate

            lock_.Lock();
            if (currState == State::Candidate or currState == State::Follower)
            {
                ConvertToCandidate();
            }
            else if (currState == State::Leader)
            {
                SendAppendEntries();
                reset_heartbeat_timeout();
            }
            lock_.Unlock();
        }

        void RaftServiceImpl::SetAlarm(int after_ms)
        {
            struct itimerval timer;
            timer.it_value.tv_sec = after_ms / 1000;
            timer.it_value.tv_usec = 1000 * (after_ms % 1000); // microseconds
            timer.it_interval = timer.it_value;
            setitimer(ITIMER_REAL, &timer, nullptr);
            return;
        }

        grpc::Status RaftServiceImpl::OpenFile(grpc::ServerContext *context,
                                               const protos::grpc::OpenFileRequest *request,
                                               protos::grpc::OpenFileReply *reply)
        {
            // TODO: logic
            if (currState == State::Leader)
            {
                lock_.Lock();
                LOG(INFO) << "God save our 512 project";
                LogEntry new_log;
                OpenFileRequest* new_request = new protos::grpc::OpenFileRequest(*request);
                new_log.set_allocated_open_file(new_request);
                new_log.set_index(log_.size()+1);
                new_log.set_term(currentTerm);
                log_.push_back(new_log);
                for (auto entry : log_) {
                    LOG(INFO) << entry.term();
                    LOG(INFO) << entry.open_file().filename();
                }
                SendAppendEntries();
                lock_.Unlock();
                return grpc::Status::OK;
            }
            else
            {
                LOG(INFO) << "NOT LEADER!";
                return grpc::Status::CANCELLED;
            }
        }

        grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext *context,
                                                  const protos::grpc::RequestVoteRequest *request,
                                                  protos::grpc::RequestVoteReply *reply)
        {
            // TODO: implement logic here

            lock_.Lock();
            LOG(INFO) << "Handle Request Vote RPC from " << request->candidateid();

            reply->set_term(currentTerm);
            reply->set_votegranted(0);

            // reply false if term < currentTerm
            if (request->term() < currentTerm)
            {
                lock_.Unlock();
                return grpc::Status::OK;
            }
            // increment term if RPC contains higher term and convert to follower
            else if (request->term() > currentTerm)
            {
                // TODO: add some way to get current servers address for logging purposes
                LOG(INFO) << "Server converting to follower ";
                currentTerm = request->term();
                ConvertToFollower();
                // raft_service_log_manager_->UpdateCurrentTerm(request->term());
            }

            if (currState == State::Leader)
            {
                lock_.Unlock();
                return grpc::Status::OK;
            }

            // if votedFor is null or candidateId, and candidates
            // log is at least as up to date as receiver's log, grant vote
            if ((votedFor == -1 || votedFor == request->candidateid()) && (log_.empty() ||
                                                                           ((log_.back().term() < request->lastlogterm()) ||
                                                                            (log_.back().term() == request->lastlogterm() && log_.size() - 1 <= request->lastlogindex()))))
            {
                votedFor = request->candidateid();
                reply->set_votegranted(1);
                // TODO: set votedFor in persistent storage and currentTerm
                // TODO: add some way to get current server id for logging
                LOG(INFO) << "Server voted for " << request->candidateid();
                // raft_service_log_manager_->UpdateVotedFor(request->candidateid());
            }
            else
            {
                lock_.Unlock();
                return grpc::Status::OK;
            }

            // TODO: add timer for election to timeout when necessary

            // reset election when
            reset_election_timeout();
            lock_.Unlock();

            return grpc::Status::OK;
        }

        grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext *context,
                                                    const protos::grpc::AppendEntriesRequest *request,
                                                    protos::grpc::AppendEntriesReply *reply)
        {

            lock_.Lock();

            LOG(INFO) << "Handle Append Entries RPC from: " << request->leaderid();

            for (auto log : log_) {
                LOG(INFO) << log.term() << " and  " << log.open_file().filename();
            }
            // Testing purposes only, once the logs start working we're ging to
            // remove this

            reset_election_timeout();

            // TODO: implement logic here

            int prev_log_index = request->prevlogindex();
            int prev_log_term = request->prevlogterm();

            // reject the request if leader term is less than current term
            if (request->term() < currentTerm)
            {
                // currentTerm = request->term();
                reply->set_term(currentTerm);
                reply->set_success(false);
                lock_.Unlock();
                return grpc::Status::OK; // might need to change this
            }
            // increment term if RPC contains higher term and convert to follower
            else if (request->term() > currentTerm)
            {
                // TODO: add some way to get current servers address for logging purposes
                LOG(INFO) << "Server converting to follower ";
                currentTerm = request->term();
                ConvertToFollower();
            }

            // TODO: handle election timeout
            reset_election_timeout();

            // If the log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reject the request

            if (prev_log_index >= log_.size() || log_[prev_log_index].term() != prev_log_term)
            {
                reply->set_term(currentTerm);
                reply->set_success(false);
                lock_.Unlock();
                return grpc::Status::OK;
            }

            // iterate over entry and append to the log
            int index = prev_log_index + 1;

            for (const auto &entry : request->entries())
            {
                int term = entry.term();

                // TODO: implement append entries logic here
                // might need to fix

                if (index < log_.size() && log_[index].term() != term)
                {
                    // if the entry conflicts with the log, i.e has the same index but different term, delete entries
                    log_.resize(index);
                }

                if (index >= log_.size())
                {
                    log_.push_back(entry);
                    // raft_service_log_manager_->DeleteLogEntries(log_.size());
                }

                // raft_service_log_manager_->AppendLogEntries(log_);

                // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                if (request->leadercommit() > commitIndex)
                {
                    // log.size() - 1 is the index of last entry, might change depend on implementation
                    commitIndex = std::min(request->leadercommit(), (uint32_t)log_.size() - 1);
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

            lock_.Unlock();

            // If the leader's commit index is greater than ours, update our commit index

            return grpc::Status::OK;
        }

        void RaftServiceImpl::ConvertToFollower()
        {
            currState = State::Follower;
            votedFor = -1;
        }

        // hold lock while entering
        void RaftServiceImpl::ConvertToCandidate()
        {
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

            int requestTerm = currentTerm;
            for (auto server_name : all_servers)
            {
                LOG(INFO) << "Sending request vote RPC to server " << server_name;
                request_vote_results.push_back(
                    std::async(std::launch::async, [&, server_name]()
                               {
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
                    server_name, request_vote_reply); }));
            }

            // count the votes
            for (int i = 0; i < all_servers.size(); ++i)
            {
                // TODO: abstract this into configurable variable
                // wait for future with a timeout time
                lock_.Unlock();
                std::future_status status = request_vote_results[i].wait_for(std::chrono::milliseconds(500));
                lock_.Lock();
                if (currState != State::Candidate | currentTerm != requestTerm)
                {
                    return;
                }
                // check if future has resolved
                if (status == std::future_status::ready)
                {
                    LOG(INFO) << "Future instance successfully executed";
                    auto request_vote_result = request_vote_results[i].get();
                    auto server_name = request_vote_result.first;
                    auto request_vote_reply = request_vote_result.second;

                    // logic to handle votes
                    if (request_vote_reply.ok())
                    {
                        auto reply = request_vote_reply.value();
                        // if outdated term, convert to follower

                        LOG(INFO) << "Grant vote? " << reply.votegranted();
                        LOG(INFO) << "Term of reply " << reply.term();
                        LOG(INFO) << "Current term " << currentTerm;
                        if (reply.term() > currentTerm)
                        {
                            LOG(INFO) << "Server converting to follower ";
                            currentTerm = reply.term();
                            ConvertToFollower();
                            return;
                        }
                        else if (reply.votegranted() == 1 and reply.term() <= currentTerm)
                        {
                            numVotes++;
                        }
                    }
                }
            }

            LOG(INFO) << "Numbers of votes are " << numVotes;

            // TODO: CHANGE THIS TO CALCULATED QUORUM
            if (numVotes >= 1)
            {
                ConvertToLeader();
            }
        }

        RaftServiceImpl::State RaftServiceImpl::GetCurrentState()
        {
            return currState;
        }

        // hold lock
        void RaftServiceImpl::reset_election_timeout()
        {
            // TODO: add a Timer here
            if (currState == State::Leader)
            {
                return;
            }

            int ELECTION_TIMEOUT_LOW = 1000;
            int ELECTION_TIMEOUT_HIGH = 2000;

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);

            float election_timeout = dis(gen);

            SetAlarm(election_timeout);
        }

        void RaftServiceImpl::reset_heartbeat_timeout()
        {
            int HEARTBEAT_TIMEOUT_LOW = 100;
            int HEARTBEAT_TIMEOUT_HIGH = 200;

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(HEARTBEAT_TIMEOUT_LOW, HEARTBEAT_TIMEOUT_HIGH);

            float heartbeat_timeout = dis(gen);

            SetAlarm(heartbeat_timeout);
        }

        // We should be going into this function holding the lock. We should leave it holding the lock.
        void RaftServiceImpl::ConvertToLeader()
        {

            currState = State::Leader;

            // initialize nextIndex and matchIndex
            for (std::string server_name : all_servers)
            {
                nextIndex[server_name] = log_.size();
                matchIndex[server_name] = 0;
            }

            // Upon election, send empty AppendEntries RPC to all other servers
            SendAppendEntries();
            // TODO: make a function that resets heartbeat timeout
            reset_election_timeout();
            LOG(INFO) << "Server converts to leader";
        }

        void RaftServiceImpl::SendAppendEntries()
        {
            std::deque<
                std::future<std::pair<std::string, StatusOr<AppendEntriesReply>>>>
                append_entries_results;

            for (auto server_name : all_servers)
            {
                LOG(INFO) << "Sending an empty AppendEntries RPC upon election to server" << server_name;
                append_entries_results.push_back(
                    std::async(std::launch::async, [&, server_name]()
                               {
                // create reply and send
                AppendEntriesRequest request = createAppendEntriesRequest(server_name);
                // AppendEntriesRequest request;
                auto client = masterServerClients[server_name];
                auto append_entries_reply = client->SendRequest(request);

                return std::pair<std::string, StatusOr<AppendEntriesReply>>(
                    server_name, append_entries_reply); }));
            }

            while (!append_entries_results.empty())
            {
                auto response_future = std::move(append_entries_results.front());
                append_entries_results.pop_front();

                // TODO: abstract this into configurable variable
                // wait for future with a timeout time
                std::future_status status = response_future.wait_for(std::chrono::milliseconds(500));

                // check if the future has resolved
                if (status == std::future_status::ready)
                {
                    std::pair<std::string, StatusOr<AppendEntriesReply>> append_entries_result = response_future.get();
                    std::string server_name = append_entries_result.first;
                    StatusOr<AppendEntriesReply> append_entries_reply = append_entries_result.second;

                    if (append_entries_reply.ok())
                    {
                        AppendEntriesReply reply = append_entries_reply.value();
                        // TODO: add logic to resend appendentries if needed
                        LOG(INFO) << "Received AppendEntriesReply " << reply.SerializeAsString();
                    }
                    else
                    {
                    }
                }
            }
        }

        protos::grpc::AppendEntriesRequest RaftServiceImpl::createAppendEntriesRequest(std::string server_name)
        {
            AppendEntriesRequest request;
            request.set_term(currentTerm);
            request.set_leaderid(currLeader);

            int prev_log_index = nextIndex[server_name] - 1;

            // index of log entry immediately preceding new ones
            request.set_prevlogindex(prev_log_index);

            // term of prevLogIndex entry
            if (!log_.empty()) {
                request.set_prevlogterm(log_[prev_log_index].term());
            }
            request.set_leadercommit(commitIndex);

            // log entries to store
            for (int j = prev_log_index + 1; j < log_.size(); j++)
            {
                LogEntry *entry = request.add_entries();
            }
            return request;
        }

protos::grpc::AppendEntriesRequest RaftServiceImpl::createAppendEntriesRequest(std::string server_name){
    AppendEntriesRequest request;
    request.set_term(currentTerm);
    request.set_leaderid(currLeader);

    int prev_log_index = nextIndex[server_name] - 1;

    //index of log entry immediately preceding new ones
    if(log_.size() == 0){
        request.set_prevlogindex(-1);
        request.set_prevlogterm(-1);
        request.set_leadercommit(0);
    }
    else{
        request.set_prevlogindex(prev_log_index);
    // term of prevLogIndex entry
        request.set_prevlogterm(log_[prev_log_index].term());
        request.set_leadercommit(commitIndex);
    }
    

    // log entries to store
    for(int j = prev_log_index + 1; j < log_.size(); j++){
        LogEntry* entry = request.add_entries();
    }
    return request;
}


}
}