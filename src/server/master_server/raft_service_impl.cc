#include "raft_service_impl.h"
#include "absl/synchronization/mutex.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"
#include "src/server/master_server/master_metadata_service_impl.h"
#include "src/common/system_logger.h"
#include <csignal>
#include <random>
#include <future>
#include <utility>
#include <vector>
#include <deque>
#include <thread>

using gfs::service::MasterMetadataServiceImpl;
using google::protobuf::Empty;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ServerCompletionQueue;
using protos::grpc::AppendEntriesReply;
using protos::grpc::AppendEntriesRequest;
using protos::grpc::DeleteFileRequest;
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

            if (master_name == "master_server_01")
            {
                serverId = 1;
            }
            else if (master_name == "master_server_02")
            {
                serverId = 2;
            }
            else if (master_name == "master_server_03")
            {
                serverId = 3;
            }
            else if (master_name == "master_server_04")
            {
                serverId = 4;
            }
            else if (master_name == "master_server_05")
            {
                serverId = 5;
            }
            else if (master_name == "master_server_06")
            {
                serverId = 6;
            }
            else if (master_name == "master_server_07")
            {
                serverId = 7;
            }

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
                LOG(INFO) << "Establishing new connection to server: "
                          << server_address;
                masterServerClients[server_name] =
                    std::make_shared<RaftServiceClient>(
                        grpc::CreateChannel(server_address,
                                            grpc::InsecureChannelCredentials()));
            }
            // Set up raft service log manager for use
            raft_service_log_manager_ = RaftServiceLogManager::GetInstance();
            LOG(INFO) << "Starting Raft Service...";
            currState = State::Follower;
            auto vote = raft_service_log_manager_->GetVotedFor();
            if (vote.ok())
            {
                votedFor = vote.value();
                LOG(INFO) << "Get votedFor " << votedFor << " from DB.";
            }
            else
            {
                votedFor = 0;
            }

            auto curr_term = raft_service_log_manager_->GetCurrentTerm();

            if (curr_term.ok())
            {
                currentTerm = curr_term.value();
                LOG(INFO) << "Get current Term " << currentTerm << " from DB.";
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
                LOG(INFO) << "EMPTY LOG";
                // initialize with an empty entry
                LogEntry empty_entry;
                empty_entry.set_term(0);
                empty_entry.set_index(0);
                log_ = std::vector<LogEntry>(0);
                log_.push_back(empty_entry);
            }

            commitIndex = 0;
            lastApplied = 0;

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
            lock_.Lock();
            if (currState == State::Leader)
            {
                LOG(INFO) << "Handle OpenFile Request from client.";
                MasterMetadataServiceImpl MetadataHandler(config_manager_, resolve_hostname_);
                if (request->mode() == protos::grpc::OpenFileRequest::CREATE)
                {
                    LogEntry new_log;
                    OpenFileRequest *new_request = new protos::grpc::OpenFileRequest(*request);
                    new_log.set_allocated_open_file(new_request);
                    new_log.set_index(log_.size());
                    new_log.set_term(currentTerm);
                    log_.push_back(new_log);
                    // for (auto entry : log_) {
                    //     LOG(INFO) << entry.term();
                    //     LOG(INFO) << entry.open_file().filename();
                    // }
                    LOG(INFO) << "(LEADER) Replicated Create File request to followers.";
                    SendAppendEntries();
                    lock_.Unlock();
                    protos::ChunkServerLocation *new_location;
                    return MetadataHandler.OpenFile(context, request, reply, true, new_location, 0);
                }
                else if (request->mode() == protos::grpc::OpenFileRequest::WRITE)
                {
                    LogEntry new_log;
                    OpenFileRequest *new_request = new protos::grpc::OpenFileRequest(*request);
                    new_log.set_allocated_open_file(new_request);
                    new_log.set_index(log_.size());
                    new_log.set_term(currentTerm);
                    // Get lease and check if a lease currently is still valid. If it is, we don't need to grant it so no need to log.
                    auto lease_pair = MetadataHandler.GetPreLeaseMetadata(request);
                    if (lease_pair.ok())
                    {
                        protos::ChunkServerLocation *new_location = new protos::ChunkServerLocation(lease_pair.value().first);
                        new_log.set_allocated_chunk_server_location(new_location);
                        new_log.set_expirationtime(lease_pair.value().second);
                        log_.push_back(new_log);
                        LOG(INFO) << "(LEADER) Replicated Create Lease request to followers.";
                        SendAppendEntries();
                        lock_.Unlock();
                        return MetadataHandler.OpenFile(context, request, reply, true, new_location, lease_pair.value().second);
                    }
                    else
                    {
                        LOG(INFO) << "(LEADER) Metadata already exists, no need to append entries to log";
                        lock_.Unlock();
                        protos::ChunkServerLocation *new_location;
                        return MetadataHandler.OpenFile(context, request, reply, true, new_location, 0);
                    }
                }
                else {
                    // read request, just handle
                    LOG(INFO) << "(LEADER) Handling Read Request";
                        lock_.Unlock();
                        protos::ChunkServerLocation *new_location;
                        return MetadataHandler.OpenFile(context, request, reply, true, new_location, 0);
                }
            }
            else
            {
                LOG(INFO) << "(NOT LEADER) Reject request from client to get metadata.";
                lock_.Unlock();
                return grpc::Status::CANCELLED;
            }
        }

        grpc::Status RaftServiceImpl::DeleteFile(grpc::ServerContext *context,
                                                 const protos::grpc::DeleteFileRequest *request,
                                                 google::protobuf::Empty *reply)
        {
            lock_.Lock();
            if (currState == State::Leader)
            {
                LOG(INFO) << "Handle delete file request from client.";
                LogEntry new_log;
                DeleteFileRequest *new_request = new protos::grpc::DeleteFileRequest(*request);
                new_log.set_allocated_delete_file(new_request);
                new_log.set_index(log_.size());
                new_log.set_term(currentTerm);
                log_.push_back(new_log);
                // for (auto entry : log_) {
                //     LOG(INFO) << entry.term();
                //     LOG(INFO) << entry.delete_file().filename();
                // }
                SendAppendEntries();
                lock_.Unlock();
                MasterMetadataServiceImpl MetadataHandler(config_manager_, resolve_hostname_);
                return MetadataHandler.DeleteFile(context, new_request, reply);
            }
            else
            {
                LOG(INFO) << "(NOT LEADER) Reject request from client to get metadata.";
                lock_.Unlock();
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
                LOG(INFO) << "Server converting to follower.";
                currentTerm = request->term();
                ConvertToFollower();
                LOG(INFO) << "Current term " << currentTerm << " persisted into storage.";
                raft_service_log_manager_->UpdateCurrentTerm(request->term());
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
                raft_service_log_manager_->UpdateVotedFor(request->candidateid());
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

            // LOG(INFO) << "Handle Append Entries RPC from: " << request->leaderid();
            // LOG(INFO) << "Current Log Size: " << log_.size();
            // if (log_.size() > 1)
            // {
            //     LOG(INFO) << "Last Log Entry: " << log_[log_.size() - 1].open_file().filename();
            // }

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
                raft_service_log_manager_->UpdateCurrentTerm(currentTerm);
                // LOG(INFO) << "Current term " << currentTerm << " persisted into storage";
                ConvertToFollower();
            }

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
                    raft_service_log_manager_->DeleteLogEntries(log_.size());
                }

                // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                if (request->leadercommit() > commitIndex)
                {
                    // log.size() - 1 is the index of last entry, might change depend on implementation
                    commitIndex = std::min(request->leadercommit(), (uint32_t)log_.size() - 1);
                }
            }

            raft_service_log_manager_->AppendLogEntries(std::vector<LogEntry>(log_.begin() + prev_log_index, log_.end()));

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
            // Commit every entry up till leaderindex
            // Check if we need to
            if (request->leadercommit() > lastApplied)
            {
                for (uint32_t commit_index = lastApplied; commit_index < request->leadercommit(); ++commit_index)
                {
                    OpenFileRequest *new_request = new OpenFileRequest(log_[commit_index + 1].open_file());
                    OpenFileReply *new_reply;
                    MasterMetadataServiceImpl MetadataHandler(config_manager_, resolve_hostname_);
                    if (new_request->mode() == protos::grpc::OpenFileRequest::WRITE)
                    {
                        // apply the lease
                        protos::ChunkServerLocation *new_location = new protos::ChunkServerLocation(log_[commit_index + 1].chunk_server_location());
                        MetadataHandler.OpenFile(context, new_request, new_reply, false, new_location, log_[commit_index + 1].expirationtime());
                    }
                    else
                    {
                        protos::ChunkServerLocation *new_location;
                        MetadataHandler.OpenFile(context, new_request, new_reply, false, new_location, 0);
                    }
                    lastApplied++;
                }
            }

            lock_.Unlock();

            // If the leader's commit index is greater than ours, update our commit index

            return grpc::Status::OK;
        }

        void RaftServiceImpl::ConvertToFollower()
        {
            currState = State::Follower;
            votedFor = -1;
            raft_service_log_manager_->UpdateVotedFor(votedFor);
        }

        // hold lock while entering
        void RaftServiceImpl::ConvertToCandidate()
        {
            currState = State::Candidate;
            // Once a server is converted to candidate, we increase the current term
            currentTerm++;
            raft_service_log_manager_->UpdateCurrentTerm(currentTerm);
            LOG(INFO) << "Current term " << currentTerm << " persisted into storage";
            numVotes = 0;
            votedFor = serverId;
            raft_service_log_manager_->UpdateVotedFor(votedFor);

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
                if(log_.size() == 1){
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
                    auto request_vote_result = request_vote_results[i].get();
                    auto server_name = request_vote_result.first;
                    auto request_vote_reply = request_vote_result.second;

                    // logic to handle votes
                    if (request_vote_reply.ok())
                    {
                        auto reply = request_vote_reply.value();
                        // if outdated term, convert to follower

                        // LOG(INFO) << "Grant vote? " << reply.votegranted();
                        // LOG(INFO) << "Term of reply " << reply.term();
                        // LOG(INFO) << "Current term " << currentTerm;
                        if (reply.term() > currentTerm)
                        {
                            LOG(INFO) << "Server converting to follower ";
                            currentTerm = reply.term();
                            raft_service_log_manager_->UpdateCurrentTerm(currentTerm);
                            // LOG(INFO) << "Current term " << currentTerm << " persisted into storage";
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
            if (numVotes >= 3)
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

            /*If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
            • If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
            • If successful: update nextIndex and matchIndex for
            follower (§5.3)
            • If AppendEntries fails because of log inconsistency:
            decrement nextIndex and retry (§5.3)
            • If there exists an N such that N > commitIndex, a majority
            of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).*/
            int HEARTBEAT_TIMEOUT_LOW = 100;
            int HEARTBEAT_TIMEOUT_HIGH = 100;

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(HEARTBEAT_TIMEOUT_LOW, HEARTBEAT_TIMEOUT_HIGH);

            float heartbeat_timeout = dis(gen);

            SetAlarm(heartbeat_timeout);
            for (int N = commitIndex + 1; N < log_.size(); ++N)
            {
                if (log_[N].term() == currentTerm)
                {
                    int count = 1;
                    for (auto pair : matchIndex)
                    {
                        if (pair.second >= N)
                        {
                            count++;
                        }
                    }
                    if (count >= 2)
                    {
                        commitIndex = N;
                    }
                }
            }
            // uint32_t index_threshold;

            // std::sort(matchIndArr, matchIndArr + 3);
            // index_threshold = matchIndArr[1];

            // for (int N = commitIndex; N <= index_threshold; N++)
            // {
            //     // just in case this loops break
            //     if (N >= log_.size())
            //     {
            //         break;
            //     }
            //     if (log_[N].term() == currentTerm)
            //     {
            //         commitIndex = N;
            //         break;
            //     }
            // }
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
            reset_heartbeat_timeout();
            LOG(INFO) << "Server converts to leader";
        }

        // We should be going into this function holding the lock. We should leave it holding the lock.
        void RaftServiceImpl::SendAppendEntries()
        {
            std::deque<
                std::future<std::pair<std::string, StatusOr<AppendEntriesReply>>>>
                append_entries_results;

            int maxIndex = log_.size() - 1;
            int requestTerm = currentTerm;

            for (auto server_name : all_servers)
            {
                // LOG(INFO) << "Sending an AppendEntries RPC to server" << server_name;
                append_entries_results.push_back(
                    std::async(std::launch::async, [&, server_name]()
                               {
                // create reply and send
                AppendEntriesRequest request = createAppendEntriesRequest(server_name, maxIndex);
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
                lock_.Unlock();
                std::future_status status = response_future.wait_for(std::chrono::milliseconds(100));
                lock_.Lock();

                // re-check entry conditions
                if (currState != State::Leader || requestTerm != currentTerm)
                {
                    return;
                }

                // check if the future has resolved
                if (status == std::future_status::ready)
                {
                    std::pair<std::string, StatusOr<AppendEntriesReply>> append_entries_result = response_future.get();
                    std::string server_name = append_entries_result.first;
                    StatusOr<AppendEntriesReply> append_entries_reply = append_entries_result.second;

                    if (append_entries_reply.ok())
                    {
                        AppendEntriesReply reply = append_entries_reply.value();
                        // LOG(INFO) << "Received AppendEntriesReply " << reply.SerializeAsString();

                        if (reply.term() > currentTerm)
                        {
                            LOG(INFO) << "Server converting to follower ";
                            currentTerm = reply.term();
                            raft_service_log_manager_->UpdateCurrentTerm(currentTerm);
                            // LOG(INFO) << "Current term " << currentTerm << " persisted into storage";
                            ConvertToFollower();
                            return;
                        }
                        else if (reply.success())
                        {
                            nextIndex[server_name] = maxIndex + 1;
                            matchIndex[server_name] = maxIndex;
                            // LOG(INFO) << "successful from " << server_name << " " << nextIndex[server_name] << " " << matchIndex[server_name];
                        }
                        else
                        {
                            nextIndex[server_name] -= 1;
                            append_entries_results.push_back(std::async(std::launch::async, [&, server_name]() { // create reply and send
                                AppendEntriesRequest request = createAppendEntriesRequest(server_name, maxIndex);
                                // AppendEntriesRequest request;
                                auto client = masterServerClients[server_name];
                                auto append_entries_reply = client->SendRequest(request);
                                return std::pair<std::string, StatusOr<AppendEntriesReply>>(
                                    server_name, append_entries_reply);
                            }));
                        }
                    }
                }
            }
        }

        protos::grpc::AppendEntriesRequest RaftServiceImpl::createAppendEntriesRequest(std::string server_name, int maxIndex)
        {
            AppendEntriesRequest request;
            request.set_term(currentTerm);
            request.set_leaderid(serverId);

            int prev_log_index = nextIndex[server_name] - 1;

            // index of log entry immediately preceding new ones
            if (log_.size() == 1)
            {
                request.set_prevlogindex(0);
                request.set_prevlogterm(0);
                request.set_leadercommit(0);
            }
            else
            {
                request.set_prevlogindex(prev_log_index);
                // term of prevLogIndex entry
                request.set_prevlogterm(log_[prev_log_index].term());
                for (auto pair : matchIndex)
                {
                    // LOG(INFO) << "MATCH INDEX FOR SERVER " << pair.first << " is " << pair.second;
                }
                request.set_leadercommit(commitIndex);
            }
            // log entries to store
            for (int j = prev_log_index + 1; j <= maxIndex; j++)
            {
                request.add_entries()->CopyFrom(log_[j]);
            }
            return request;
        }
    }
}