#ifndef GFS_COMMON_PROTOCOL_CLIENT_RAFT_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_RAFT_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/protos/grpc/raft_service.grpc.pb.h"

namespace gfs {
namespace service {

// Communication manager for sending Raft Requests between master servers.
class RaftServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in the
  // RaftService.
  RaftServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(protos::grpc::RaftService::NewStub(std::static_pointer_cast<grpc::ChannelInterface>(channel))) {}

  // Send an RequestVote gRPC |request| to the master server, and return master's
  // corresponding reply if successful; otherwise a Status with error message.
  google::protobuf::util::StatusOr<protos::grpc::RequestVoteReply> SendRequest(
      const protos::grpc::RequestVoteRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::RequestVoteReply> SendRequest(
      const protos::grpc::RequestVoteRequest& request,
      grpc::ClientContext& context);

  // Send an AppendEntries gRPC |request| to the master server. 
  google::protobuf::util::StatusOr<protos::grpc::AppendEntriesReply> SendRequest(
      const protos::grpc::AppendEntriesRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::AppendEntriesReply> SendRequest(
      const protos::grpc::AppendEntriesRequest& request,
      grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in RaftSerice
  std::unique_ptr<protos::grpc::RaftService::Stub> stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_MASTER_SERVICE_CLIENT_H_