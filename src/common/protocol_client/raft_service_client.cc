#include "src/common/protocol_client/raft_service_client.h"

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::Empty;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::RequestVoteRequest;
using protos::grpc::RequestVoteReply;
using protos::grpc::AppendEntriesRequest;
using protos::grpc::AppendEntriesReply;
using protos::grpc::OpenFileRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::DeleteFileRequest;


namespace gfs {
namespace service {

StatusOr<RequestVoteReply> RaftServiceClient::SendRequest(
    const RequestVoteRequest& request, ClientContext& context) {
  RequestVoteReply reply;
  grpc::Status status = stub_->RequestVote(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<RequestVoteReply> RaftServiceClient::SendRequest(
    const RequestVoteRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<AppendEntriesReply> RaftServiceClient::SendRequest(
    const AppendEntriesRequest& request, ClientContext& context) {
  AppendEntriesReply reply;
  grpc::Status status = stub_->AppendEntries(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<AppendEntriesReply> RaftServiceClient::SendRequest(
    const AppendEntriesRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<OpenFileReply> RaftServiceClient::SendRequest(
    const OpenFileRequest& request, ClientContext& context) {
  OpenFileReply reply;
  grpc::Status status = stub_->OpenFile(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<OpenFileReply>RaftServiceClient::SendRequest(
    const OpenFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

Status RaftServiceClient::SendRequest(
    const DeleteFileRequest& request, ClientContext& context) {
  google::protobuf::Empty reply;
  grpc::Status status = stub_->DeleteFile(&context, request, &reply);
  return ConvertGrpcStatusToProtobufStatus(status);
}

Status RaftServiceClient::SendRequest(
    const DeleteFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}
}  // namespace service
}  // namespace gfs