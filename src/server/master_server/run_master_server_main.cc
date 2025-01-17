#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_heartbeat_monitor_task.h"
#include "src/server/master_server/master_chunk_server_manager_service_impl.h"
#include "src/server/master_server/master_metadata_service_impl.h"
#include "src/server/master_server/raft_service_log_manager.h"
#include "raft_service_impl.h"

using gfs::common::ConfigManager;
using gfs::server::ChunkServerHeartBeatMonitorTask;
using gfs::service::MasterChunkServerManagerServiceImpl;
using gfs::service::MasterMetadataServiceImpl;
using gfs::service::RaftServiceImpl;
using gfs::service::RaftServiceLogManager;
using grpc::Server;
using grpc::ServerBuilder;

ABSL_FLAG(std::string, config_path, "data/config.yml", "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "run as the given master, as defined in the config");
ABSL_FLAG(bool, use_docker_dns_server, false, "use docker's DNS server");

int main(int argc, char **argv)
{
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // Parse command line arguments
  absl::ParseCommandLine(argc, argv);
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string master_name = absl::GetFlag(FLAGS_master_name);
  const bool resolve_hostname = !absl::GetFlag(FLAGS_use_docker_dns_server);

  // Initialize configurations
  LOG(INFO) << "Reading GFS configuration: " << config_path;
  ConfigManager *config = ConfigManager::GetConfig(config_path).value();
  if (!config->HasMasterServer(master_name))
  {
    LOG(ERROR) << "No master server found in config: " << master_name;
    return 1;
  }

  LOG(INFO) << "Running as master server: " << master_name;
  LOG(INFO) << "Server starting...";

  // Initialize the raft service log manager
  auto master_database_name = config->GetDatabaseName(master_name);
  RaftServiceLogManager::GetInstance()->Initialize(master_database_name);
  LOG(INFO) << "Raft Service Log Manager initialized with master database: "
            << master_database_name;

  ServerBuilder builder;
  auto credentials = grpc::InsecureChannelCredentials();

  std::string server_address(
      config->GetServerAddress(master_name, resolve_hostname));

  // Listen on the given address without any authentication mechanism for now.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  // Register a synchronous service for handling clients' metadata requests
  MasterMetadataServiceImpl metadata_service(config, resolve_hostname);
  builder.RegisterService(&metadata_service);

  // Register a synchronous service for coordinating with chunkservers
  MasterChunkServerManagerServiceImpl chunk_server_mgr_service;
  builder.RegisterService(&chunk_server_mgr_service);

  // Register a synchronous service for Raft fault tolerance
  RaftServiceImpl raft_service(config);
  raft_service.Initialize(master_name, resolve_hostname);
  builder.RegisterService(&raft_service);

  // Assemble and start the server
  std::unique_ptr<Server> server(builder.BuildAndStart());

  LOG(INFO) << "Server listening on " << server_address;

  // Start the chunk server heartbeat monitor task after services have been
  // started.
  auto chunk_servers_heartbeat_task =
      ChunkServerHeartBeatMonitorTask::GetInstance();
  chunk_servers_heartbeat_task->Start(config, resolve_hostname);

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  // Shutdown the task
  chunk_servers_heartbeat_task->Terminate();

  return 0;
}
