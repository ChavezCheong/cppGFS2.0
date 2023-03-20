// TODO: intended to be the interface that implements the heartbeat tasks
// might be deleted if we can directly change the interface and implementation in 
// chunk_server_impl or chunk_server_control_service_impl
// TODO: config in BAZEL after finish implementing
// Each chunkserver will periodically send to the current master leader the chunkservers' current states