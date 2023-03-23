#ifndef GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_
#define GFS_SERVER_MASTER_SERVER_RAFT_CONSENSUS_H_

#include "src/protos/grpc/raft_service.grpc.pb.h"

namespace gfs{
namespace server{

// This interface aims to extend single-master server to multiple master servers
// where we obtain consensus using Raft algorithm

class RaftServiceImpl final : public protos::grpc::RaftService::Service{
    public:


};

}
}


#endif 