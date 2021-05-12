#ifndef LIGHTKV_SHARD_MANAGER
#define LIGHTKV_SHARD_MANAGER

#include <vector>
#include <string>
#include <butil/endpoint.h>
#include <butil/time.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <unordered_map>
#include <braft/cli.h>
#include <unordered_set>
#include <braft/configuration.h>
#include "util/util.h"
#include "interface/lightkv.pb.h"

DECLARE_int32(timeout_ms);
DECLARE_string(group);

namespace lightkv {

struct Tablet {
    std::string address;
    bool is_leader;
};

struct StorageState {
    int64_t last_heartbeat;
    bool offline;
    std::vector<ShardID> remove_shard;
};

class ShardManager {
public: 
    void set_config(int shard_count, int replica) {
        LOG(INFO) << "shard_count: " + std::to_string(shard_count) + " replica: " + std::to_string(replica); 
        this->shard_count = shard_count;
        this->replica = replica;
        shards.resize(shard_count);
        for (int i = 0; i < shard_count; i++) {
            shards.push_back(std::vector<Tablet>(replica));
        }
    }
    void start();
    void receive_heartBeat(const HeartBeatRequest* request);
    void check_offline_storage();
    std::string get_leader(ShardID shard_id) {
        std::vector<std::string> leaders;
        for (Tablet& tablet : shards[shard_id]) {
            if (tablet.is_leader) {
                leaders.push_back(tablet.address);
            }
        }
        int newest_leader_index = -1;
        int64_t newest_time = 0;
        for (int i = 0; i < leaders.size(); i++) {
            if (!storages[leaders[i]].offline && storages[leaders[i]].last_heartbeat > newest_time) {
                newest_time = storages[leaders[i]].last_heartbeat;
                newest_leader_index = i;
            }
        }
        if (newest_leader_index != -1) {
            return leaders[newest_leader_index];
        }
        return "0:0";
    }
    void get_shards(const std::string& address, std::vector<ShardID>* shard_ids);
    void get_current_shards_state(std::vector<int>*);
    void get_new_replica_address(ShardID shard_id, const std::vector<std::string>&, std::vector<std::string>*);
    void get_old_replica_addresses(ShardID shard_id, std::vector<std::string>*, std::vector<std::string>*);
    braft::Configuration get_current_config(ShardID shard_id);

    int get_shard_count() {
        return shard_count;
    }
    
public:
    static void* _start(void*);
    static braft::Configuration get_current_config
            (ShardID shard_id, const std::vector<std::string>& old_replicas);
    std::vector<std::vector<Tablet>> shards;
    std::unordered_map<std::string, StorageState> storages;
    int shard_count;
    int replica;
};

}
#endif