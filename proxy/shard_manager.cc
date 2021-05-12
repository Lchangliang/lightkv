#include "shard_manager.h"

namespace lightkv {

void ShardManager::receive_heartBeat(const HeartBeatRequest* request) {
    std::string address;
    endpoint_to_string(request->address(), &address);
    if (storages.find(address) == storages.end()) {   // can not find address, the storage is new
        StorageState ss;
        ss.last_heartbeat = butil::gettimeofday_s();
        ss.offline = false;
        storages[address] = ss;
    } else {  // has the address
        storages[address].last_heartbeat = butil::gettimeofday_s();
        if (storages[address].offline) { // remove peer before
            storages[address].offline = false;
            brpc::Channel channel;
            EndPoint endpoint;
            string_to_endpoint(address, &endpoint);
            if (channel.Init(endpoint.ip().c_str(), endpoint.port(), NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to " << endpoint.ip() + ":" + std::to_string(endpoint.port());
                return ;
            }
            StorageService_Stub stub(&channel);
            for (ShardID shard_id : storages[address].remove_shard) {
                DeleteStoreRequest request;
                request.set_shard_id(shard_id);
                DeleteStoreResponse response;
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
                stub.delete_store(&cntl, &request, &response, NULL);
            }
            storages[address].remove_shard.clear();
            return ;
        }
    }
    for (int i = 0; i < request->store_shards_size(); i++) {
        bool is_exist = false;
        for (Tablet& tablet : shards[request->store_shards(i)]) { 
            if (tablet.address == address) {
                tablet.is_leader = false;
                is_exist = true;
            }
        }
        if (!is_exist) {
            Tablet tablet;
            tablet.address = address;
            tablet.is_leader = false;
            shards[request->store_shards(i)].push_back(tablet);
        }
    }
    for (int i = 0; i < request->leader_shards_size(); i++) {
        bool is_exist = false;
        for (Tablet& tablet : shards[request->leader_shards(i)]) { 
            if (tablet.address == address) {
                tablet.is_leader = true;
                is_exist = true;
            }
        }
        if (!is_exist) {
            Tablet tablet;
            tablet.address = address;
            tablet.is_leader = true;
            shards[request->leader_shards(i)].push_back(tablet);
        }
    }
}

void ShardManager::check_offline_storage() {
    int64_t cur_time = butil::gettimeofday_s();
    for (auto it = storages.begin(); it != storages.end(); it++) {
        if (cur_time - it->second.last_heartbeat >= HEARTBEAT_TIMEOUT) {
            it->second.offline = true;
        }
    }
}

void ShardManager::get_current_shards_state(std::vector<int>* need_replica) {
    for (int i = 0; i < shard_count; i++) {
        int need = replica;
        if (shards[i].size() != 0) {
            for (Tablet& tablet : shards[i]) {
                if (!storages[tablet.address].offline) {
                    need--;
                }
            }
        }
        if (need != 0) {
            LOG(INFO) << "shard_id: " + std::to_string(i) << " need replica: " + std::to_string(need);
        }
        need_replica->push_back(need);
    }
}

void ShardManager::get_new_replica_address(ShardID shard_id, const std::vector<std::string>& addresses, std::vector<std::string>* add_replicas) {
    srand(butil::gettimeofday_s());
    int start_index = rand() % addresses.size();
    for (int k = 0; k < addresses.size(); k++) {
        int cur_index = (start_index + k) % addresses.size();
        if (!storages[addresses[cur_index]].offline) {
            bool _flag = true;
            for (Tablet& tablet : shards[shard_id]) {
                if (tablet.address == addresses[cur_index]) {
                    _flag = false;
                    break;
                }
            }
            for (int i = 0; i < add_replicas->size(); i++) {
                if (addresses[cur_index] == add_replicas->at(i)) {
                    _flag = false;
                    break;
                }
            }
            if (_flag) {
                add_replicas->push_back(addresses[cur_index]);
                break;
            }
        }
    }
}

void ShardManager::get_old_replica_addresses(ShardID shard_id, std::vector<std::string>* old_replicas,
            std::vector<std::string>* remove_replicas) {
    for (Tablet& tablet : shards[shard_id]) {
        if (!storages[tablet.address].offline) {
            old_replicas->push_back(tablet.address);
        } else {
            remove_replicas->push_back(tablet.address);
        }
    }
}

braft::Configuration ShardManager::get_current_config(ShardID shard_id) {
    butil::StringPiece conf;
    std::string _conf;
    for (Tablet& tablet : shards[shard_id]) {
        _conf += tablet.address + ":" + std::to_string(shard_id) + ",";
    }
    conf.set(_conf.c_str());
    braft::Configuration braft_conf;
    braft_conf.parse_from(conf);
    return braft_conf;
}

braft::Configuration ShardManager::get_current_config(ShardID shard_id, const std::vector<std::string>& old_replicas) {
    braft::Configuration conf;
    std::string _conf;
    for (std::string replica : old_replicas) {
        _conf += replica + ":" + std::to_string(shard_id) + ",";
    }
    conf.parse_from(_conf);
    return conf;
}

void* ShardManager::_start(void* arg) {
    ShardManager* shard_manager = (ShardManager*) arg;
    for (;;) {
        sleep(SHARD_MANAGER_INTERVAL);
        if (shard_manager->storages.size() < shard_manager->replica) continue;
        shard_manager->check_offline_storage();
        std::vector<int> need_replica;
        shard_manager->get_current_shards_state(&need_replica);
        std::vector<std::string> addresses;
        for (auto& pair : shard_manager->storages) {
            addresses.push_back(pair.first);
        }
        for (int i = 0; i < need_replica.size(); i++) {
            if (need_replica[i] == 0) continue;
            std::vector<std::string> add_replicas;
            std::vector<std::string> old_replicas;
            std::vector<std::string> remove_replicas;
            for (int j = 0; j < need_replica[i]; j++) {
                shard_manager->get_new_replica_address(i, addresses, &add_replicas);
            }
            shard_manager->get_old_replica_addresses(i, &old_replicas, &remove_replicas);
            braft::Configuration config = get_current_config(i, old_replicas);
            if (add_replicas.size() < remove_replicas.size()) {
                int number = remove_replicas.size() - add_replicas.size();
                while (number > 0) {
                    old_replicas.push_back(remove_replicas.back());
                    remove_replicas.pop_back();
                    number--;
                }
            }
            for (int j = 0; j < add_replicas.size(); j++) {
                braft::PeerId add_peer;
                add_peer.parse(add_replicas[j] + ":" + std::to_string(i));
                InitStoreRequest request;
                request.set_shard_id(i);
                InitStoreResponse response;
                brpc::Channel channel;
                EndPoint endpoint;
                string_to_endpoint(add_replicas[j], &endpoint);
                if (channel.Init(endpoint.ip().c_str(), endpoint.port(), NULL) != 0) {
                    LOG(ERROR) << "Fail to init channel to " << endpoint.ip() + ":" + std::to_string(endpoint.port());
                    continue;
                }
                for (std::string replica : add_replicas) {
                    string_to_endpoint(replica, request.add_peers());
                }       
                for (std::string replica : old_replicas) {
                    string_to_endpoint(replica, request.add_peers());
                }
                Tablet tablet;
                tablet.address = add_replicas[j];
                tablet.is_leader = false;
                shard_manager->shards[i].push_back(tablet);
                StorageService_Stub stub(&channel);
                brpc::Controller cntl;
                cntl.set_timeout_ms(FLAGS_timeout_ms);
                stub.init_store(&cntl, &request, &response, NULL);
                braft::cli::add_peer(FLAGS_group + "_" + std::to_string(i), config, add_peer, braft::cli::CliOptions());
                config.add_peer(add_peer);
            }
            for (int j = 0; j < remove_replicas.size(); j++) {
                braft::PeerId remove_peer;
                remove_peer.parse(remove_replicas[j] + ":" + std::to_string(i));
                braft::cli::remove_peer(FLAGS_group + "_" + std::to_string(i), config, remove_peer, braft::cli::CliOptions());
                shard_manager->storages[remove_replicas[j]].remove_shard.push_back(i);
                for (auto it = shard_manager->shards[i].begin(); it != shard_manager->shards[i].end(); it++) {
                    if (it->address == remove_replicas[j]) {
                        shard_manager->shards[i].erase(it);
                    }
                }
            }
        }
    }
}

void ShardManager::start() {
    bthread_t id;
    bthread_start_background(&id, NULL, _start, this);
}

}