#include "heartbeat.h"

namespace lightkv {

void* HeartBeat::_start(void* arg) {
    while (true) {
        sleep(HEARTBEAT_INTERVAL);
        LOG(WARNING) << "send a heartBeadt";
        HeartBeat* point = (HeartBeat*)arg;
        HeartBeatRequest request;
        request.mutable_address()->set_ip(point->ip);
        request.mutable_address()->set_port(point->port);
        HeartBeatResponse response;
        brpc::Channel channel;
        if (channel.Init(point->proxy_ip.c_str(), point->proxy_port, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << point->proxy_ip << ":" << point->proxy_port;
            continue ;
        }
        std::shared_lock<std::shared_mutex> lock(point->storage_map->_mutex);
        for (auto& pair : point->storage_map->kv_stores) {
            if (pair.second.is_leader()) {
                request.mutable_leader_shards()->Add(pair.first);
            } else {
                request.mutable_store_shards()->Add(pair.first);
            }
        }
        brpc::Controller cntl;
        ProxyService_Stub stub(&channel);
        stub.heart_beat(&cntl, &request, &response, NULL);
    }
    return nullptr;
}

void HeartBeat::start() {
    bthread_t id;
    bthread_start_background(&id, NULL, _start, this);
}

}