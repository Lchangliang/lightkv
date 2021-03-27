#ifndef LIGHTKV_ROCKSDB_STORE
#define LIGHTKV_ROCKSDB_STORE

#include <cassert>
#include <unistd.h>
#include <mutex>
#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/checkpoint.h>
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include "util/util.h"
#include "store.h"

namespace lightkv {
class RocksDBStoreImpl : public StoreInterface {
public:
    RocksDBStoreImpl(const std::string& store_path) : _store_path(store_path), db(nullptr) {}
    ~RocksDBStoreImpl() { delete db; }
    Error insert(const std::string&, const std::string&);
    Error delete_(const std::string&);
    Error select(const std::string&, std::vector<std::pair<std::string, std::string>>*);
    Error select_prefix(const std::string&, std::vector<std::pair<std::string, std::string>>*);
    Error select_range(const std::string&, const std::string&, std::vector<std::pair<std::string, std::string>>*);
    Error do_checkpoint(const std::string&);
    Error read_snapshot(const std::vector<std::string>&);
private:
    void init_rocksdb() {
        if (db == nullptr) {
            std::lock_guard<std::mutex> guard(mutex);
            if (db == nullptr) {
                LOG(INFO) << "init rocksdb";
                rocksdb::Options options;
                options.create_if_missing = true;
                rocksdb::Status status = rocksdb::DB::Open(options, _store_path, &db);
                assert(status.ok());   
            }
        }
    }
    const std::string _store_path;
    rocksdb::DB* db;
    std::mutex mutex;
};
} // namespace LightKV
#endif