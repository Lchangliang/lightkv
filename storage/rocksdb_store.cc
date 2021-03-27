#include <rocksdb/slice.h>
#include "rocksdb_store.h"

namespace lightkv {
Error RocksDBStoreImpl::insert(const std::string& key, const std::string& value) {
    init_rocksdb();
    rocksdb::Slice key_(key);
    rocksdb::Slice value_(value);
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::Status status = db->Put(write_option, key, value);
    LOG(WARNING) << "insert key: " << key << " value: " << value;
    LOG(WARNING) << "code: " << status.code() << " message: " << status.ToString();
    Error error;
    error.set_error_code(status.code());
    error.set_error_message(status.ToString());
    return error;
};

Error RocksDBStoreImpl::select(const std::string& key, std::vector<std::pair<std::string, std::string>>* pairs) {
    init_rocksdb();
    rocksdb::Slice key_(key);
    rocksdb::ReadOptions read_option;
    std::string value;
    rocksdb::Status status = db->Get(read_option, key_, &value);
    LOG(WARNING) << "select from key: " << key << " get value: " << value;
    pairs->push_back(std::pair<std::string, std::string>(key, value));
    Error error;
    error.set_error_code(status.code());
    error.set_error_message(status.ToString());
    return error;
};

Error RocksDBStoreImpl::delete_(const std::string& key) {
    init_rocksdb();
    rocksdb::Slice key_(key);
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::Status status = db->Delete(write_option, key);
    Error error;
    error.set_error_code(status.code());
    error.set_error_message(status.ToString());
    return error;
};

Error RocksDBStoreImpl::select_prefix(const std::string& key, std::vector<std::pair<std::string, std::string>>* pairs) {
    init_rocksdb();
    Error error;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(key); it->Valid() && it->key().ToString().find(key) == 0; it->Next()) {
        pairs->push_back(std::make_pair<std::string, std::string>(it->key().ToString(), it->value().ToString()));
    }
    if (pairs->empty()) {
        error.set_error_code(-2);
        error.set_error_message("The result is empty");
    } else {
        error.set_error_code(0);
        error.set_error_message("");  
    }
    return error; 
};

Error RocksDBStoreImpl::select_range(const std::string& llimit, const std::string& rlimit, std::vector<std::pair<std::string, std::string>>* pairs) {
    init_rocksdb();
    Error error;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    if (llimit == "") {
        for (it->SeekForPrev(rlimit); it->Valid(); it->Prev()) {
            pairs->push_back(std::make_pair<std::string, std::string>(it->key().ToString(), it->value().ToString()));
        }
    } else if (rlimit == "") {
        for (it->Seek(llimit); it->Valid(); it->Next()) {
            pairs->push_back(std::make_pair<std::string, std::string>(it->key().ToString(), it->value().ToString()));
        }
    } else {
        for (it->Seek(llimit); it->Valid() && it->key().ToString() <= rlimit; it->Next()) {
            pairs->push_back(std::make_pair<std::string, std::string>(it->key().ToString(), it->value().ToString()));
        }
    }
    if (pairs->empty()) {
        error.set_error_code(-2);
        error.set_error_message("The result is empty");
    } else {
        error.set_error_code(0);
        error.set_error_message("");  
    }
    return error; 
};

Error RocksDBStoreImpl::do_checkpoint(const std::string& snapshot_path) {
    Error error;
    if (db != nullptr) {
        rocksdb::Checkpoint* checkpoint_ptr;
        rocksdb::Checkpoint::Create(db, &checkpoint_ptr);
        rocksdb::Status status = checkpoint_ptr->CreateCheckpoint(snapshot_path);
        error.set_error_code(status.code());
        error.set_error_message(status.ToString());
    } else {
        error.set_error_code(0);
    }
    return error;
}

Error RocksDBStoreImpl::read_snapshot(const std::vector<std::string>& files) {
    if (db != nullptr) {
        rocksdb::Status status = db->Close();
        assert(status.ok());
        delete db;
        db = nullptr;
    }
    if (does_dir_exist(_store_path)) {
        rmdir(_store_path.c_str());
    }
    mkdir(_store_path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    for (auto& file : files) {
        std::string new_path = _store_path + "/" + file.substr(file.find_last_of("/")+1);
        if (file.substr(file.find_last_of(".") + 1) == "sst") {
            link(file.c_str(), new_path.c_str());
        } else {
            copy_file(file.c_str(), new_path.c_str());
        }
    }
    init_rocksdb();
    Error error;
    error.set_error_code(0);
    error.set_error_message("");
    return error;
}

}; // namespace LightKV