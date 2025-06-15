// ========================================
// HARIYA KV STORE - Header File (WAL-Enabled)
// ========================================

#ifndef KV_STORE_H
#define KV_STORE_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "wal_system.h"  // NEW: Include our WAL system

class ThreadSafeKVStore {
private:
    std::unordered_map<std::string, std::string> store;
    mutable std::mutex storeMutex;
    WriteAheadLog wal;  // NEW: WAL instance for crash recovery
    
public:
    // NEW: Constructor now takes WAL file path
    explicit ThreadSafeKVStore(const std::string& wal_file_path = "data/hariya.wal");
    
    // Existing public interface remains the same
    std::string processCommand(const std::string& command);
    
    // NEW: Method to get store statistics including WAL info
    void printDetailedStats() const;
    
    // NEW: Force WAL to flush to disk
    void sync();

    std::vector<std::string> getAllKeys() const;
    
private:
    // Existing command handlers (mostly unchanged)
    std::string handleGet(const std::string& key);
    std::string handleSet(const std::string& key, const std::string& value);
    std::string handleDelete(const std::string& key);
    std::string handleExists(const std::string& key);
    std::string handleSize();
    std::string handleKeys();
    std::string handleClear();
    std::string handleInfo();
};  

#endif // KV_STORE_H