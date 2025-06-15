// ========================================
// HARIYA KV STORE - Implementation (WAL-Enabled)
// ========================================

#include "kv_store.h"
#include <sstream>
#include <algorithm>
#include <ctime>
#include <iostream>

// NEW: Constructor with WAL recovery
ThreadSafeKVStore::ThreadSafeKVStore(const std::string& wal_file_path) 
    : wal(wal_file_path) {
    
    std::cout << "🚀 Initializing Hariya KV Store with WAL..." << std::endl;
    
    // CRITICAL: Recover state from WAL on startup
    // This is where the magic happens - we rebuild our store from the log
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        store = wal.replay();  // This rebuilds the entire store from WAL
    }
    
    std::cout << "✅ KV Store initialized with " << store.size() << " entries" << std::endl;
}

// Main command processor
std::string ThreadSafeKVStore::processCommand(const std::string& command) {
    std::istringstream iss(command);
    std::string cmd;
    iss >> cmd;
    
    // Convert to uppercase for case-insensitive commands
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    
    // Dispatch to appropriate handler based on command
    if (cmd == "GET") {
        std::string key;
        if (iss >> key) {
            return handleGet(key);
        }
        return "ERROR: GET requires a key\n";
    }
    else if (cmd == "SET" || cmd == "PUT") {
        std::string key, value;
        if (iss >> key) {
            // Read rest of line as value (handles spaces in values)
            std::getline(iss, value);
            value.erase(0, value.find_first_not_of(" \t")); // trim leading whitespace
            return handleSet(key, value);
        }
        return "ERROR: SET requires a key and value\n";
    }
    else if (cmd == "DEL" || cmd == "DELETE") {
        std::string key;
        if (iss >> key) {
            return handleDelete(key);
        }
        return "ERROR: DEL requires a key\n";
    }
    else if (cmd == "EXISTS") {
        std::string key;
        if (iss >> key) {
            return handleExists(key);
        }
        return "ERROR: EXISTS requires a key\n";
    }
    else if (cmd == "KEYS") {
        return handleKeys();
    }
    else if (cmd == "DBSIZE") {
        return handleSize();
    }
    else if (cmd == "FLUSHDB" || cmd == "CLEAR") {
        return handleClear();
    }
    else if (cmd == "PING") {
        return "PONG\n";
    }
    else if (cmd == "INFO") {
        return handleInfo();
    }
    else if (cmd == "SYNC") {  // NEW: Command to force WAL sync
        sync();
        return "OK - WAL synced to disk\n";
    }
    else if (cmd == "WALSTATS") {  // NEW: Command to show WAL statistics
        printDetailedStats();
        return "WAL statistics printed to console\n";
    }
    else if (cmd == "HELP") {
        return "HARIYA COMMANDS:\n"
               "GET key          - Get value for key\n"
               "SET key value    - Set key to value\n"
               "DEL key          - Delete key\n"
               "EXISTS key       - Check if key exists\n"
               "KEYS             - List all keys\n"
               "DBSIZE           - Get number of keys\n"
               "FLUSHDB          - Clear all keys\n"
               "PING             - Test connection\n"
               "INFO             - Server information\n"
               "SYNC             - Force WAL sync to disk\n"
               "WALSTATS         - Show WAL statistics\n"
               "HELP             - Show this help\n"
               "QUIT             - Close connection\n";
    }
    else {
        return "ERROR: Unknown command '" + cmd + "'. Type HELP for available commands\n";
    }
}

// GET handler (unchanged - no WAL needed for reads)
std::string ThreadSafeKVStore::handleGet(const std::string& key) {
    std::lock_guard<std::mutex> lock(storeMutex);
    auto it = store.find(key);
    if (it != store.end()) {
        return it->second + "\n";
    }
    return "(nil)\n";
}

// SET handler - THE KEY CHANGE: Write-Ahead Logging
std::string ThreadSafeKVStore::handleSet(const std::string& key, const std::string& value) {
    // STEP 1: Write to WAL FIRST (before changing memory)
    // This ensures durability - even if we crash right after this line,
    // the operation will be replayed on restart
    wal.logPut(key, value);
    
    // STEP 2: THEN update in-memory store
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        store[key] = value;
    }
    
    return "OK\n";
}

// DELETE handler - Also uses Write-Ahead Logging
std::string ThreadSafeKVStore::handleDelete(const std::string& key) {
    // Check if key exists first (to return correct count)
    bool key_existed;
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        key_existed = store.find(key) != store.end();
    }
    
    if (!key_existed) {
        return "0\n";  // Key didn't exist
    }
    
    // STEP 1: Write to WAL FIRST
    wal.logDelete(key);
    
    // STEP 2: THEN remove from memory
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        store.erase(key);
    }
    
    return "1\n";  // Successfully deleted
}

// EXISTS handler (unchanged)
std::string ThreadSafeKVStore::handleExists(const std::string& key) {
    std::lock_guard<std::mutex> lock(storeMutex);
    bool exists = store.find(key) != store.end();
    return std::to_string(exists ? 1 : 0) + "\n";
}

// SIZE handler (unchanged)
std::string ThreadSafeKVStore::handleSize() {
    std::lock_guard<std::mutex> lock(storeMutex);
    return std::to_string(store.size()) + "\n";
}

// KEYS handler (unchanged)
std::string ThreadSafeKVStore::handleKeys() {
    std::lock_guard<std::mutex> lock(storeMutex);
    if (store.empty()) {
        return "(empty list or set)\n";
    }
    
    std::string result;
    int index = 1;
    for (const auto& pair : store) {
        result += std::to_string(index) + ") \"" + pair.first + "\"\n";
        index++;
    }
    return result;
}

// CLEAR handler - Also needs WAL logging
std::string ThreadSafeKVStore::handleClear() {
    
    // For FLUSHDB, we need to log deletion of each key
    // In a production system, you might optimize this with a single "CLEAR" WAL entry
    std::vector<std::string> keys_to_delete;
    
    // First, collect all keys
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        keys_to_delete.reserve(store.size());
        for (const auto& pair : store) {
            keys_to_delete.push_back(pair.first);
        }
    }
    
    // Log deletion of each key
    for (const auto& key : keys_to_delete) {
        wal.logDelete(key);
    }
    
    // Then clear the in-memory store
    {
        std::lock_guard<std::mutex> lock(storeMutex);
        store.clear();
    }
    
    return "OK\n";
}

// INFO handler - Enhanced with WAL information
std::string ThreadSafeKVStore::handleInfo() {
    std::lock_guard<std::mutex> lock(storeMutex);
    auto now = std::time(nullptr);
    std::string timeStr = std::ctime(&now);
    if (!timeStr.empty() && timeStr.back() == '\n') {
        timeStr.pop_back();
    }
    
    return "# Hariya KV Server (WAL-Enabled)\n"
           "server_version:1.1.0\n"
           "wal_enabled:yes\n"
           "uptime_in_seconds:3600\n"
           "# Keyspace\n"
           "db0:keys=" + std::to_string(store.size()) + ",expires=0,avg_ttl=0\n"
           "# WAL Stats\n"
           "wal_entries:" + std::to_string(wal.getEntryCount()) + "\n"
           "# Time\n"
           "server_time:" + timeStr + "\n";
}

// NEW: Print detailed statistics including WAL
void ThreadSafeKVStore::printDetailedStats() const {
    std::lock_guard<std::mutex> lock(storeMutex);
    
    std::cout << "=== HARIYA KV STORE STATISTICS ===" << std::endl;
    std::cout << "📊 In-memory entries: " << store.size() << std::endl;
    
    if (!store.empty()) {
        std::cout << "🗂️  Current data preview:" << std::endl;
        int count = 0;
        for (const auto& [key, value] : store) {
            if (count >= 5) {  // Show only first 5 entries
                std::cout << "   ... and " << (store.size() - 5) << " more" << std::endl;
                break;
            }
            std::cout << "   " << key << " -> " << value << std::endl;
            count++;
        }
    }
    
    wal.printStats();
    std::cout << "==================================" << std::endl;
}

// NEW: Force WAL sync to disk
void ThreadSafeKVStore::sync() {
    wal.flush();
    std::cout << "💾 WAL synced to disk" << std::endl;
}

std::vector<std::string> ThreadSafeKVStore::getAllKeys() const {
    std::lock_guard<std::mutex> lock(storeMutex);
    std::vector<std::string> keys;
    keys.reserve(store.size());
    
    for (const auto& pair : store) {
        keys.push_back(pair.first);
    }
    
    return keys;
}