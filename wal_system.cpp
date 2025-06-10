// ========================================
// WRITE-AHEAD LOG SYSTEM - Implementation
// ========================================

#include "wal_system.h"
#include <sstream>
#include <iostream>
#include <filesystem>
#include <vector>

// WALEntry Constructor
WALEntry::WALEntry(WALOperationType op_type, const std::string& k, const std::string& v,
                   const std::string& node_id, const std::string& target_node)
    : type(op_type), key(k), value(v), timestamp(getCurrentTimestamp()),
      nodeId(node_id), targetNodeId(target_node), clusterVersion(0), isReplicated(false) {}

// Get current timestamp in microseconds
uint64_t WALEntry::getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// Convert WALEntry to string format for disk storage
std::string WALEntry::serialize() const {
    std::ostringstream oss;
    oss << static_cast<int>(type) << "|"
        << timestamp << "|"
        << nodeId << "|"
        << targetNodeId << "|"
        << clusterVersion << "|"
        << (isReplicated ? "1" : "0") << "|"
        << key.length() << "|" << key << "|"
        << value.length() << "|" << value << "\n";
    return oss.str();
}

// Convert string back to WALEntry
std::optional<WALEntry> WALEntry::deserialize(const std::string& line) {
    if (line.empty()) return std::nullopt;
    
    std::istringstream iss(line);
    std::string token;
    
    try {
        // Parse operation type
        if (!std::getline(iss, token, '|')) return std::nullopt;
        int type_int = std::stoi(token);
        
        // Parse timestamp
        if (!std::getline(iss, token, '|')) return std::nullopt;
        uint64_t timestamp = std::stoull(token);
        
        // Parse nodeId
        if (!std::getline(iss, token, '|')) return std::nullopt;
        std::string nodeId = token;
        
        // Parse targetNodeId
        if (!std::getline(iss, token, '|')) return std::nullopt;
        std::string targetNodeId = token;
        
        // Parse clusterVersion
        if (!std::getline(iss, token, '|')) return std::nullopt;
        uint64_t clusterVersion = std::stoull(token);
        
        // Parse isReplicated
        if (!std::getline(iss, token, '|')) return std::nullopt;
        bool isReplicated = (token == "1");
        
        // Parse key length and key
        if (!std::getline(iss, token, '|')) return std::nullopt;
        size_t key_len = std::stoul(token);
        
        if (!std::getline(iss, token, '|')) return std::nullopt;
        if (token.length() != key_len) return std::nullopt;
        std::string key = token;
        
        // Parse value length and value
        if (!std::getline(iss, token, '|')) return std::nullopt;
        size_t value_len = std::stoul(token);
        
        std::string value;
        if (value_len > 0) {
            if (!std::getline(iss, value)) return std::nullopt;
            if (value.length() != value_len) return std::nullopt;
        }
        
        WALEntry entry(static_cast<WALOperationType>(type_int), key, value, nodeId, targetNodeId);
        entry.timestamp = timestamp;
        entry.clusterVersion = clusterVersion;
        entry.isReplicated = isReplicated;
        return entry;
        
    } catch (const std::exception& e) {
        std::cerr << "Error parsing WAL entry: " << e.what() << std::endl;
        return std::nullopt;
    }
}

// WAL Constructor
WriteAheadLog::WriteAheadLog(const std::string& file_path, const std::string& node_id) 
    : wal_file_path(file_path), entry_count(0), nodeId_(node_id), clusterVersion_(0) {
    std::filesystem::path path(file_path);
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    
    wal_file.open(wal_file_path, std::ios::app);
    if (!wal_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + wal_file_path);
    }
    
    std::cout << "📝 WAL system initialized for node: " << nodeId_ << std::endl;
}

// WAL Destructor
WriteAheadLog::~WriteAheadLog() {
    if (wal_file.is_open()) {
        wal_file.flush();
        wal_file.close();
    }
}

// Log a PUT operation
void WriteAheadLog::logPut(const std::string& key, const std::string& value) {
    WALEntry entry = createEntry(WALOperationType::PUT, key, value);
    writeEntry(entry);
}

// Log a DELETE operation
void WriteAheadLog::logDelete(const std::string& key) {
    WALEntry entry = createEntry(WALOperationType::DELETE, key, "");
    writeEntry(entry);
}

// Force all buffered data to disk
void WriteAheadLog::flush() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    wal_file.flush();
}

// Replay all WAL entries to reconstruct the key-value store
std::unordered_map<std::string, std::string> WriteAheadLog::replay() const {
    std::unordered_map<std::string, std::string> recovered_store;
    std::ifstream replay_file(wal_file_path);
    
    if (!replay_file.is_open()) {
        std::cout << "📂 No existing WAL file found, starting with empty store" << std::endl;
        return recovered_store;
    }
    
    std::string line;
    size_t entries_replayed = 0;
    size_t puts = 0, deletes = 0;
    
    std::cout << "🔄 Replaying WAL from " << wal_file_path << "..." << std::endl;
    
    while (std::getline(replay_file, line)) {
        auto entry = WALEntry::deserialize(line);
        if (!entry) {
            std::cerr << "⚠️  Failed to parse WAL entry: " << line << std::endl;
            continue;
        }
        
        switch (entry->type) {
            case WALOperationType::PUT:
                recovered_store[entry->key] = entry->value;
                puts++;
                break;
            case WALOperationType::DELETE:
                recovered_store.erase(entry->key);
                deletes++;
                break;
            // Other types (e.g., CLUSTER_JOIN) don't affect the key-value store directly
            default:
                break;
        }
        
        entries_replayed++;
    }
    
    replay_file.close();
    
    std::cout << "✅ WAL replay complete:" << std::endl;
    std::cout << "   📊 Entries replayed: " << entries_replayed << std::endl;
    std::cout << "   ➕ PUT operations: " << puts << std::endl;
    std::cout << "   ➖ DELETE operations: " << deletes << std::endl;
    std::cout << "   🗂️  Final store size: " << recovered_store.size() << std::endl;
    
    return recovered_store;
}

// Print WAL statistics
void WriteAheadLog::printStats() const {
    std::lock_guard<std::mutex> lock(wal_mutex);
    std::filesystem::path path(wal_file_path);
    size_t file_size = std::filesystem::exists(path) ? std::filesystem::file_size(path) : 0;
    std::cout << "📈 WAL Statistics:\n"
              << "   📁 File: " << wal_file_path << "\n"
              << "   📝 Entries written: " << entry_count << "\n"
              << "   💾 File size: " << file_size << " bytes\n";
}

// Get entry count
uint64_t WriteAheadLog::getEntryCount() const {
    std::lock_guard<std::mutex> lock(wal_mutex);
    return entry_count;
}

// Internal method to write an entry to disk (THREAD-SAFE)
void WriteAheadLog::writeEntry(const WALEntry& entry) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    std::string serialized = entry.serialize();
    wal_file << serialized;
    wal_file.flush(); // Ensure data reaches disk immediately
    entry_count++;
    std::cout << "📝 WAL: " << (entry.type == WALOperationType::PUT ? "PUT" : "DELETE") 
              << " " << entry.key << (entry.type == WALOperationType::PUT ? " = " + entry.value : "") << "\n";
}

// Create a WAL entry with cluster metadata
WALEntry WriteAheadLog::createEntry(WALOperationType type, const std::string& key, 
                                    const std::string& value, const std::string& target) {
    WALEntry entry(type, key, value, nodeId_, target);
    entry.clusterVersion = clusterVersion_;
    return entry;
}

// Cluster-aware methods
void WriteAheadLog::logReplication(const std::string& key, const std::string& value, 
                                   const std::string& target_node) {
    WALEntry entry = createEntry(WALOperationType::REPLICATION, key, value, target_node);
    entry.isReplicated = true;
    writeEntry(entry);
}

void WriteAheadLog::logMigration(const std::string& key, const std::string& value,
                                 const std::string& target_node) {
    WALEntry entry = createEntry(WALOperationType::MIGRATION, key, value, target_node);
    writeEntry(entry);
}

void WriteAheadLog::logClusterJoin(const std::string& new_node_id) {
    WALEntry entry = createEntry(WALOperationType::CLUSTER_JOIN, new_node_id, "");
    writeEntry(entry);
}

void WriteAheadLog::logClusterLeave(const std::string& leaving_node_id) {
    WALEntry entry = createEntry(WALOperationType::CLUSTER_LEAVE, leaving_node_id, "");
    writeEntry(entry);
}

void WriteAheadLog::setNodeId(const std::string& node_id) {
    nodeId_ = node_id;
}

void WriteAheadLog::setClusterVersion(uint64_t version) {
    clusterVersion_ = version;
}

std::vector<WALEntry> WriteAheadLog::getEntriesSince(uint64_t timestamp) const {
    std::vector<WALEntry> entries;
    std::ifstream replay_file(wal_file_path);
    if (!replay_file.is_open()) return entries;
    
    std::string line;
    while (std::getline(replay_file, line)) {
        auto entry = WALEntry::deserialize(line);
        if (entry && entry->timestamp > timestamp) {
            entries.push_back(*entry);
        }
    }
    replay_file.close();
    return entries;
}