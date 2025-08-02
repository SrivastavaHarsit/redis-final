// ========================================
// WRITE-AHEAD LOG SYSTEM - Implementation
// ========================================

#include "wal_system.h"
#include <sstream>
#include <iostream>
#include <filesystem>
#include <vector>

/// Constructor for a Write-Ahead Log (WAL) entry.
// Each entry represents a single operation (PUT, DELETE, REPLICATION, etc.) in the WAL system.
// This constructor initializes all fields needed to describe the operation and its context.
WALEntry::WALEntry(WALOperationType op_type, const std::string& k, const std::string& v,
                   const std::string& node_id, const std::string& target_node)
    // Set the type of operation (e.g., PUT, DELETE, REPLICATION, CLUSTER_JOIN, etc.)
    : type(op_type)
    // The key involved in the operation (could be empty for some cluster ops)
    , key(k)
    // The value associated with the key (empty for DELETE or some cluster ops)
    , value(v)
    // Record the current timestamp (in microseconds) when the entry is created.
    // This is used for ordering and recovery.
    , timestamp(getCurrentTimestamp())
    // The ID of the node performing this operation (for auditing and replication)
    , nodeId(node_id)
    // The target node for replication/migration (empty if not applicable)
    , targetNodeId(target_node)
    // The cluster version at the time of this operation (set to 0 by default, can be updated later)
    , clusterVersion(0)
    // Indicates if this entry is a result of replication (false by default)
    , isReplicated(false)
{}

// Get current timestamp in microseconds
uint64_t WALEntry::getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}


// Convert WALEntry to string format for disk storage
// Serializes the WALEntry object into a single string for storage in the WAL file.
// The format is a pipe-separated list of fields: operation type, timestamp, node IDs, cluster version,
// replication flag, key length and value, and value length and value. This allows for efficient parsing
// and recovery of WAL entries during replay or replication.
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
/*
 * Deserializes a string from the WAL file back into a WALEntry object.
 *
 * Flow:
 * 1. Checks if the input line is empty. If so, returns std::nullopt (no entry).
 * 2. Creates a string stream to parse the line, splitting fields by the '|' delimiter.
 * 3. Parses each field in order:
 *    a. Operation type (as integer, then cast to WALOperationType).
 *    b. Timestamp (when the operation occurred).
 *    c. nodeId (the node that performed the operation).
 *    d. targetNodeId (the target node for replication/migration, if any).
 *    e. clusterVersion (the cluster state version at the time).
 *    f. isReplicated (flag: "1" for true, "0" for false).
 *    g. key length (ensures correct parsing of the key).
 *    h. key (the actual key string, must match the specified length).
 *    i. value length (ensures correct parsing of the value).
 *    j. value (the actual value string, must match the specified length; may be empty).
 * 4. If any field is missing, malformed, or does not match the expected length, returns std::nullopt.
 * 5. Constructs a WALEntry object using the parsed fields.
 * 6. Sets additional fields (timestamp, clusterVersion, isReplicated) on the entry.
 * 7. Returns the reconstructed WALEntry object.
 * 8. If any exception occurs during parsing, logs the error and returns std::nullopt.
 *
 * This function is used during WAL replay and recovery to reconstruct the sequence of operations
 * that occurred in the system, ensuring data durability and consistency after crashes or restarts.
 */
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

    //Create parent directories if needed:
    // std::filesystem::path path(file_path);
    // // if (path.has_parent_path()) {   std::filesystem::create_directories(path.parent_path()); }
    // // This ensures that the directory for the WAL file exists. If     it doesn’t, it creates all necessary parent directories.

    // Open the WAL file for appending:
    // wal_file.open(wal_file_path, std::ios::app);
    // // Opens the WAL file in append mode, so new entries are added     to the end of the file.

    // Check if the file opened successfully:
    // if (!wal_file.is_open()) { throw std::runtime_error("Failed to open WAL file: " + wal_file_path); }
    // If the file cannot be opened, it throws a runtime error, stopping the program and reporting the problem.
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
/*
 * Writes a WALEntry to the Write-Ahead Log (WAL) file in a thread-safe manner.
 *
 * Flow:
 * 1. Acquires a lock to ensure only one thread writes to the WAL at a time.
 * 2. Checks if the entry is a duplicate of the last written entry (by comparing timestamp, key, and value).
 *    - If it is a duplicate, skips writing to avoid redundant log entries.
 * 3. Updates the static variables to remember the last written entry's timestamp, key, and value.
 * 4. Serializes the WALEntry to a string format suitable for disk storage.
 * 5. Writes the serialized entry to the WAL file and flushes the file to ensure durability.
 * 6. Increments the entry count for statistics.
 * 7. Prints a log message to the console indicating the operation (PUT or DELETE) and the key/value.
 *
 * This function ensures that all WAL writes are atomic, avoids duplicate entries,
 * and guarantees that every operation is safely persisted for recovery.
 */
void WriteAheadLog::writeEntry(const WALEntry& entry) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    
    // Check if this is a duplicate entry
    static uint64_t lastTimestamp = 0;
    static std::string lastKey;
    static std::string lastValue;
    
    if (entry.timestamp == lastTimestamp && 
        entry.key == lastKey && 
        entry.value == lastValue) {
        return; // Skip duplicate entry
    }
    
    // Update last entry info
    lastTimestamp = entry.timestamp;
    lastKey = entry.key;
    lastValue = entry.value;
    
    // Write to WAL
    std::string serialized = entry.serialize();
    wal_file << serialized;
    wal_file.flush();
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