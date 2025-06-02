// ========================================
// WRITE-AHEAD LOG SYSTEM - Implementation
// ========================================

#include "wal_system.h"
#include <sstream>
#include <iostream>
#include <filesystem>

// WALEntry Constructor
WALEntry::WALEntry(WALOperationType op_type, const std::string& k, const std::string& v)
    : type(op_type), key(k), value(v), timestamp(getCurrentTimestamp()) {}

// Get current timestamp in microseconds
uint64_t WALEntry::getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// Convert WALEntry to string format for disk storage
// Format: "type|timestamp|key_length|key|value_length|value\n"
std::string WALEntry::serialize() const {
    std::ostringstream oss;
    oss << static_cast<int>(type) << "|"           // Operation type (1=PUT, 2=DELETE)
        << timestamp << "|"                        // When it happened
        << key.length() << "|" << key << "|"       // Key with its length
        << value.length() << "|" << value << "\n"; // Value with its length
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
        
        WALEntry entry(static_cast<WALOperationType>(type_int), key, value);
        entry.timestamp = timestamp;
        return entry;
        
    } catch (const std::exception& e) {
        std::cerr << "Error parsing WAL entry: " << e.what() << std::endl;
        return std::nullopt;
    }
}

// WAL Constructor
WriteAheadLog::WriteAheadLog(const std::string& file_path) 
    : wal_file_path(file_path), entry_count(0) {
    
    // Create directory if it doesn't exist
    std::filesystem::path path(file_path);
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    
    // Open WAL file in append mode (so we don't lose existing data)
    wal_file.open(wal_file_path, std::ios::app);
    if (!wal_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + wal_file_path);
    }
    
    std::cout << "📝 WAL system initialized: " << wal_file_path << std::endl;
}

// WAL Destructor
WriteAheadLog::~WriteAheadLog() {
    if (wal_file.is_open()) {
        wal_file.flush(); // Ensure all data is written
        wal_file.close();
    }
}

// Log a PUT operation
void WriteAheadLog::logPut(const std::string& key, const std::string& value) {
    WALEntry entry(WALOperationType::PUT, key, value);
    writeEntry(entry);
}

// Log a DELETE operation
void WriteAheadLog::logDelete(const std::string& key) {
    WALEntry entry(WALOperationType::DELETE, key, "");
    writeEntry(entry);
}

// Force all buffered data to disk
void WriteAheadLog::flush() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    wal_file.flush();
    // In production systems, you might also call fsync() here for extra safety
}

// The MAGIC method: Replay WAL to reconstruct key-value store
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
    
    // Read each line and apply the operation
    while (std::getline(replay_file, line)) {
        auto entry = WALEntry::deserialize(line);
        if (!entry) {
            std::cerr << "⚠️  Failed to parse WAL entry: " << line << std::endl;
            continue;
        }
        
        // Apply the operation to our in-memory store
        switch (entry->type) {
            case WALOperationType::PUT:
                recovered_store[entry->key] = entry->value;
                puts++;
                break;
                
            case WALOperationType::DELETE:
                recovered_store.erase(entry->key);
                deletes++;
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
    
    // Get file size
    std::filesystem::path path(wal_file_path);
    size_t file_size = 0;
    if (std::filesystem::exists(path)) {
        file_size = std::filesystem::file_size(path);
    }
    
    std::cout << "📈 WAL Statistics:" << std::endl;
    std::cout << "   📁 File: " << wal_file_path << std::endl;
    std::cout << "   📝 Entries written: " << entry_count << std::endl;
    std::cout << "   💾 File size: " << file_size << " bytes" << std::endl;
}

// Get entry count
uint64_t WriteAheadLog::getEntryCount() const {
    std::lock_guard<std::mutex> lock(wal_mutex);
    return entry_count;
}

// Internal method to write an entry to disk (THREAD-SAFE)
void WriteAheadLog::writeEntry(const WALEntry& entry) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    
    // Convert entry to string and write to file
    std::string serialized = entry.serialize();
    wal_file << serialized;
    wal_file.flush(); // CRITICAL: Ensure data reaches disk immediately
    
    entry_count++;
    
    // Debug output (you can remove this in production)
    std::cout << "📝 WAL: " << (entry.type == WALOperationType::PUT ? "PUT" : "DELETE") 
              << " " << entry.key;
    if (entry.type == WALOperationType::PUT) {
        std::cout << " = " << entry.value;
    }
    std::cout << std::endl;
}