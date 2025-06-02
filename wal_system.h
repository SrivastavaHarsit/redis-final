// ========================================
// WRITE-AHEAD LOG SYSTEM - Header File
// ========================================

#ifndef WAL_SYSTEM_H
#define WAL_SYSTEM_H

#include <string>
#include <fstream>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <chrono>

// Types of operations we can log
enum class WALOperationType {
    PUT = 1,
    DELETE = 2
};

// Structure representing a single WAL entry
struct WALEntry {
    WALOperationType type;     // PUT or DELETE
    std::string key;          // The key being operated on
    std::string value;        // Value (empty for DELETE)
    uint64_t timestamp;       // When the operation happened
    
    // Constructor for creating new entries
    WALEntry(WALOperationType op_type, const std::string& k, const std::string& v = "");
    
    // Convert entry to string format for disk storage
    std::string serialize() const;
    
    // Convert string back to WALEntry (returns empty if parsing fails)
    static std::optional<WALEntry> deserialize(const std::string& line);
    
private:
    static uint64_t getCurrentTimestamp();
};

// The main WAL class that handles logging and recovery
class WriteAheadLog {
private:
    std::string wal_file_path;        // Path to WAL file on disk
    std::ofstream wal_file;           // File stream for writing
    mutable std::mutex wal_mutex;     // Thread safety
    uint64_t entry_count;             // Statistics counter
    
public:
    // Constructor: opens WAL file for writing
    explicit WriteAheadLog(const std::string& file_path);
    
    // Destructor: ensures file is properly closed
    ~WriteAheadLog();
    
    // Log a PUT operation (key-value pair)
    void logPut(const std::string& key, const std::string& value);
    
    // Log a DELETE operation (key only)
    void logDelete(const std::string& key);
    
    // Force all buffered data to disk (important for crash safety)
    void flush();
    
    // Replay all WAL entries to reconstruct the key-value store
    // This is called during server startup
    std::unordered_map<std::string, std::string> replay() const;
    
    // Get statistics about the WAL
    void printStats() const;
    
    // Get the number of entries written
    uint64_t getEntryCount() const;
    
private:
    // Internal method to write an entry to disk
    void writeEntry(const WALEntry& entry);
};

#endif // WAL_SYSTEM_H