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
#include <vector>

// Types of operations we can log
enum class WALOperationType {
    PUT = 1,
    DELETE = 2,
    CLUSTER_JOIN = 3,  // Node joined cluster
    CLUSTER_LEAVE = 4, // Node left cluster
    REPLICATION = 5,   // Data replicated to another node
    MIGRATION = 6      // Key migrated to another node
};

// Structure representing a single WAL entry
struct WALEntry {
    WALOperationType type;     // Operation type
    std::string key;           // The key being operated on
    std::string value;         // Value (empty for DELETE)
    uint64_t timestamp;        // When the operation happened
    std::string nodeId;        // Node performing the operation
    std::string targetNodeId;  // Target node for replication/migration
    uint64_t clusterVersion;   // Cluster state version
    bool isReplicated;         // Is this a replicated operation?
    
    // Constructor for creating new entries
    WALEntry(WALOperationType op_type, const std::string& k, const std::string& v = "",
             const std::string& node_id = "", const std::string& target_node = "");
    
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
    std::string nodeId_;              // Node ID for cluster operations
    uint64_t clusterVersion_;         // Cluster version for consistency
    
public:
    // Constructor: opens WAL file for writing
    explicit WriteAheadLog(const std::string& file_path, const std::string& node_id = "");
    
    // Destructor: ensures file is properly closed
    ~WriteAheadLog();
    
    // Log a PUT operation (key-value pair)
    void logPut(const std::string& key, const std::string& value);
    
    // Log a DELETE operation (key only)
    void logDelete(const std::string& key);
    
    // Force all buffered data to disk (important for crash safety)
    void flush();
    
    // Replay all WAL entries to reconstruct the key-value store
    std::unordered_map<std::string, std::string> replay() const;
    
    // Get statistics about the WAL
    void printStats() const;
    
    // Get the number of entries written
    uint64_t getEntryCount() const;
    
    // Cluster-aware methods
    void logReplication(const std::string& key, const std::string& value, 
                        const std::string& target_node);
    void logMigration(const std::string& key, const std::string& value,
                      const std::string& target_node);
    void logClusterJoin(const std::string& new_node_id);
    void logClusterLeave(const std::string& leaving_node_id);
    
    void setNodeId(const std::string& node_id);
    void setClusterVersion(uint64_t version);
    
    std::vector<WALEntry> getEntriesSince(uint64_t timestamp) const;
    
private:
    // Internal method to write an entry to disk
    void writeEntry(const WALEntry& entry);
    WALEntry createEntry(WALOperationType type, const std::string& key, 
                         const std::string& value, const std::string& target = "");
};

#endif // WAL_SYSTEM_H