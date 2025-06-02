// ========================================
// CONNECTION MANAGER - Header File
// Tracks active connections and command statistics
// ========================================

#ifndef CONNECTION_MANAGER_H
#define CONNECTION_MANAGER_H

#include <atomic>
#include <mutex>
#include <chrono>

class ConnectionManager {
private:
    std::atomic<int> activeConnections{0};     // Current number of active connections
    std::atomic<uint64_t> totalConnections{0}; // Total connections since server start
    std::atomic<uint64_t> totalCommands{0};    // Total commands processed
    
    mutable std::mutex statsMutex;             // Protects detailed statistics
    std::chrono::steady_clock::time_point serverStartTime;
    
public:
    ConnectionManager();
    
    // Connection lifecycle management
    void newConnection();         // Called when a new client connects
    void closeConnection();       // Called when a client disconnects
    
    // Command tracking
    void commandExecuted();       // Called each time a command is processed
    
    // Statistics getters
    int getActiveConnections() const;
    uint64_t getTotalConnections() const;
    uint64_t getTotalCommands() const;
    
    // Get server uptime in seconds
    uint64_t getUptimeSeconds() const;
    
    // Print detailed connection statistics
    void printStats() const;
    
    // Reset all statistics (mainly for testing)
    void reset();
};

#endif // CONNECTION_MANAGER_H