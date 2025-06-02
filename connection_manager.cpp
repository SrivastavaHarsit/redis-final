// ========================================
// CONNECTION MANAGER - Implementation
// Manages client connections and tracks server statistics
// ========================================

#include "connection_manager.h"
#include <iostream>
#include <iomanip>

ConnectionManager::ConnectionManager() 
    : serverStartTime(std::chrono::steady_clock::now()) {
    std::cout << "📊 Connection manager initialized" << std::endl;
}

// Called when a new client connects to the server
void ConnectionManager::newConnection() {
    activeConnections.fetch_add(1);          // Atomic increment of active connections
    totalConnections.fetch_add(1);           // Atomic increment of total connections
    
    std::cout << "🔗 New connection established (Active: " 
              << activeConnections.load() << ", Total: " 
              << totalConnections.load() << ")" << std::endl;
}

// Called when a client disconnects from the server
void ConnectionManager::closeConnection() {
    int currentActive = activeConnections.fetch_sub(1); // Atomic decrement
    
    std::cout << "🔌 Connection closed (Active: " 
              << (currentActive - 1) << ")" << std::endl;
}

// Called each time a command is successfully processed
void ConnectionManager::commandExecuted() {
    totalCommands.fetch_add(1);  // Atomic increment of command counter
}

// Get current number of active connections
int ConnectionManager::getActiveConnections() const {
    return activeConnections.load();
}

// Get total number of connections since server start
uint64_t ConnectionManager::getTotalConnections() const {
    return totalConnections.load();
}

// Get total number of commands processed since server start
uint64_t ConnectionManager::getTotalCommands() const {
    return totalCommands.load();
}

// Calculate server uptime in seconds
uint64_t ConnectionManager::getUptimeSeconds() const {
    auto now = std::chrono::steady_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - serverStartTime);
    return uptime.count();
}

// Print detailed statistics about server connections and performance
void ConnectionManager::printStats() const {
    std::lock_guard<std::mutex> lock(statsMutex);
    
    uint64_t uptime = getUptimeSeconds();
    uint64_t totalConns = getTotalConnections();
    uint64_t totalCmds = getTotalCommands();
    int activeConns = getActiveConnections();
    
    std::cout << "=== CONNECTION MANAGER STATISTICS ===" << std::endl;
    std::cout << "🔗 Active connections: " << activeConns << std::endl;
    std::cout << "📈 Total connections: " << totalConns << std::endl;
    std::cout << "⚡ Total commands: " << totalCmds << std::endl;
    std::cout << "⏰ Uptime: " << uptime << " seconds" << std::endl;
    
    // Calculate rates (avoid division by zero)
    if (uptime > 0) {
        double connRate = static_cast<double>(totalConns) / uptime;
        double cmdRate = static_cast<double>(totalCmds) / uptime;
        
        std::cout << "📊 Connection rate: " << std::fixed << std::setprecision(2) 
                  << connRate << " conn/sec" << std::endl;
        std::cout << "⚡ Command rate: " << std::fixed << std::setprecision(2) 
                  << cmdRate << " cmd/sec" << std::endl;
    }
    
    // Calculate average commands per connection
    if (totalConns > 0) {
        double avgCmdsPerConn = static_cast<double>(totalCmds) / totalConns;
        std::cout << "🎯 Avg commands/connection: " << std::fixed << std::setprecision(2) 
                  << avgCmdsPerConn << std::endl;
    }
    
    std::cout << "====================================" << std::endl;
}

// Reset all statistics (useful for testing or server restart)
void ConnectionManager::reset() {
    std::lock_guard<std::mutex> lock(statsMutex);
    
    activeConnections.store(0);
    totalConnections.store(0);
    totalCommands.store(0);
    serverStartTime = std::chrono::steady_clock::now();
    
    std::cout << "🔄 Connection manager statistics reset" << std::endl;
}