// ========================================
// HARIYA KV SERVER - Header File (WAL-Enabled)
// ========================================

#ifndef HARIYA_SERVER_H
#define HARIYA_SERVER_H

#include <atomic>
#include <thread>
#include "distributed_kv_store.h"
#include "connection_manager.h"
#include "message_protocol.h"

class HariyaServer {
private:
    int serverSocket;
    int port;
    std::atomic<bool> running{true};
    std::unique_ptr<DistributedKVStore> distributedStore_;  // Will be initialized with WAL path
    ConnectionManager connectionManager;

    std::string nodeId_;  // Unique ID for this node
    std::string hostname_; // Hostname of this node
    
public:
    // NEW: Constructor can optionally take WAL file path
    explicit HariyaServer(int port, 
        const std::string& nodeId = "",
        const std::string& hostname = "127.0.0.1",
        const std::string& wal_path = "data/hariya.wal");
    ~HariyaServer();
    
    bool start();
    void run();
    void stop();
    int getActiveConnections() const;
    
    // NEW: Method to sync WAL to disk
    // void syncWAL();

    // Cluster management
    void joinCluster(const std::vector<std::string>& seedNodes = {});
    void addClusterNode(const std::string& nodeId, const std::string& host, int port);
    void removeClusterNode(const std::string& nodeId);
    void debugStatus();
    
private:
    void handleClient(int clientSocket);
    std::string generateNodeId();
};

#endif // HARIYA_SERVER_H