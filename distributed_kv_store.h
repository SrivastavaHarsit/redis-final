#ifndef DISTRIBUTED_KV_STORE_H
#define DISTRIBUTED_KV_STORE_H

#include "kv_store.h"
#include "cluster_manager.h"
#include <memory>
#include <string>

class DistributedKVStore : public std::enable_shared_from_this<DistributedKVStore> {
private:
    std::unique_ptr<ThreadSafeKVStore> localStore_;  // Local KV store instance
    std::unique_ptr<ClusterManager> clusterManager_; // Manages the hash ring
    std::string nodeId_;                             // Unique ID for this node
    std::string hostname_;                           // Hostname of this node
    int port_;                                       // Port this node listens on
    ClusterConfig config_;  // Add this

public:
    DistributedKVStore(const std::string& nodeId, 
                       const std::string& hostname, 
                       int port,
                       const std::string& walPath);
    
    // Process commands in a cluster-aware manner
    std::string processCommand(const std::string& command);
    
    // Cluster management methods
    bool joinCluster(const std::vector<std::string>& seedNodes = {});
    void addNode(const std::string& nodeId, const std::string& host, int port);
    void removeNode(const std::string& nodeId);
    
    // Utility methods
    void printStats() const;
    void sync();
    void redistributeKeys(); 
    void initializeClusterManager();
    void notifyJoin(const std::string& newNodeId, const std::string& host, int port);

    std::string readWithConsistency(const std::string& key, 
                                  ClusterConfig::ConsistencyLevel level);
    
    std::string writeWithConsistency(const std::string& key,
                                   const std::string& value,
                                   ClusterConfig::ConsistencyLevel level);

private:
    // Check if a key belongs to this node
    bool isLocalKey(const std::string& key);
    
    // Route operations to the appropriate node
    std::string routeOperation(const std::string& command, 
                              const std::string& key, 
                              const std::string& value = "");
    
    // Handle operations that don’t require routing
    std::string handleLocalOperation(const std::string& command);
    
    // Handle cluster-specific commands (e.g., CLUSTER INFO)
    std::string handleClusterCommand(const std::string& command);
};

#endif // DISTRIBUTED_KV_STORE_H