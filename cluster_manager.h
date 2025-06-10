#ifndef CLUSTER_MANAGER_H
#define CLUSTER_MANAGER_H

#include "hash_ring.h"
#include <thread>
#include <chrono>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <algorithm>
#include <iostream>
#include <map>
#include <vector>
#include <memory>
#include <string>

/**
 * ClusterManager - Manages distributed cache cluster operations
 *
 * Responsibilities:
 * - Node lifecycle management
 * - Data routing based on consistent hashing
 * - Failure detection and recovery
 * - Load balancing
 */
class ClusterManager {
private:
    ConsistentHashRing hashRing_; // The consistent-hash ring object
    std::vector<std::shared_ptr<ClusterNode>> nodes_; // All physical nodes in the cluster
    std::recursive_mutex nodesMutex_; // Protects access to `nodes_` and related data
    
    // Health monitoring
    std::thread healthMonitorThread_;
    std::atomic<bool> stopHealthMonitor_{false};
    
    // Statistics
    std::atomic<size_t> totalRequests_{0};
    std::atomic<size_t> failedRequests_{0};
    std::atomic<size_t> redistributions_{0}; // increments when ring changes
    std::chrono::steady_clock::time_point startTime_;

public:
    explicit ClusterManager(int virtualNodesPerNode = 150)
        : hashRing_(virtualNodesPerNode), startTime_(std::chrono::steady_clock::now()) {
        startHealthMonitoring();
    }
    
    ~ClusterManager() {
        stopHealthMonitor_ = true;
        if (healthMonitorThread_.joinable()) {
            healthMonitorThread_.join();
        }
    }
    
    // Node management (add/remove/get)
    bool addNode(const std::string& nodeId, const std::string& hostname, int port);
    bool removeNode(const std::string& nodeId);
    
    // Key routing
    std::shared_ptr<ClusterNode> getNodeForKey(const std::string& key);
    std::vector<std::shared_ptr<ClusterNode>> getReplicaNodes(const std::string& key, int replicationFactor = 3);
    
    // Cluster operations that integrate with your KV store
    struct OperationResult {
        bool success = false;
        std::string response;
        std::string errorMessage;
        std::shared_ptr<ClusterNode> targetNode;
        int replicasSucceeded = 0;
    };
    
    OperationResult routeGet(const std::string& key);
    OperationResult routeSet(const std::string& key, const std::string& value, int replicationFactor = 2);
    
    // Health monitoring
    void startHealthMonitoring();
    void checkNodeHealth();
    void updateNodeHeartbeat(const std::string& nodeId);
    bool isNodeHealthy(const std::string& nodeId);
    
    // Cluster status and monitoring
    void printClusterStatus();
    
    // Get cluster statistics
    struct ClusterStats {
        size_t totalNodes = 0;
        size_t healthyNodes = 0;
        size_t virtualNodes = 0;
        size_t totalRequests = 0;
        size_t failedRequests = 0;
        size_t redistributions = 0;
        double uptimeSeconds = 0.0;
        std::map<std::string, double> loadDistribution;
    };
    
    ClusterStats getClusterStats();
    
    // Simulate data migration (for when nodes are added/removed)
    struct MigrationPlan {
        std::string sourceNodeId;
        std::string targetNodeId;
        std::vector<std::string> keysToMigrate;
        size_t estimatedDataSize = 0;
        double loadDifference = 0.0;
    };
    
    std::vector<MigrationPlan> planDataMigration(const std::vector<std::string>& allKeys);
};

/**
 * Demo usage showing integration with existing KV store
 */
class ClusterDemo {
public:
    static void runDemo();
};

#endif // CLUSTER_MANAGER_H