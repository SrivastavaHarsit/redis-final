#include "cluster_manager.h"
#include "distributed_kv_store.h"
#include <iomanip>
#include <set>

// Node management
bool ClusterManager::addNode(const std::string& nodeId, const std::string& hostname, int port) {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    
    // Check if node already exists
    for (const auto& node : nodes_) {
        if (node->getId() == nodeId) {
            return false;  // Node already exists
        }
    }
    
    auto node = std::make_shared<ClusterNode>(nodeId, hostname, port);
    nodes_.push_back(node);
    
    bool added = hashRing_.addNode(node);
    if (added) {
        redistributions_++;
        std::cout << "Added node to cluster: " << node->toString() << std::endl;
        printClusterStatus();
        // Trigger key redistribution
        // Call redistributeKeys through KVStore if available
        if (auto store = kvStore_.lock()) {
            store->redistributeKeys();
        }
    }
    return added;
}

bool ClusterManager::removeNode(const std::string& nodeId) {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    
    auto it = std::find_if(nodes_.begin(), nodes_.end(),
        [&nodeId](const std::shared_ptr<ClusterNode>& node) {
            return node->getId() == nodeId;
        });
    
    if (it == nodes_.end()) {
        return false;  // Node not found
    }
    
    auto node = *it;
    nodes_.erase(it);
    
    bool removed = hashRing_.removeNode(node);
    if (removed) {
        redistributions_++;
        std::cout << "Removed node from cluster: " << node->toString() << std::endl;
        printClusterStatus();
    }
    
    return removed;
}

// Key routing
std::shared_ptr<ClusterNode> ClusterManager::getNodeForKey(const std::string& key) {
    totalRequests_++;
    return hashRing_.getNode(key);
}

std::vector<std::shared_ptr<ClusterNode>> ClusterManager::getReplicaNodes(const std::string& key, int replicationFactor) {
    totalRequests_++;
    return hashRing_.getNodes(key, replicationFactor);
}

ClusterManager::OperationResult ClusterManager::routeGet(const std::string& key) {
    OperationResult result;
    result.targetNode = getNodeForKey(key);
    
    if (!result.targetNode) {
        result.errorMessage = "No available nodes";
        failedRequests_++;
        return result;
    }
    
    if (!result.targetNode->isHealthy()) {
        // Try to find a healthy replica
        auto replicas = getReplicaNodes(key, 3);
        for (auto replica : replicas) {
            if (replica->isHealthy()) {
                result.targetNode = replica;
                break;
            }
        }
        
        if (!result.targetNode || !result.targetNode->isHealthy()) {
            result.errorMessage = "No healthy nodes available for key";
            failedRequests_++;
            return result;
        }
    }
    
    // Here you would make the actual network call to the node
    // For now, simulate the operation
    result.success = true;
    result.response = "GET operation routed to " + result.targetNode->getId();
    
    return result;
}

ClusterManager::OperationResult ClusterManager::routeSet(const std::string& key, const std::string& value, int replicationFactor) {
    OperationResult result;
    auto replicas = getReplicaNodes(key, replicationFactor);
    
    if (replicas.empty()) {
        result.errorMessage = "No available nodes";
        failedRequests_++;
        return result;
    }
    
    // Filter out unhealthy nodes
    std::vector<std::shared_ptr<ClusterNode>> healthyReplicas;
    for (auto replica : replicas) {
        if (replica->isHealthy()) {
            healthyReplicas.push_back(replica);
        }
    }
    
    if (healthyReplicas.empty()) {
        result.errorMessage = "No healthy nodes available";
        failedRequests_++;
        return result;
    }
    
    result.targetNode = healthyReplicas[0];  // Primary node
    result.replicasSucceeded = healthyReplicas.size();
    
    // Here you would:
    // 1. Write to primary node
    // 2. Asynchronously replicate to other healthy replicas
    // For now, simulate the operation
    result.success = true;
    result.response = "SET operation routed to " + std::to_string(healthyReplicas.size()) + " nodes";
    
    return result;
}

// Health monitoring
void ClusterManager::startHealthMonitoring() {
    healthMonitorThread_ = std::thread([this]() {
        while (!stopHealthMonitor_) {
            checkNodeHealth();
            std::this_thread::sleep_for(std::chrono::seconds(5));  // Check every 5 seconds
        }
    });
}

void ClusterManager::checkNodeHealth() {
    // std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    // auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     std::chrono::system_clock::now().time_since_epoch()).count();
    
    // for (auto& node : nodes_) {
    //     // Simulate health check (in practice, you'd ping the node)
    //     uint64_t timeSinceHeartbeat = now - node->getLastHeartbeat();
        
    //     if (timeSinceHeartbeat > 30000) {  // 30 seconds timeout
    //         if (node->isHealthy()) {
    //             node->markUnhealthy();
    //             std::cout << "⚠ Node marked unhealthy: " << node->getId() << std::endl;
    //         }
    //     } else {
    //         if (!node->isHealthy()) {
    //             node->markHealthy();
    //             std::cout << "✓ Node recovered: " << node->getId() << std::endl;
    //         }
    //     }
    // }

    // Above is being commented out for development purposes.

    // Disable health checks - all nodes stay healthy
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    for (auto& node : nodes_) {
        if (!node->isHealthy()) {
            node->markHealthy();
            std::cout << "✓ Node marked healthy: " << node->getId() << std::endl;
        }
    }

}

// Update node heartbeat (called when receiving responses from nodes)
void ClusterManager::updateNodeHeartbeat(const std::string& nodeId) {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (auto& node : nodes_) {
        if (node->getId() == nodeId) {
            node->updateHeartbeat(now);
            break;
        }
    }
}

// Check if a node is healthy
bool ClusterManager::isNodeHealthy(const std::string& nodeId) {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    
    for (const auto& node : nodes_) {
        if (node->getId() == nodeId) {
            return node->isHealthy();
        }
    }
    
    return false;  // Node not found
}

// Cluster status and monitoring
void ClusterManager::printClusterStatus() {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - startTime_).count();
    
    std::cout << "\n=== Cluster Status ===" << std::endl;
    std::cout << "Uptime: " << uptime << " seconds" << std::endl;
    std::cout << "Total nodes: " << nodes_.size() << std::endl;
    std::cout << "Virtual nodes: " << hashRing_.getVirtualNodeCount() << std::endl;
    std::cout << "Total requests: " << totalRequests_.load() << std::endl;
    std::cout << "Failed requests: " << failedRequests_.load() << std::endl;
    std::cout << "Redistributions: " << redistributions_.load() << std::endl;
    
    int healthyNodes = 0;
    for (const auto& node : nodes_) {
        if (node->isHealthy()) healthyNodes++;
    }
    
    std::cout << "Healthy nodes: " << healthyNodes << "/" << nodes_.size() << std::endl;
    
    // Show load distribution if we have test data
    std::vector<std::string> sampleKeys;
    for (int i = 0; i < 1000; ++i) {
        sampleKeys.push_back("sample_key_" + std::to_string(i));
    }
    
    if (!nodes_.empty()) {
        auto distribution = hashRing_.analyzeLoadDistribution(sampleKeys);
        std::cout << "Load distribution (sample):" << std::endl;
        for (const auto& pair : distribution.nodeLoads) {
            std::cout << "  " << pair.first << ": " << std::fixed << std::setprecision(1) 
                     << pair.second << "%" << std::endl;
        }
        std::cout << "Standard deviation: " << std::fixed << std::setprecision(2) 
                 << distribution.stdDev << std::endl;
    }
    
    std::cout << "==================\n" << std::endl;
}

// Get cluster statistics
ClusterManager::ClusterStats ClusterManager::getClusterStats() {
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    ClusterStats stats;
    
    stats.totalNodes = nodes_.size();
    stats.virtualNodes = hashRing_.getVirtualNodeCount();
    stats.totalRequests = totalRequests_.load();
    stats.failedRequests = failedRequests_.load();
    stats.redistributions = redistributions_.load();
    stats.uptimeSeconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - startTime_).count();
    
    for (const auto& node : nodes_) {
        if (node->isHealthy()) {
            stats.healthyNodes++;
        }
    }
    
    // Get load distribution
    std::vector<std::string> sampleKeys;
    for (int i = 0; i < 1000; ++i) {
        sampleKeys.push_back("sample_key_" + std::to_string(i));
    }
    
    if (!nodes_.empty()) {
        auto distribution = hashRing_.analyzeLoadDistribution(sampleKeys);
        stats.loadDistribution = distribution.nodeLoads;
    }
    
    return stats;
}

// Simulate data migration (for when nodes are added/removed)
std::vector<ClusterManager::MigrationPlan> ClusterManager::planDataMigration(const std::vector<std::string>& allKeys) {
    std::vector<MigrationPlan> plans;
    
    // This is a simplified version - in practice, we need to:
    // 1. Compare current key->node mapping with desired mapping
    // 2. Identify keys that need to move
    // 3. Group by source->target pairs
    // 4. Estimate data sizes
    // 5. Prioritize migrations
    
    std::cout << "Planning data migration for " << allKeys.size() << " keys..." << std::endl;
    
    // For demonstration, just showing which nodes would handle which keys
    std::map<std::string, std::vector<std::string>> nodeKeyMap;
    
    for (const auto& key : allKeys) {
        auto node = getNodeForKey(key);
        if (node) {
            nodeKeyMap[node->getId()].push_back(key);
        }
    }
    
    std::cout << "Current key distribution:" << std::endl;
    for (const auto& pair : nodeKeyMap) {
        std::cout << "  " << pair.first << ": " << pair.second.size() << " keys" << std::endl;
    }
    
    // 2. (In a full system, we’d also get the “desired” mapping after ring changes 
    //    and compare old vs. new. Then compute exactly which keys to move from A→B.)
    //    For this demo, we just show a toy example: pick the first two nodes and say 
    //    we’ll migrate “some keys” from node 0 to node 1.


    // Create a sample migration plan for demonstration
    if (nodeKeyMap.size() >= 2) {
        MigrationPlan plan;
        auto it = nodeKeyMap.begin();
        plan.sourceNodeId = it->first;
        ++it;
        plan.targetNodeId = it->first;
        plan.loadDifference = 15.0; // Sample load difference
        plan.keysToMigrate.push_back("sample_key_migration");
        plans.push_back(plan);
    }
    
    return plans;
}



// Demo implementation
void ClusterDemo::runDemo() {
    std::cout << "=== Distributed Cache Cluster Demo ===" << std::endl;
    
    ClusterManager cluster(100);  // 100 virtual nodes per physical node
    
    // Add nodes to the cluster
    cluster.addNode("cache-1", "192.168.1.101", 6379);
    cluster.addNode("cache-2", "192.168.1.102", 6379);
    cluster.addNode("cache-3", "192.168.1.103", 6379);
    
    // Simulate some operations
    std::vector<std::string> testKeys = {
        "user:1001:profile",
        "session:abc123",
        "product:item456",
        "cache:user:1001:friends",
        "temp:calculation_result_789"
    };
    
    std::cout << "\n=== Routing Operations ===" << std::endl;
    
    // Test GET operations
    for (const auto& key : testKeys) {
        auto result = cluster.routeGet(key);
        if (result.success) {
            std::cout << "GET " << key << " -> " << result.targetNode->getId() 
                     << " (" << result.targetNode->getHostname() << ":" 
                     << result.targetNode->getPort() << ")" << std::endl;
        } else {
            std::cout << "GET " << key << " -> ERROR: " << result.errorMessage << std::endl;
        }
    }
    
    std::cout << std::endl;
    
    // Test SET operations with replication
    for (const auto& key : testKeys) {
        auto result = cluster.routeSet(key, "value_for_" + key, 2);
        if (result.success) {
            std::cout << "SET " << key << " -> " << result.response << std::endl;
        } else {
            std::cout << "SET " << key << " -> ERROR: " << result.errorMessage << std::endl;
        }
    }
    
    // Show cluster status
    cluster.printClusterStatus();
    
    // Simulate node failure
    std::cout << "=== Simulating Node Failure ===" << std::endl;
    cluster.removeNode("cache-2");
    
    // Test operations after failure
    std::cout << "Operations after node failure:" << std::endl;
    for (const auto& key : testKeys) {
        auto result = cluster.routeGet(key);
        if (result.success) {
            std::cout << "GET " << key << " -> " << result.targetNode->getId() << std::endl;
        }
    }
    
    // Add a new node
    std::cout << "\n=== Adding New Node ===" << std::endl;
    cluster.addNode("cache-4", "192.168.1.104", 6379);
    
    // Plan data migration
    cluster.planDataMigration(testKeys);
    
    // Final status
    cluster.printClusterStatus();
    
    std::cout << "Demo completed!" << std::endl;
}