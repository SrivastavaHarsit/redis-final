#include "cluster_manager.h"
#include "cluster_config.h"
#include "distributed_kv_store.h"
#include <iomanip>
#include <set>

/*
 * Adds a new node to the cluster.
 *
 * Flow:
 * 1. Acquires a recursive lock to ensure thread-safe access to the cluster's node list.
 * 2. Checks if a node with the same nodeId already exists in the cluster.
 *    - If it exists, returns false (no duplicate nodes allowed).
 * 3. Creates a new ClusterNode object for the given nodeId, hostname, and port.
 * 4. Adds the new node to the cluster's node list.
 * 5. Adds the node to the consistent hash ring for key distribution.
 * 6. If the node was successfully added to the hash ring:
 *    a. Increments the redistributions counter (tracks ring changes).
 *    b. Prints a message and the updated cluster status.
 *    c. If a KV store is attached, triggers key redistribution so data is balanced across the new cluster.
 * 7. Returns true if the node was added, false otherwise.
 *
 * This function ensures that new nodes are safely integrated into the cluster,
 * the hash ring is updated, and data is redistributed for balanced load and fault tolerance.
 */
bool ClusterManager::addNode(const std::string& nodeId, const std::string& hostname, int port) {
    // Lock the nodesMutex_ to prevent other threads from modifying the node list at the same time.
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    
    // Loop through all existing nodes to check if a node with the same ID already exists.
    for (const auto& node : nodes_) {
        if (node->getId() == nodeId) {
            // If found, do not add a duplicate. Return false.
            return false;  // Node already exists
        }
    }
    
    // Create a new ClusterNode object (shared_ptr for memory safety and sharing).
    auto node = std::make_shared<ClusterNode>(nodeId, hostname, port);
    // Add the new node to the nodes_ vector (the cluster's list of nodes).
    nodes_.push_back(node);
    
    // Add the node to the consistent hash ring for key distribution.
    bool added = hashRing_.addNode(node);
    if (added) {
        // If the node was successfully added to the ring:
        redistributions_++; // Increment the count of redistributions (for stats/monitoring).
        std::cout << "Added node to cluster: " << node->toString() << std::endl;
        printClusterStatus(); // Print the updated cluster status to the console.
        // If a key-value store is attached, trigger key redistribution.
        if (auto store = kvStore_.lock()) {
            store->redistributeKeys(); // Move keys to their new correct nodes.
        }
    }
    // Return true if the node was added to the ring, false otherwise.
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

/*
 Not used only for demo this file standalone
 */
ClusterManager::OperationResult ClusterManager::routeGet(const std::string& key) {
    OperationResult result;
    // Find the primary node responsible for this key using the hash ring.
    result.targetNode = getNodeForKey(key);
    
    // If no node is available (e.g., cluster is empty), return an error.
    if (!result.targetNode) {
        result.errorMessage = "No available nodes";
        failedRequests_++;
        return result;
    }
    
    // If the chosen node is unhealthy, try to find a healthy replica.
    if (!result.targetNode->isHealthy()) {
        // Get all replicas for this key using the default replication factor (e.g., 2).
        auto replicas = getReplicaNodes(key, 2);
        for (auto replica : replicas) {
            if (replica->isHealthy()) {
                result.targetNode = replica;
                break;
            }
        }
        
        // If no healthy replica is found, return an error.
        if (!result.targetNode || !result.targetNode->isHealthy()) {
            result.errorMessage = "No healthy nodes available for key";
            failedRequests_++;
            return result;
        }
    }
    
    // (In a real system, here you would send the GET request over the network.)
    // For now, just simulate a successful operation.
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

// Currently, this function is disabled for development purposes.
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


/*
 * Updates the heartbeat timestamp for a specific node in the cluster.
 *
 * Flow:
 * 1. Acquires a recursive lock to ensure thread-safe access to the cluster's node list.
 * 2. Gets the current time in milliseconds since the epoch.
 * 3. Loops through all nodes in the cluster to find the node with the given nodeId.
 * 4. If the node is found, updates its last heartbeat timestamp to the current time.
 * 5. Exits the loop after updating the node.
 *
 * This function is typically called whenever a node sends a response or heartbeat,
 * helping the cluster manager track which nodes are alive and responsive.
 */
void ClusterManager::updateNodeHeartbeat(const std::string& nodeId) {
    // Lock the nodesMutex_ to prevent other threads from modifying the node list at the same time.
    std::lock_guard<std::recursive_mutex> lock(nodesMutex_);
    
    // Get the current time in milliseconds since the epoch.
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    // Loop through all nodes to find the one with the matching nodeId.
    for (auto& node : nodes_) {
        if (node->getId() == nodeId) {
            // Update the node's last heartbeat timestamp.
            node->updateHeartbeat(now);
            break; // Stop searching after updating the correct node.
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

/*
 * Plans data migration between nodes when the cluster changes (e.g., nodes are added or removed).
 *
 * Flow:
 * 1. Initializes an empty list of migration plans.
 * 2. (In a real system) Would compare the current key-to-node mapping with the desired mapping after a ring change,
 *    identify which keys need to move, group them by source and target nodes, estimate data sizes, and prioritize migrations.
 * 3. For demonstration, builds a map of which node currently owns which keys.
 * 4. Prints the current key distribution across nodes.
 * 5. (Demo only) If there are at least two nodes, creates a sample migration plan to move a sample key from the first node to the second.
 * 6. Returns the list of migration plans.
 *
 * This function helps visualize and plan how data would be moved to keep the cluster balanced and consistent
 * after topology changes, but the current implementation is only a simplified demo.
 */
std::vector<ClusterManager::MigrationPlan> ClusterManager::planDataMigration(const std::vector<std::string>& allKeys) {
    std::vector<MigrationPlan> plans; // List to hold migration plans
    
    // This is a simplified version - in practice, we need to:
    // 1. Compare current key->node mapping with desired mapping
    // 2. Identify keys that need to move
    // 3. Group by source->target pairs
    // 4. Estimate data sizes
    // 5. Prioritize migrations
    
    std::cout << "Planning data migration for " << allKeys.size() << " keys..." << std::endl;
    
    // Build a map of nodeId -> list of keys currently owned by that node
    std::map<std::string, std::vector<std::string>> nodeKeyMap;
    for (const auto& key : allKeys) {
        auto node = getNodeForKey(key); // Find which node currently owns this key
        if (node) {
            nodeKeyMap[node->getId()].push_back(key);
        }
    }
    
    // Print out the current key distribution for visibility
    std::cout << "Current key distribution:" << std::endl;
    for (const auto& pair : nodeKeyMap) {
        std::cout << "  " << pair.first << ": " << pair.second.size() << " keys" << std::endl;
    }
    
    // In a real system, here you would:
    // - Get the new mapping after a ring change
    // - Compare old and new mappings
    // - Figure out which keys need to move from which node to which node
    // - Create migration plans for those keys
    
    // For this demo, just create a sample migration plan if there are at least two nodes
    if (nodeKeyMap.size() >= 2) {
        MigrationPlan plan;
        auto it = nodeKeyMap.begin();
        plan.sourceNodeId = it->first; // First node
        ++it;
        plan.targetNodeId = it->first; // Second node
        plan.loadDifference = 15.0; // Sample load difference (not calculated)
        plan.keysToMigrate.push_back("sample_key_migration"); // Just a placeholder key
        plans.push_back(plan);
    }
    
    return plans; // Return the list of migration plans (real or demo)
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