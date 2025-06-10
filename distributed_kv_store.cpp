#include "distributed_kv_store.h"
#include <sstream>
#include <algorithm>
#include <iostream>

DistributedKVStore::DistributedKVStore(const std::string& nodeId, 
                                       const std::string& hostname, 
                                       int port,
                                       const std::string& walPath)
    : nodeId_(nodeId), hostname_(hostname), port_(port) {
    // Initialize the local store with WAL
    localStore_ = std::make_unique<ThreadSafeKVStore>(walPath);
    
    // Initialize the cluster manager
    clusterManager_ = std::make_unique<ClusterManager>();
    
    // Register this node in the cluster
    clusterManager_->addNode(nodeId_, hostname_, port_);
    
    std::cout << "🚀 Distributed KV Store initialized: " << nodeId_ 
              << "@" << hostname_ << ":" << port_ << std::endl;
}

std::string DistributedKVStore::processCommand(const std::string& command) {
    std::istringstream iss(command);
    std::string cmd;
    iss >> cmd;
    
    // Standardize command to uppercase
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    
    // Handle cluster-specific commands
    if (cmd == "CLUSTER") {
        return handleClusterCommand(command);
    }
    
    // Parse and route key-value commands
    if (cmd == "GET") {
        std::string key;
        if (iss >> key) {
            return routeOperation("GET", key);
        }
        return "ERROR: GET requires a key\n";
    }
    else if (cmd == "SET" || cmd == "PUT") {
        std::string key, value;
        if (iss >> key) {
            std::getline(iss, value);
            value.erase(0, value.find_first_not_of(" \t"));
            return routeOperation("SET", key, value);
        }
        return "ERROR: SET requires a key and value\n";
    }
    else if (cmd == "DEL" || cmd == "DELETE") {
        std::string key;
        if (iss >> key) {
            return routeOperation("DEL", key);
        }
        return "ERROR: DEL requires a key\n";
    }
    else {
        // Handle non-key commands locally (e.g., STATS)
        return handleLocalOperation(command);
    }
}

bool DistributedKVStore::isLocalKey(const std::string& key) {
    auto targetNode = clusterManager_->getNodeForKey(key);
    return targetNode && targetNode->getId() == nodeId_;
}

std::string DistributedKVStore::routeOperation(const std::string& command, 
                                               const std::string& key, 
                                               const std::string& value) {
    if (isLocalKey(key)) {
        // Construct the full command and process locally
        std::string fullCommand = command + " " + key;
        if (!value.empty()) {
            fullCommand += " " + value;
        }
        std::cout << "🏠 LOCAL: " << fullCommand << std::endl;
        return localStore_->processCommand(fullCommand);
    } else {
        // Route to the correct node
        auto targetNode = clusterManager_->getNodeForKey(key);
        if (!targetNode) {
            return "ERROR: No available nodes\n";
        }
        std::cout << "📡 ROUTE: " << command << " " << key 
                  << " -> " << targetNode->getId() << std::endl;
        // Placeholder: In a real system, you’d make a network call here
        return "ROUTED to " + targetNode->getId() + "@" + 
               targetNode->getHostname() + ":" + std::to_string(targetNode->getPort()) + "\n";
    }
}

std::string DistributedKVStore::handleLocalOperation(const std::string& command) {
    return localStore_->processCommand(command);
}

std::string DistributedKVStore::handleClusterCommand(const std::string& command) {
    std::istringstream iss(command);
    std::string cluster, subcommand;
    iss >> cluster >> subcommand;
    
    std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(), ::toupper);
    
    if (subcommand == "INFO") {
        auto stats = clusterManager_->getClusterStats();
        return "cluster_state:ok\n"
               "cluster_nodes:" + std::to_string(stats.totalNodes) + "\n"
               "cluster_healthy_nodes:" + std::to_string(stats.healthyNodes) + "\n"
               "cluster_virtual_nodes:" + std::to_string(stats.virtualNodes) + "\n"
               "cluster_requests:" + std::to_string(stats.totalRequests) + "\n";
    }
    else if (subcommand == "NODES") {
        return "Node listing not implemented yet\n";
    }
    else {
        return "ERROR: Unknown cluster command\n";
    }
}

bool DistributedKVStore::joinCluster(const std::vector<std::string>& seedNodes) {
    std::cout << "🤝 Joining cluster with " << seedNodes.size() << " seed nodes\n";
    return true; // Placeholder for actual cluster join logic
}

void DistributedKVStore::addNode(const std::string& nodeId, const std::string& host, int port) {
    clusterManager_->addNode(nodeId, host, port);
    std::cout << "➕ Added node: " << nodeId << "@" << host << ":" << port << std::endl;
}

void DistributedKVStore::removeNode(const std::string& nodeId) {
    clusterManager_->removeNode(nodeId);
    std::cout << "➖ Removed node: " << nodeId << std::endl;
}

void DistributedKVStore::printStats() const {
    localStore_->printDetailedStats();
    clusterManager_->printClusterStatus();
}

void DistributedKVStore::sync() {
    localStore_->sync();
}