#include "hash_ring.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <chrono>

// ConsistentHashRing::RingEntry implementation
ConsistentHashRing::RingEntry::RingEntry(HashValue h, NodePtr n, int vid) 
    : hash(h), node(n), virtualNodeId(vid) {
}

// ConsistentHashRing implementation
ConsistentHashRing::ConsistentHashRing(int virtualNodes, HashFunction hasher)
    : virtualNodesPerNode_(virtualNodes) {
    
    if (hasher) {
        hashFunc_ = hasher;
    } else {
        // Default: Simple hash function (replace with SHA-1 or Murmur3 in production)
        hashFunc_ = [](const std::string& key) -> HashValue {
            std::hash<std::string> hasher;
            return hasher(key);
        };
    }
}

bool ConsistentHashRing::addNode(NodePtr node) {
    if (!node || nodes_.count(node)) {
        return false;  // Node already exists or invalid
    }
    
    nodes_.insert(node);
    
    // Add virtual nodes to the ring
    for (int i = 0; i < virtualNodesPerNode_; ++i) {
        std::string virtualKey = node->getId() + ":" + std::to_string(i);
        HashValue hash = hashFunc_(virtualKey);
        
        ring_.emplace(hash, RingEntry(hash, node, i));
    }
    
    redistributionCount_++;
    return true;
}


// What Happens: All virtual nodes belonging to the failed physical node are removed. Keys that were pointing to those virtual nodes will now point to the next clockwise virtual node (which belongs to a different physical node).
bool ConsistentHashRing::removeNode(NodePtr node) {
    if (!node || !nodes_.count(node)) {
        return false;  // Node doesn't exist
    }
    
    nodes_.erase(node);
    
    // Remove all virtual nodes from the ring
    auto it = ring_.begin();
    while (it != ring_.end()) {
        if (it->second.node == node) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }
    
    redistributionCount_++;
    return true;
}

ConsistentHashRing::NodePtr ConsistentHashRing::getNode(const std::string& key) const {
    if (ring_.empty()) {
        return nullptr;
    }
    
    lookupCount_++;
    
    HashValue keyHash = hashFunc_(key);
    
    // Find the first node with hash >= keyHash
    auto it = ring_.lower_bound(keyHash);
    
    // If no node found, wrap around to the beginning (ring topology)
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    
    return it->second.node;
}



// Real distributed caches need multiple copies of each key for fault tolerance:
// Returns up to 'count' unique physical nodes responsible for a given key.
// This is used for replication: the first node is the primary, the rest are backups.
// It walks clockwise around the ring, skipping duplicate physical nodes (i.e., only one virtual node per physical node is counted).
std::vector<ConsistentHashRing::NodePtr> ConsistentHashRing::getNodes(const std::string& key, int count) const {
    std::vector<NodePtr> result;
    if (ring_.empty() || count <= 0) {
        return result;
    }
    
    lookupCount_++;
    
    HashValue keyHash = hashFunc_(key);
    auto it = ring_.lower_bound(keyHash);
    
    // If no node found, start from the beginning
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    
    std::unordered_set<NodePtr> uniqueNodes;
    auto startIt = it;
    
    // Collect unique nodes (skip virtual nodes of same physical node)
    while (uniqueNodes.size() < static_cast<size_t>(count) && uniqueNodes.size() < nodes_.size()) {
        if (uniqueNodes.find(it->second.node) == uniqueNodes.end()) {
            uniqueNodes.insert(it->second.node);
            result.push_back(it->second.node);
        }
        
        ++it;
        if (it == ring_.end()) {
            it = ring_.begin();  // Wrap around
        }
        
        // Prevent infinite loop
        if (it == startIt && uniqueNodes.empty()) {
            break;
        }
    }
    
    return result;
}


// This function determines which keys from a given list (allKeys) would be affected if a node in a consistent hash ring is replaced (or added/removed). It returns a vector of the keys whose ownership might change due to the presence or absence of oldNode or newNode.
std::vector<std::string> ConsistentHashRing::getAffectedKeys(const std::vector<std::string>& allKeys, 
                                       NodePtr oldNode, NodePtr newNode) const {
    std::vector<std::string> affected;
    
    for (const auto& key : allKeys) {
        NodePtr currentOwner = getNode(key);
        
        // Simulate the change and see if ownership changes
        // This is a simplified version - in practice, you'd need more complex logic
        if ((oldNode && currentOwner == oldNode) || 
            (newNode && currentOwner == newNode)) {
            affected.push_back(key);
        }
    }
    
    return affected;
}

size_t ConsistentHashRing::getNodeCount() const { 
    return nodes_.size(); 
}

size_t ConsistentHashRing::getVirtualNodeCount() const { 
    return ring_.size(); 
}

size_t ConsistentHashRing::getLookupCount() const { 
    return lookupCount_; 
}

size_t ConsistentHashRing::getRedistributionCount() const { 
    return redistributionCount_; 
}

ConsistentHashRing::LoadDistribution ConsistentHashRing::analyzeLoadDistribution(const std::vector<std::string>& testKeys) const {
    LoadDistribution dist;
    std::map<NodePtr, int> nodeCounts;
    
    // Count keys per node
    for (const auto& key : testKeys) {
        NodePtr node = getNode(key);
        if (node) {
            nodeCounts[node]++;
        }
    }
    
    if (testKeys.empty() || nodeCounts.empty()) {
        return dist;
    }
    
    // Calculate percentages
    double totalKeys = static_cast<double>(testKeys.size());
    std::vector<double> loads;
    
    for (const auto& pair : nodeCounts) {
        double percentage = (pair.second / totalKeys) * 100.0;
        dist.nodeLoads[pair.first->getId()] = percentage;
        loads.push_back(percentage);
        
        dist.maxLoad = std::max(dist.maxLoad, percentage);
        dist.minLoad = std::min(dist.minLoad, percentage);
    }
    
    // Calculate standard deviation
    if (loads.size() > 1) {
        double mean = 100.0 / nodes_.size();  // Expected load per node
        double variance = 0.0;
        
        for (double load : loads) {
            variance += (load - mean) * (load - mean);
        }
        
        dist.stdDev = std::sqrt(variance / loads.size());
    }
    
    return dist;
}

void ConsistentHashRing::printRing() const {
    std::cout << "=== Hash Ring State ===" << std::endl;
    std::cout << "Physical nodes: " << nodes_.size() << std::endl;
    std::cout << "Virtual nodes: " << ring_.size() << std::endl;
    std::cout << "Virtual nodes per physical: " << virtualNodesPerNode_ << std::endl;
    
    std::cout << "\nRing entries (first 10):" << std::endl;
    int count = 0;
    for (const auto& pair : ring_) {
        if (count++ >= 10) break;
        
        std::cout << "Hash: " << std::hex << pair.first 
                 << " -> Node: " << pair.second.node->getId()
                 << " (Virtual: " << pair.second.virtualNodeId << ")" << std::endl;
    }
    
    std::cout << std::dec;  // Reset to decimal
}



// ClusterNode implementation
ClusterNode::ClusterNode(const std::string& id, const std::string& host, int port)
    : nodeId_(id), hostname_(host), port_(port), isHealthy_(true), lastHeartbeat_(0) {
    // Initialize lastHeartbeat_ to current time
    // lastHeartbeat_ = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     std::chrono::system_clock::now().time_since_epoch()).count();

    // Initialize as healthy and stay that way
    markHealthy();
}

const std::string& ClusterNode::getId() const { 
    return nodeId_; 
}

const std::string& ClusterNode::getHostname() const { 
    return hostname_; 
}

int ClusterNode::getPort() const { 
    return port_; 
}

bool ClusterNode::isHealthy() const { 
    // return isHealthy_; 
    return true;  // Disable health checks - all nodes stay healthy
}

uint64_t ClusterNode::getLastHeartbeat() const { 
    return lastHeartbeat_; 
}

void ClusterNode::markHealthy() { 
    // isHealthy_ = true;
    // For development purposes, we disable health checks 
}

void ClusterNode::markUnhealthy() { 
    isHealthy_ = false; 
}

void ClusterNode::updateHeartbeat(uint64_t timestamp) { 
    lastHeartbeat_ = timestamp; 
}

void ClusterNode::setMemoryCapacity(size_t capacity) { 
    metadata_.memoryCapacity = capacity; 
}

void ClusterNode::setMemoryUsed(size_t used) { 
    metadata_.memoryUsed = used; 
}

void ClusterNode::setConnectionCount(size_t count) { 
    metadata_.connectionCount = count; 
}

void ClusterNode::setCpuUsage(double usage) { 
    metadata_.cpuUsage = usage; 
}

void ClusterNode::setVersion(const std::string& version) { 
    metadata_.version = version; 
}

size_t ClusterNode::getMemoryCapacity() const { 
    return metadata_.memoryCapacity; 
}

size_t ClusterNode::getMemoryUsed() const { 
    return metadata_.memoryUsed; 
}

size_t ClusterNode::getConnectionCount() const { 
    return metadata_.connectionCount; 
}

double ClusterNode::getCpuUsage() const { 
    return metadata_.cpuUsage; 
}

const std::string& ClusterNode::getVersion() const { 
    return metadata_.version; 
}

double ClusterNode::getLoadFactor() const {
    if (metadata_.memoryCapacity == 0) return 0.0;
    return static_cast<double>(metadata_.memoryUsed) / metadata_.memoryCapacity;
}

std::string ClusterNode::toString() const {
    std::ostringstream oss;
    oss << nodeId_ << "@" << hostname_ << ":" << port_ 
        << " (healthy: " << (isHealthy_ ? "yes" : "no") << ")";
    return oss.str();
}

bool ClusterNode::operator==(const ClusterNode& other) const {
    return nodeId_ == other.nodeId_;
}

bool ClusterNode::operator<(const ClusterNode& other) const {
    return nodeId_ < other.nodeId_;
}