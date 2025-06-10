#ifndef HASH_RING_H
#define HASH_RING_H

#include <map>
#include <vector>
#include <string>
#include <memory>
#include <unordered_set>
#include <functional>

// Forward declaration
class ClusterNode;

/**
 * ConsistentHashRing - Implements consistent hashing for distributed cache
 * 
 * Key Features:
 * - Virtual nodes to improve load distribution
 * - Configurable hash function (SHA-1, Murmur3)
 * - Minimal data movement on node add/remove
 * - O(log N) node lookup using std::map
 */

class ClusterNode {
private:
    std::string nodeId_;
    std::string hostname_;
    int port_;
    bool isHealthy_;
    uint64_t lastHeartbeat_;
    
    // Node metadata
    struct NodeMetadata {
        size_t memoryCapacity = 0;
        size_t memoryUsed = 0;
        size_t connectionCount = 0;
        double cpuUsage = 0.0;
        std::string version;
    } metadata_;

public:
    ClusterNode(const std::string& id, const std::string& host, int port);
    
    // Getters
    const std::string& getId() const;
    const std::string& getHostname() const;
    int getPort() const;
    bool isHealthy() const;
    uint64_t getLastHeartbeat() const;
    
    // Health management
    void markHealthy();
    void markUnhealthy();
    void updateHeartbeat(uint64_t timestamp);
    
    // Metadata management
    void setMemoryCapacity(size_t capacity);
    void setMemoryUsed(size_t used);
    void setConnectionCount(size_t count);
    void setCpuUsage(double usage);
    void setVersion(const std::string& version);
    
    size_t getMemoryCapacity() const;
    size_t getMemoryUsed() const;
    size_t getConnectionCount() const;
    double getCpuUsage() const;
    const std::string& getVersion() const;
    
    // Load factor (0.0 to 1.0)
    double getLoadFactor() const;
    
    // Utility
    std::string toString() const;
    
    // Comparison operators for use in containers
    bool operator==(const ClusterNode& other) const;
    bool operator<(const ClusterNode& other) const;
};

// Custom hash and equality functors for NodePtr in unordered containers
struct NodePtrHash {
    size_t operator()(const std::shared_ptr<ClusterNode>& node) const {
        return std::hash<std::string>()(node->getId());
    }
};
struct NodePtrEqual {
    bool operator()(const std::shared_ptr<ClusterNode>& a, 
                   const std::shared_ptr<ClusterNode>& b) const {
        return a->getId() == b->getId();
    }
};


class ConsistentHashRing {
public:
    using HashValue = uint64_t;
    using NodePtr = std::shared_ptr<ClusterNode>;
    using HashFunction = std::function<HashValue(const std::string&)>;
    
    // Hash ring entry - maps hash value to node
    struct RingEntry {
        HashValue hash;
        NodePtr node;
        int virtualNodeId;  // Which virtual node (0 to virtualNodes-1)
        
        RingEntry(HashValue h, NodePtr n, int vid);
    };
    
    // Load distribution analysis
    struct LoadDistribution {
        std::map<std::string, double> nodeLoads;  // nodeId -> percentage
        double maxLoad = 0.0;
        double minLoad = 100.0;
        double stdDev = 0.0;
    };
    
private:
    // The ring: sorted map of hash -> node
    std::map<HashValue, RingEntry> ring_;
    
    // All nodes in the cluster (using custom hash/equality by node ID)
    std::unordered_set<NodePtr, NodePtrHash, NodePtrEqual> nodes_;
    
    // Configuration
    int virtualNodesPerNode_;
    HashFunction hashFunc_;
    
    // Statistics
    mutable size_t lookupCount_ = 0;
    mutable size_t redistributionCount_ = 0;

public:
    explicit ConsistentHashRing(int virtualNodes = 150, HashFunction hasher = nullptr);
    
    // Node management
    bool addNode(NodePtr node);
    bool removeNode(NodePtr node);
    
    // Key lookup
    NodePtr getNode(const std::string& key) const;
    std::vector<NodePtr> getNodes(const std::string& key, int count = 1) const;
    
    // Analysis
    std::vector<std::string> getAffectedKeys(const std::vector<std::string>& allKeys, 
                                           NodePtr oldNode, NodePtr newNode) const;
    
    // Statistics and monitoring
    size_t getNodeCount() const;
    size_t getVirtualNodeCount() const;
    size_t getLookupCount() const;
    size_t getRedistributionCount() const;
    
    LoadDistribution analyzeLoadDistribution(const std::vector<std::string>& testKeys) const;
    
    // Debug
    void printRing() const;
};

/**
 * ClusterNode - Represents a single node in the distributed cache cluster
 */

#endif // HASH_RING_H