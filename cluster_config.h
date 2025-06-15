#ifndef CLUSTER_CONFIG_H
#define CLUSTER_CONFIG_H

#include <string>
#include <vector>

struct ClusterConfig {
    // Bootstrap node configuration  
    std::string bootstrap_host = "127.0.0.1";
    int bootstrap_port = 6379;
    
    // Cluster configuration
    int virtual_nodes = 150;
    
    // Node configuration
    std::vector<std::string> seed_nodes;
    std::string node_id;
    std::string hostname = "127.0.0.1";
    int port = 6379;
    
    // WAL configuration
    std::string wal_path = "data/hariya.wal";
    
    // Timeout settings
    int heartbeat_interval_ms = 5000;
    int connection_timeout_ms = 3000;

    // Cassandra-like configuration
    enum class ReplicationStrategy {
        SIMPLE,        // Copy data to N nodes
        NETWORK_TOPOLOGY    // Copy data to N nodes in different racks/DCs
    };

    enum class ConsistencyLevel {
        ONE,           // At least one replica must respond
        QUORUM,        // Majority of replicas must respond
        ALL,           // All replicas must respond
        LOCAL_ONE,     // One replica in local datacenter
        LOCAL_QUORUM   // Majority in local datacenter
    };

    // Replication settings
    ReplicationStrategy strategy = ReplicationStrategy::SIMPLE;
    int replication_factor = 2;    // Store each key on 3 nodes
    std::string default_dc = "dc1";    

    // Consistency settings
    ConsistencyLevel read_consistency = ConsistencyLevel::ONE;
    ConsistencyLevel write_consistency = ConsistencyLevel::ONE; // Changed to ONE for testing
};

#endif // CLUSTER_CONFIG_H