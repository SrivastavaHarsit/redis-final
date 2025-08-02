#include "distributed_kv_store.h"
#include "cluster_config.h"
#include <sstream>
#include <algorithm>
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <future> 

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

    // Ensure the node stays healthy
    clusterManager_->updateNodeHeartbeat(nodeId_);

    std::cout << "🚀 Distributed KV Store initialized: " << nodeId_ 
              << "@" << hostname_ << ":" << port_ << std::endl;
}

// Adding this new method to set the KVStore after construction
void DistributedKVStore::initializeClusterManager() {
    if (clusterManager_) {
        clusterManager_->setKVStore(shared_from_this());
    }
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
            return readWithConsistency(key, config_.read_consistency);
        }
        return "ERROR: GET requires a key\n";
    }
    else if (cmd == "SET" || cmd == "PUT") {
        std::string key, value;
        if (iss >> key) {
            std::getline(iss, value);
            value.erase(0, value.find_first_not_of(" \t"));
            return writeWithConsistency(key, value, config_.write_consistency);
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

// The function DistributedKVStore::isLocalKey determines whether a given key is managed by the local node in a distributed key-value store cluster.
bool DistributedKVStore::isLocalKey(const std::string& key) {
    auto targetNode = clusterManager_->getNodeForKey(key);
    return targetNode && targetNode->getId() == nodeId_;
}




/*
 * Routes a key-value operation to the correct node in the cluster.
 * 
 * Flow:
 * 1. Checks if the key belongs to the local node using isLocalKey(key).
 *    - If yes: Builds the command string and processes it locally using localStore_->processCommand().
 *    - If no:  Proceeds to route the command to the correct remote node.
 * 
 * 2. For remote keys:
 *    a. Uses clusterManager_->getNodeForKey(key) to find the responsible node.
 *       - If no node is available, returns an error.
 *    b. Tries to get an existing TCP connection to the target node from the cluster manager.
 *       - If no connection exists, creates a new socket and connects to the target node.
 *       - If connection fails, returns an error.
 *       - If connection succeeds, stores the new connection in the cluster manager.
 *    c. Builds the full command string (including value if present) and sends it over the socket.
 *    d. Waits for a response from the remote node (using recv).
 *    e. If a new connection was created for this operation, closes the socket after use.
 *    f. Returns the response from the remote node.
 * 
 * This function ensures that every operation is executed on the correct node in the cluster,
 * either locally or by routing it over the network, and handles connection management for efficiency.
 */
std::string DistributedKVStore::routeOperation(const std::string& command, 
                                               const std::string& key, 
                                               const std::string& value) {
    if (isLocalKey(key)) {
        // Process locally as before
        std::string fullCommand = command + " " + key;
        if (!value.empty()) {
            fullCommand += " " + value;
        }
        std::cout << "🏠 LOCAL: " << fullCommand << std::endl;
        return localStore_->processCommand(fullCommand);
    } else {
        // Get target node
        auto targetNode = clusterManager_->getNodeForKey(key);
        if (!targetNode) {
            return "ERROR: No available nodes\n";
        }

        // Try to get existing connection from cluster manager
        int sock = clusterManager_->getNodeConnection(targetNode->getId());
        bool newConnection = false;

        if (sock == -1) {
            // Create new connection if none exists
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                return "ERROR: Failed to create socket for routing\n";
            }

            // Setup connection
            sockaddr_in targetAddr{};
            targetAddr.sin_family = AF_INET;
            targetAddr.sin_port = htons(targetNode->getPort());
            targetAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

            if (connect(sock, (struct sockaddr*)&targetAddr, sizeof(targetAddr)) < 0) {
                close(sock);
                return "ERROR: Failed to connect to target node\n";
            }

            // Store the new connection
            clusterManager_->storeNodeConnection(targetNode->getId(), sock);
            newConnection = true;
        }

        // Send command
        std::string fullCommand = command + " " + key;
        if (!value.empty()) {
            fullCommand += " " + value;
        }
        fullCommand += "\n";

        std::cout << "📡 ROUTE: " << command << " " << key 
                  << " -> " << targetNode->getId() << std::endl;

        send(sock, fullCommand.c_str(), fullCommand.length(), 0);

        // Read response
        char buffer[1024];
        std::string response;
        ssize_t bytesRead = recv(sock, buffer, sizeof(buffer)-1, 0);
        if (bytesRead > 0) {
            buffer[bytesRead] = '\0';
            response = buffer;
        }

        // Only close if it's a new connection and not needed anymore
        if (newConnection) {
            close(sock);
        }

        return response;
    }
}



// Handles operations that do not require routing, such as local commands
std::string DistributedKVStore::handleLocalOperation(const std::string& command) {
    return localStore_->processCommand(command);
}





/*
 * Handles cluster management commands sent to the distributed key-value store.
 * 
 * Flow:
 * 1. Parses the command to extract the cluster subcommand (e.g., INFO, JOIN, LEAVE, etc.).
 * 2. Converts the subcommand to uppercase for case-insensitive matching.
 * 3. Executes the appropriate action based on the subcommand:
 *    - INFO:      Returns cluster statistics (node count, health, requests, etc.).
 *    - JOIN:      Adds a new node to the cluster (expects nodeId, hostname, port).
 *    - LEAVE:     Removes a node from the cluster (expects nodeId).
 *    - REDISTRIBUTE: Triggers redistribution of keys across the cluster.
 *    - STATS:     Prints detailed cluster and storage statistics.
 *    - SYNC:      Forces a sync of the local store (e.g., flushes WAL to disk).
 *    - ADD:       Adds a node to the cluster (same as JOIN, expects nodeId, hostname, port).
 *    - REMOVE:    Removes a node from the cluster (expects nodeId).
 *    - NODES:     (Not implemented) Intended to list all nodes in the cluster.
 *    - HEARTBEAT: Updates the heartbeat timestamp for a node (expects nodeId).
 *    - Unknown:   Returns an error for unrecognized subcommands.
 * 4. For each command, validates the required arguments and returns an error if missing.
 * 5. Returns a response string indicating the result of the operation.
 * 
 * This function allows nodes and clients to manage cluster membership, monitor health,
 * trigger key redistribution, and perform other cluster-wide operations.
 */
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
    else if (subcommand == "JOIN") {
        // Format: CLUSTER JOIN nodeId hostname port
        std::string newNodeId, hostname;
        int port;
        if (!(iss >> newNodeId >> hostname >> port)) {
            return "ERROR: CLUSTER JOIN requires nodeId hostname port\n";
        }
        
        // Add the new node to our cluster
        addNode(newNodeId, hostname, port);
        return "OK Cluster joined\n";
    }
    else if (subcommand == "LEAVE") {
        // Format: CLUSTER LEAVE nodeId
        std::string nodeId;
        if (!(iss >> nodeId)) {
            return "ERROR: CLUSTER LEAVE requires nodeId\n";
        }
        
        // Remove the node from our cluster
        removeNode(nodeId);
        return "OK Cluster left\n";
    }
    else if (subcommand == "REDISTRIBUTE") {
        redistributeKeys();
        return "OK Keys redistributed\n";
    }
    else if (subcommand == "STATS") {
        printStats();
        return "OK Cluster stats printed\n";
    }
    else if (subcommand == "SYNC") {
        sync();
        return "OK Cluster synced\n";
    }
    else if (subcommand == "ADD") {
        // Format: CLUSTER ADD nodeId hostname port
        std::string newNodeId, hostname;
        int port;
        if (!(iss >> newNodeId >> hostname >> port)) {
            return "ERROR: CLUSTER ADD requires nodeId hostname port\n";
        }
        
        addNode(newNodeId, hostname, port);
        return "OK Node added to cluster\n";
    }
    else if (subcommand == "REMOVE") {
        // Format: CLUSTER REMOVE nodeId
        std::string nodeId;
        if (!(iss >> nodeId)) {
            return "ERROR: CLUSTER REMOVE requires nodeId\n";
        }
        
        removeNode(nodeId);
        return "OK Node removed from cluster\n";
    }
    else if (subcommand == "NODES") {
        return "Node listing not implemented yet\n";
    }

    else if (subcommand == "HEARTBEAT") {
        std::string fromNodeId;
        if (!(iss >> fromNodeId)) {
            return "ERROR: HEARTBEAT requires nodeId\n";
        }
        clusterManager_->updateNodeHeartbeat(fromNodeId);
        return "OK\n";  // Return OK for heartbeats
    }
    else {
        return "ERROR: Unknown cluster command\n";
    }
}



// Hardcoded joinCluster method
/*
 * Attempts to join an existing cluster using a list of seed nodes.
 * 
 * Flow:
 * 1. Iterates over each seed node (given as "host:port"):
 *    a. Splits the string to extract the host and port.
 *    b. Creates a TCP socket for connecting to the seed node.
 *    c. Tries to connect to the seed node using the socket.
 *       - If connection fails, logs an error and tries the next seed node.
 *    d. If connection succeeds:
 *       - Adds the seed node to the local cluster manager.
 *       - Sends a "CLUSTER JOIN" command to the seed node, announcing this node's presence.
 *       - Reads and prints the response from the seed node.
 *       - Stores the socket for persistent communication with the seed node.
 *       - Starts a background thread to send periodic "CLUSTER HEARTBEAT" messages to the seed node and monitor the connection.
 * 2. If at least one seed node was successfully joined, triggers key redistribution to balance data across the cluster.
 * 3. Returns true if joined to any seed node, false otherwise.
 * 
 * This function allows a new node to join an existing distributed cluster, establish persistent connections,
 * announce itself to other nodes, and participate in cluster-wide data management.
 */
bool DistributedKVStore::joinCluster(const std::vector<std::string>& seedNodes) {
    std::cout << "🤝 Joining cluster with " << seedNodes.size() << " seed nodes\n";
    bool joinedAny = false;
    
    for (const auto& node : seedNodes) {
        size_t colon = node.find(':');
        if (colon != std::string::npos) {
            std::string host = node.substr(0, colon);
            int port = std::stoi(node.substr(colon + 1));
            
            // Create persistent socket connection to seed node
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                std::cerr << "❌ Failed to create socket for node " << host << ":" << port << std::endl;
                continue;
            }

            // Connect to seed node
            sockaddr_in seedAddr{};
            seedAddr.sin_family = AF_INET;
            seedAddr.sin_port = htons(port);
            seedAddr.sin_addr.s_addr = inet_addr("127.0.0.1"); // Use localhost

            if (connect(sock, (struct sockaddr*)&seedAddr, sizeof(seedAddr)) < 0) {
                std::cerr << "❌ Connection failed to " << host << ":" << port 
                         << " (errno: " << errno << ")" << std::endl;
                close(sock);
                continue;
            }

            // Add the seed node to our cluster first
            std::string seedNodeId = "node_" + host + "_" + std::to_string(port);
            clusterManager_->addNode(seedNodeId, host, port);
            std::cout << "✅ Added seed node: " << seedNodeId << "@" << host << ":" << port << std::endl;
            joinedAny = true;

            // Send CLUSTER JOIN command
            std::string joinCmd = "CLUSTER JOIN " + nodeId_ + " " + hostname_ + " " + std::to_string(port_) + "\n";
            send(sock, joinCmd.c_str(), joinCmd.length(), 0);

            // Read response but keep connection open
            char buffer[1024];
            ssize_t bytesRead = recv(sock, buffer, sizeof(buffer)-1, 0);
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0';
                std::cout << "📥 Received from seed node: " << buffer;
            }

            // Store socket in cluster manager for persistent connection
            clusterManager_->storeNodeConnection(seedNodeId, sock);

            // Start heartbeat thread for this connection
            std::thread([this, sock, seedNodeId]() {
                char heartbeatBuf[128];
                while (true) {
                    std::string heartbeat = "CLUSTER HEARTBEAT " + nodeId_ + "\n";
                    send(sock, heartbeat.c_str(), heartbeat.length(), 0);
                    
                    ssize_t bytes = recv(sock, heartbeatBuf, sizeof(heartbeatBuf)-1, 0);
                    if (bytes <= 0) {
                        std::cerr << "❌ Lost connection to " << seedNodeId << std::endl;
                        break;
                    }
                    
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
            }).detach();
        }
    }
    
    if (joinedAny) {
        redistributeKeys();
    }
    
    return joinedAny;
}


// Adds a new node to the cluster and updates the cluster manager   
void DistributedKVStore::addNode(const std::string& nodeId, const std::string& host, int port) {
    clusterManager_->addNode(nodeId, host, port);
    std::cout << "➕ Added node: " << nodeId << "@" << host << ":" << port << std::endl;
}

void DistributedKVStore::removeNode(const std::string& nodeId) {
    clusterManager_->removeNode(nodeId);
    // Update heartbeat immediately after adding
    clusterManager_->updateNodeHeartbeat(nodeId);
    std::cout << "➖ Removed node: " << nodeId << std::endl;
}

void DistributedKVStore::printStats() const {
    localStore_->printDetailedStats();
    clusterManager_->printClusterStatus();
}

void DistributedKVStore::sync() {
    localStore_->sync();
}



/*
 * Redistributes all keys managed by the local node to their correct replica nodes in the cluster.
 * 
 * Flow:
 * 1. Retrieves all keys currently stored on the local node.
 * 2. For each key:
 *    a. Uses the cluster manager to determine which nodes should hold replicas of this key (based on the replication factor).
 *    b. Skips the key if no replica nodes are found.
 *    c. Gets the value for the key from the local store.
 *    d. Skips the key if the value is missing or nil.
 *    e. For each replica node (excluding self):
 *       i.   Creates a TCP connection to the replica node.
 *       ii.  Sends a SET command with the key and value to the replica node.
 *       iii. Waits for a response to confirm replication.
 *       iv.  Logs the replication and closes the connection.
 * 3. After all keys are processed, prints the total number of successful replications.
 * 
 * This function ensures that, after cluster membership changes (like node join/leave),
 * all keys are correctly replicated to the appropriate nodes, maintaining data consistency and redundancy.
 */
void DistributedKVStore::redistributeKeys() {
   std::cout << "🔄 Starting key redistribution..." << std::endl;
    
    std::vector<std::string> allKeys = localStore_->getAllKeys();
    std::cout << "📦 Found " << allKeys.size() << " keys to redistribute" << std::endl;

    int replicated = 0;
    
    for (const auto& key : allKeys) {
        // Get N nodes for replication
        auto replicaNodes = clusterManager_->getReplicaNodes(key, config_.replication_factor); // RF=3

        std::cout << "🎯 Key '" << key << "' should be on " 
                  << replicaNodes.size() << " nodes" << std::endl;
        
        if (replicaNodes.empty()) continue;

        std::string value = localStore_->processCommand("GET " + key);
        if (value == "(nil)\n") continue;

        // Replicate to each node
        for (const auto& node : replicaNodes) {
            if (node->getId() == nodeId_) continue; // Skip self

            // Create connection
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) continue;

            sockaddr_in nodeAddr{};
            nodeAddr.sin_family = AF_INET;
            nodeAddr.sin_port = htons(node->getPort());
            nodeAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

            if (connect(sock, (struct sockaddr*)&nodeAddr, sizeof(nodeAddr)) == 0) {
                // Send SET command
                std::string setCmd = "SET " + key + " " + value + "\n";
                send(sock, setCmd.c_str(), setCmd.length(), 0);
                
                char buffer[1024];
                ssize_t bytesRead = recv(sock, buffer, sizeof(buffer)-1, 0);
                if (bytesRead > 0) {
                    replicated++;
                    std::cout << "📦 Replicated key '" << key << "' to node: " 
                              << node->getId() << std::endl;
                }
            }
            close(sock);
        }
    }
    
    std::cout << "✅ Replication complete. Created " << replicated << " replicas" << std::endl;
}


void DistributedKVStore::notifyJoin(const std::string& newNodeId, const std::string& host, int port) {
    // Add new node to cluster
    addNode(newNodeId, host, port);
    
    // This will trigger key redistribution if needed
    redistributeKeys();
}


/*
 * Reads a key from the distributed key-value store with the specified consistency level.
 * 
 * Flow:
 * 1. Checks if the key is managed by the local node using isLocalKey(key).
 *    - If yes: Reads and returns the value directly from the local store.
 * 2. If not local, retrieves the list of replica nodes for the key from the cluster manager.
 *    - If no replica nodes are found, returns (nil).
 * 3. Attempts to read the value from the primary replica node (first in the list):
 *    a. Tries to reuse an existing TCP connection to the primary node, or creates a new one if needed.
 *    b. Sends a GET command for the key to the primary node.
 *    c. Receives the value and adds it to the results.
 *    d. Closes the connection if it was newly created.
 * 4. Determines how many responses are required based on the requested consistency level:
 *    - ONE:    Only one successful response is needed.
 *    - QUORUM: A majority of replicas must respond.
 *    - ALL:    All replicas must respond.
 * 5. If the number of responses is less than required, returns an error.
 * 6. Returns the value if enough responses were received, or (nil) if not found.
 * 
 * This function ensures that read operations respect the desired consistency guarantees
 * (ONE, QUORUM, or ALL) by contacting the appropriate number of replica nodes.
 */
std::string DistributedKVStore::readWithConsistency(
    const std::string& key, 
    ClusterConfig::ConsistencyLevel level) {
    
    // First check if we have it locally
    if (isLocalKey(key)) {
        std::string localValue = localStore_->processCommand("GET " + key);
        std::cout << "📍 Found key locally: " << localValue;
        return localValue;
    }

    auto replicaNodes = clusterManager_->getReplicaNodes(key, config_.replication_factor);
    if (replicaNodes.empty()) {
        return "(nil)\n";
    }


    std::vector<std::string> values;
    int responses = 0;

    // Try primary node first (first replica)
    // Picks the primary node (first in the replica list).
    // Tries to reuse an existing TCP connection to the primary node (for efficiency).
    // If no connection exists, will create a new one.
    auto primaryNode = replicaNodes[0];
    int sock = clusterManager_->getNodeConnection(primaryNode->getId());
    bool newConnection = false;

    // If no existing connection, create a new one
    //Creates a new TCP socket.
    //Sets up the address for the primary node.
    //Tries to connect to the primary node.
    //If connection fails, returns an error.
    //If successful, stores the connection for future use and marks it as new.
    if (sock == -1) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return "ERROR: Failed to create socket\n";
        }

        sockaddr_in nodeAddr{};
        nodeAddr.sin_family = AF_INET;
        nodeAddr.sin_port = htons(primaryNode->getPort());
        nodeAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

        if (connect(sock, (struct sockaddr*)&nodeAddr, sizeof(nodeAddr)) < 0) {
            close(sock);
            return "ERROR: Failed to connect to primary node\n";
        }

        clusterManager_->storeNodeConnection(primaryNode->getId(), sock);
        newConnection = true;
    }

    // Send GET command to primary
    std::string getCmd = "GET " + key + "\n";
    if (send(sock, getCmd.c_str(), getCmd.length(), 0) > 0) {
        char buffer[1024];
        ssize_t bytesRead = recv(sock, buffer, sizeof(buffer)-1, 0);
        if (bytesRead > 0) {
            buffer[bytesRead] = '\0';
            values.push_back(buffer);
            responses++;
            std::cout << "✅ Got value from primary node" << std::endl;
        }
    }

    if (newConnection) {
        close(sock);
        clusterManager_->removeNodeConnection(primaryNode->getId());
    }

    // Check if we need more responses for consistency
    int required = 1; // Default to ONE
    switch (level) {
        case ClusterConfig::ConsistencyLevel::QUORUM:
            required = (config_.replication_factor / 2) + 1;
            break;
        case ClusterConfig::ConsistencyLevel::ALL:
            required = config_.replication_factor;
            break;
        default:
            required = 1;
    }

    std::cout << "📊 Read achieved " << responses << "/" << required 
              << " required responses" << std::endl;

    if (responses < required) {
        return "ERROR: Consistency level not met\n";
    }

    // Return the value (for now just take first response)
    return !values.empty() ? values[0] : "(nil)\n";
}



/*
 * Writes a key-value pair to the distributed key-value store with the specified consistency level.
 * 
 * Flow:
 * 1. Checks if the key is managed by the local node using isLocalKey(key).
 *    - If yes: Writes the value locally and increments successfulWrites.
 * 2. If not local, retrieves the list of replica nodes for the key from the cluster manager.
 *    - If no replica nodes are found, returns an error.
 * 3. Attempts to write the value to each replica node:
 *    a. Creates a TCP connection to each replica node (skipping self).
 *    b. Sends a SET command with the key and value to the replica node.
 *    c. Receives and checks the response for success.
 *    d. Increments successfulWrites for each successful write.
 * 4. Determines how many successful writes are required based on the requested consistency level:
 *    - ONE:    At least one successful write is required.
 *    - QUORUM: A majority of replicas must respond successfully.
 *    - ALL:    All replicas must respond successfully.
 * 5. If the number of successful writes is less than required, returns an error.
 * 6. Returns "OK" if enough successful writes were achieved, or an error message otherwise.
 * 
 * This function ensures that write operations respect the desired consistency guarantees
 * (ONE, QUORUM, or ALL) by contacting the appropriate number of replica nodes and waiting for their responses.
 */
std::string DistributedKVStore::writeWithConsistency(
    const std::string& key,
    const std::string& value,
    ClusterConfig::ConsistencyLevel level) {
    
    auto replicaNodes = clusterManager_->getReplicaNodes(key, config_.replication_factor);
    int successfulWrites = 0;

    // Write locally first
    if (isLocalKey(key)) {
        std::cout << "📝 WAL: PUT " << key << " = " << value << std::endl;
        if (localStore_->processCommand("SET " + key + " " + value).find("OK") != std::string::npos) {
            successfulWrites++;
            std::cout << "✅ Written locally as replica" << std::endl;
        }
    }

    // Write to replicas
    for (const auto& node : replicaNodes) {
        if (node->getId() == nodeId_) continue; // Skip self

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) continue;

        sockaddr_in nodeAddr{};
        nodeAddr.sin_family = AF_INET;
        nodeAddr.sin_port = htons(node->getPort());
        nodeAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

        if (connect(sock, (struct sockaddr*)&nodeAddr, sizeof(nodeAddr)) < 0) {
            close(sock);
            continue;
        }

        std::string setCmd = "SET " + key + " " + value + "\n";
        send(sock, setCmd.c_str(), setCmd.length(), 0);

        char buffer[1024];
        ssize_t bytesRead = recv(sock, buffer, sizeof(buffer)-1, 0);
        if (bytesRead > 0) {
            buffer[bytesRead] = '\0';
            if (std::string(buffer).find("OK") != std::string::npos) {
                successfulWrites++;
                std::cout << "✅ Write successful to " << node->getId() << std::endl;
            }
        }
        close(sock);

        // For ONE consistency, return as soon as we have one successful write
        if (level == ClusterConfig::ConsistencyLevel::ONE && successfulWrites >= 1) {
            return "OK\n";
        }
    }

    // Calculate required writes
    int required = 1;  // Default for ONE
    if (level == ClusterConfig::ConsistencyLevel::QUORUM) {
        required = (config_.replication_factor / 2) + 1;
    } else if (level == ClusterConfig::ConsistencyLevel::ALL) {
        required = config_.replication_factor;
    }

    std::cout << "📊 Achieved " << successfulWrites << "/" << required 
              << " successful writes (RF=" << config_.replication_factor << ")" << std::endl;

    if (successfulWrites >= required) {
        return "OK\n";
    }

    return "ERROR: Failed to achieve consistency level " + 
           std::to_string(required) + "/" + 
           std::to_string(successfulWrites) + " successful\n";
}