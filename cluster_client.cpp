#include "cluster_client.h"
#include <iostream>
#include <sstream>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <algorithm>
#include <errno.h>
#include <sys/socket.h>

HariyaClusterClient::HariyaClusterClient(int max_retries, int timeout_ms)
    : currentNodeIndex_(-1), maxRetries_(max_retries), timeoutMs_(timeout_ms), 
      autoDiscovery_(true), rng_(std::chrono::steady_clock::now().time_since_epoch().count()) {}

HariyaClusterClient::~HariyaClusterClient() { disconnect(); }

bool HariyaClusterClient::addNode(const std::string& hostname, int port) {
    nodes_.emplace_back(hostname, port);
    std::cout << "➕ Added node: " << hostname << ":" << port << "\n";
    return true;
}

bool HariyaClusterClient::connectToCluster() {
    if (nodes_.empty()) {
        std::cerr << "❌ No nodes configured\n";
        return false;
    }
    currentNodeIndex_ = findHealthyNode();
    if (currentNodeIndex_ == -1) {
        std::cerr << "❌ No healthy nodes\n";
        return false;
    }
    std::cout << "✅ Connected via " << nodes_[currentNodeIndex_].hostname 
              << ":" << nodes_[currentNodeIndex_].port << "\n";
    
    // Don't call updateClusterTopology() here as it might affect the connection
    // if (autoDiscovery_) updateClusterTopology();
    return true;
}

std::string HariyaClusterClient::smartGet(const std::string& key) {
    return executeWithRetry("GET " + key);
}

bool HariyaClusterClient::smartSet(const std::string& key, const std::string& value) {
    std::string response = executeWithRetry("SET " + key + " " + value);
    return response.find("OK") != std::string::npos;
}

std::string HariyaClusterClient::executeWithRetry(const std::string& command) {
    for (int attempt = 0; attempt < maxRetries_; ++attempt) {
        if (currentNodeIndex_ == -1) {
            currentNodeIndex_ = findHealthyNode();
            if (currentNodeIndex_ == -1) return "ERROR: No healthy nodes\n";
        }
        
        if (sendCommand(currentNodeIndex_, command)) {
            std::string response = readResponse(currentNodeIndex_);
            std::string nodeAddress;
            if (handleRoutingResponse(response, nodeAddress)) {
                std::string hostname;
                int port;
                parseNodeAddress(nodeAddress, hostname, port);
                bool found = false;
                for (size_t i = 0; i < nodes_.size(); ++i) {
                    if (nodes_[i].hostname == hostname && nodes_[i].port == port) {
                        currentNodeIndex_ = i;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    addNode(hostname, port);
                    currentNodeIndex_ = nodes_.size() - 1;
                }
                if (connectToNode(currentNodeIndex_)) continue;
            }
            return response;
        }
        
        disconnectFromNode(currentNodeIndex_);
        currentNodeIndex_ = findHealthyNode();
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
    }
    return "ERROR: All retries failed\n";
}

bool HariyaClusterClient::handleRoutingResponse(const std::string& response, std::string& nodeAddress) {
    if (response.find("ROUTED to") != std::string::npos) {
        size_t atPos = response.find('@');
        if (atPos != std::string::npos) {
            size_t start = atPos + 1;
            size_t end = response.find('\n', start);
            if (end == std::string::npos) end = response.length();
            nodeAddress = response.substr(start, end - start);
            return true;
        }
    }
    return false;
}

void HariyaClusterClient::runInteractiveCluster() {
    std::cout << "Entering interactive mode\n"; // Debug line
    std::cout << "\n🌐 Hariya Cluster Client - Interactive Mode\n"
              << "Connected to " << nodes_.size() << " nodes\n"
              << "Type 'CLUSTER INFO' or 'QUIT'\n"
              << "=====================================================\n";
    
    std::string command;
    bool keepRunning = true;
    
    while (keepRunning) {
        // Check if we have a valid connection, if not try to reconnect
        if (currentNodeIndex_ == -1) {
            std::cout << "🔄 Connection lost, attempting to reconnect...\n";
            currentNodeIndex_ = findHealthyNode();
            if (currentNodeIndex_ == -1) {
                std::cout << "❌ Unable to reconnect to any node. Exiting.\n";
                break;
            }
            std::cout << "✅ Reconnected to " << nodes_[currentNodeIndex_].hostname 
                      << ":" << nodes_[currentNodeIndex_].port << "\n";
        }
        
        std::cout << "hariya-cluster> ";
        std::cout.flush(); // Ensure prompt is displayed
        
        if (!std::getline(std::cin, command)) {
            // EOF or input error
            break;
        }
        
        if (command.empty()) continue;
        
        std::string upperCmd = command;
        std::transform(upperCmd.begin(), upperCmd.end(), upperCmd.begin(), ::toupper);
        if (upperCmd == "QUIT" || upperCmd == "EXIT") {
            keepRunning = false;
            break;
        }
        
        std::string response = executeWithRetry(command);
        std::cout << response;
        if (!response.empty() && response.back() != '\n') {
            std::cout << "\n";
        }
    }
    
    std::cout << "👋 Goodbye!\n";
}

int HariyaClusterClient::findHealthyNode() {
    for (size_t i = 0; i < nodes_.size(); ++i) {
        if (connectToNode(i)) return i;
    }
    return -1;
}

bool HariyaClusterClient::connectToNode(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex >= static_cast<int>(nodes_.size())) {
        std::cout << "❌ Invalid node index: " << nodeIndex << "\n";
        return false;
    }
    
    NodeInfo& node = nodes_[nodeIndex];
    if (node.connected) {
        std::cout << "✅ Already connected to " << node.hostname << ":" << node.port << "\n";
        return true;
    }
    
    std::cout << "🔄 Attempting to connect to " << node.hostname << ":" << node.port << "...\n";
    
    node.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (node.socket == -1) {
        std::cout << "❌ Failed to create socket\n";
        return false;
    }
    
    // Set socket timeout
    struct timeval timeout;
    timeout.tv_sec = 5;  // 5 second timeout
    timeout.tv_usec = 0;
    setsockopt(node.socket, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout));
    setsockopt(node.socket, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));
    
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(node.port);
    
    if (inet_pton(AF_INET, node.hostname.c_str(), &serverAddr.sin_addr) <= 0) {
        std::cout << "❌ Invalid address: " << node.hostname << "\n";
        close(node.socket);
        node.socket = -1;
        return false;
    }
    
    std::cout << "🚀 Connecting to socket...\n";
    if (connect(node.socket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cout << "❌ Connection failed to " << node.hostname << ":" << node.port << " (errno: " << errno << ")\n";
        close(node.socket);
        node.socket = -1;
        node.failureCount++;
        return false;
    }
    
    std::cout << "✅ Socket connected successfully\n";
    node.connected = true;
    node.failureCount = 0;
    
    // Don't try to read welcome message immediately - let the interactive loop handle it
    std::cout << "🎉 Successfully connected to " << node.hostname << ":" << node.port << "\n";
    return true;
}

std::string HariyaClusterClient::readResponse(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex >= static_cast<int>(nodes_.size())) {
        return "";
    }
    
    NodeInfo& node = nodes_[nodeIndex];
    if (!node.connected || node.socket == -1) {
        return "";
    }
    
    char buffer[1024];
    std::cout << "📖 Reading response from server...\n";
    ssize_t bytes = recv(node.socket, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes < 0) {
        std::cout << "❌ Error reading from socket (errno: " << errno << ")\n";
        disconnectFromNode(nodeIndex);
        if (currentNodeIndex_ == nodeIndex) {
            currentNodeIndex_ = -1;
        }
        return "";
    } else if (bytes == 0) {
        std::cout << "❌ Connection closed by server\n";
        disconnectFromNode(nodeIndex);
        if (currentNodeIndex_ == nodeIndex) {
            currentNodeIndex_ = -1;
        }
        return "";
    }
    
    buffer[bytes] = '\0';
    std::cout << "📨 Received " << bytes << " bytes: " << buffer;
    return std::string(buffer);
}

bool HariyaClusterClient::sendCommand(int nodeIndex, const std::string& command) {
    if (nodeIndex < 0 || nodeIndex >= static_cast<int>(nodes_.size())) {
        return false;
    }
    
    NodeInfo& node = nodes_[nodeIndex];
    if (!node.connected || node.socket == -1) {
        return false;
    }
    
    std::string cmd = command + "\n";
    std::cout << "📤 Sending command: " << command << "\n";
    ssize_t sent = send(node.socket, cmd.c_str(), cmd.length(), 0);
    if (sent == -1) {
        std::cout << "❌ Failed to send command (errno: " << errno << ")\n";
        disconnectFromNode(nodeIndex);
        if (currentNodeIndex_ == nodeIndex) {
            currentNodeIndex_ = -1;
        }
        return false;
    }
    std::cout << "✅ Command sent successfully (" << sent << " bytes)\n";
    return true;
}

// Placeholder for updateClusterTopology
void HariyaClusterClient::updateClusterTopology() {
    // Future: Query CLUSTER NODES and update nodes_
    // For now, just a placeholder to avoid affecting the connection
}

void HariyaClusterClient::parseNodeAddress(const std::string& address, std::string& hostname, int& port) {
    size_t colon = address.find(':');
    if (colon != std::string::npos) {
        hostname = address.substr(0, colon);
        port = std::stoi(address.substr(colon + 1));
    } else {
        hostname = address;
        port = 6379; // default port
    }
}

void HariyaClusterClient::disconnect() {
    for (size_t i = 0; i < nodes_.size(); ++i) {
        disconnectFromNode(i);
    }
    currentNodeIndex_ = -1;
}

void HariyaClusterClient::disconnectFromNode(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex >= static_cast<int>(nodes_.size())) return;
    NodeInfo& node = nodes_[nodeIndex];
    if (node.connected && node.socket != -1) {
        close(node.socket);
        node.socket = -1;
        node.connected = false;
    }
}

int main(int argc, char* argv[]) {
    std::cout << "🚀 Starting Hariya Cluster Client...\n";
    
    HariyaClusterClient client;
    
    // Parse command line arguments for nodes
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            std::string arg = argv[i];
            size_t colon = arg.find(':');
            if (colon != std::string::npos) {
                std::string hostname = arg.substr(0, colon);
                int port = std::stoi(arg.substr(colon + 1));
                client.addNode(hostname, port);
            } else {
                // Assume it's just a port number
                int port = std::stoi(arg);
                client.addNode("127.0.0.1", port);
            }
        }
    } else {
        // Default configuration
        client.addNode("127.0.0.1", 6379);
    }

    std::cout << "🔗 Attempting to connect to cluster...\n";
    if (!client.connectToCluster()) {
        std::cerr << "❌ Failed to connect to cluster.\n";
        return 1;
    }

    std::cout << "🎯 Connection successful! Starting interactive mode...\n";
    client.runInteractiveCluster();
    return 0;
}