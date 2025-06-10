#ifndef CLUSTER_CLIENT_H
#define CLUSTER_CLIENT_H

#include <string>
#include <vector>
#include <map>
#include <random>
#include <sys/socket.h>
#include <netinet/in.h>

class HariyaClusterClient {
private:
    struct NodeInfo {
        std::string hostname;
        int port;
        int socket;
        bool connected;
        int failureCount;
        NodeInfo(const std::string& host, int p) 
            : hostname(host), port(p), socket(-1), connected(false), failureCount(0) {}
    };
    
    std::vector<NodeInfo> nodes_;
    std::map<std::string, std::string> nodeIdToAddress_;
    int currentNodeIndex_;
    std::mt19937 rng_;
    int maxRetries_;
    int timeoutMs_;
    bool autoDiscovery_;

public:
    HariyaClusterClient(int max_retries = 3, int timeout_ms = 5000);
    ~HariyaClusterClient();
    
    bool addNode(const std::string& hostname, int port);
    bool connectToCluster();
    void disconnect();
    
    std::string smartGet(const std::string& key);
    bool smartSet(const std::string& key, const std::string& value);
    bool smartDelete(const std::string& key);
    
    std::string getClusterInfo();
    std::vector<std::string> getClusterNodes();
    
    void runInteractiveCluster();

private:
    bool connectToNode(int nodeIndex);
    void disconnectFromNode(int nodeIndex);
    int findHealthyNode();
    bool sendCommand(int nodeIndex, const std::string& command);
    std::string readResponse(int nodeIndex);
    std::string executeWithRetry(const std::string& command);
    bool handleRoutingResponse(const std::string& response, std::string& nodeAddress);
    void updateClusterTopology();
    void parseNodeAddress(const std::string& address, std::string& hostname, int& port);
};

#endif // CLUSTER_CLIENT_H