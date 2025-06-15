#ifndef HARIYA_SERVER_H
#define HARIYA_SERVER_H

#include <atomic>
#include <thread>
#include <map>
#include <mutex>
#include <netinet/in.h>  // for sockaddr_in
#include <netinet/tcp.h> // for TCP_KEEPALIVE etc
#include <arpa/inet.h>   // for inet_ntoa
#include "distributed_kv_store.h"
#include "connection_manager.h"
#include "message_protocol.h"

class HariyaServer {
private:
    int serverSocket;
    int port;
    std::atomic<bool> running{true};
    std::shared_ptr<DistributedKVStore> distributedStore_;
    ConnectionManager connectionManager;

    std::string nodeId_;
    std::string hostname_;

    // Rate limiting members
    std::map<std::string, std::pair<time_t, int>> connectionRates_;
    std::mutex rateLimitMutex_;
    const int MAX_CONNECTIONS_PER_IP = 5;
    const int RATE_LIMIT_WINDOW = 1;
    
    bool isConnectionAllowed(const struct sockaddr_in& clientAddr);  // Declaration only

public:
    explicit HariyaServer(int port, 
        const std::string& nodeId = "",
        const std::string& hostname = "127.0.0.1",
        const std::string& wal_path = "data/hariya.wal");
    ~HariyaServer();
    
    bool start();
    void run();
    void stop();
    int getActiveConnections() const;
    void joinCluster(const std::vector<std::string>& seedNodes = {});
    void addClusterNode(const std::string& nodeId, const std::string& host, int port);
    void removeClusterNode(const std::string& nodeId);
    void debugStatus();

private:
    void handleClient(int clientSocket);
    std::string generateNodeId();
};

#endif // HARIYA_SERVER_H