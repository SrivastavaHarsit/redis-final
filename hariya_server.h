// ========================================
// HARIYA KV SERVER - Header File (WAL-Enabled)
// ========================================

#ifndef HARIYA_SERVER_H
#define HARIYA_SERVER_H

#include <atomic>
#include <thread>
#include "kv_store.h"
#include "connection_manager.h"
#include "message_protocol.h"

class HariyaServer {
private:
    int serverSocket;
    int port;
    std::atomic<bool> running{true};
    ThreadSafeKVStore kvStore;  // Will be initialized with WAL path
    ConnectionManager connectionManager;
    
public:
    // NEW: Constructor can optionally take WAL file path
    explicit HariyaServer(int port, const std::string& wal_path = "data/hariya.wal");
    ~HariyaServer();
    
    bool start();
    void run();
    void stop();
    int getActiveConnections() const;
    
    // NEW: Method to sync WAL to disk
    void syncWAL();
    
private:
    void handleClient(int clientSocket);
};

#endif // HARIYA_SERVER_H