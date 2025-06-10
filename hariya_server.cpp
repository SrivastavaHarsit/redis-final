// ========================================
// HARIYA KV SERVER - ENHANCED DEBUG VERSION
// ========================================

#include "hariya_server.h"
#include <iostream>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>

HariyaServer::HariyaServer(int p, 
                           const std::string& nodeId,
                           const std::string& hostname,
                           const std::string& wal_path)
    : port(p), serverSocket(-1), running(false), hostname_(hostname) {
    
    std::cout << "🔧 HariyaServer constructor called with:" << std::endl;
    std::cout << "   Port: " << p << std::endl;
    std::cout << "   NodeId: " << nodeId << std::endl;
    std::cout << "   Hostname: " << hostname << std::endl;
    std::cout << "   WAL Path: " << wal_path << std::endl;
    
    nodeId_ = nodeId.empty() ? generateNodeId() : nodeId;
    
    try {
        std::cout << "🔄 Creating DistributedKVStore..." << std::endl;
        distributedStore_ = std::make_unique<DistributedKVStore>(nodeId_, hostname_, port, wal_path);
        std::cout << "✅ DistributedKVStore created successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to create DistributedKVStore: " << e.what() << std::endl;
        throw;
    }
    
    std::cout << "✅ Distributed server initialized: " << nodeId_ 
              << "@" << hostname_ << ":" << port << std::endl;
}

HariyaServer::~HariyaServer() {
    std::cout << "🔄 HariyaServer destructor called" << std::endl;
    stop();
    std::cout << "💾 Flushing WAL to disk before shutdown..." << std::endl;
    if (distributedStore_) {
        distributedStore_->sync();
    }
    std::cout << "✅ WAL flush complete" << std::endl;
}

bool HariyaServer::start() {
    std::cout << "🚀 HariyaServer::start() called" << std::endl;
    
    if (running) {
        std::cout << "⚠️  Server already running" << std::endl;
        return true;
    }
    
    // Step 1: Create socket
    std::cout << "🔧 Step 1: Creating server socket..." << std::endl;
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "❌ Failed to create socket (errno: " << errno << " - " << strerror(errno) << ")" << std::endl;
        return false;
    }
    std::cout << "✅ Socket created successfully (fd: " << serverSocket << ")" << std::endl;

    // Step 2: Set socket options
    std::cout << "🔧 Step 2: Setting socket options..." << std::endl;
    int opt = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "❌ Failed to set SO_REUSEADDR (errno: " << errno << " - " << strerror(errno) << ")" << std::endl;
        close(serverSocket);
        serverSocket = -1;
        return false;
    }
    std::cout << "✅ SO_REUSEADDR option set" << std::endl;

    // Step 3: Setup address structure
    std::cout << "🔧 Step 3: Setting up address structure..." << std::endl;
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;  // Listen on all interfaces
    serverAddr.sin_port = htons(port);
    
    std::cout << "   Family: " << serverAddr.sin_family << std::endl;
    std::cout << "   Address: " << inet_ntoa(serverAddr.sin_addr) << std::endl;
    std::cout << "   Port: " << ntohs(serverAddr.sin_port) << std::endl;

    // Step 4: Bind socket
    std::cout << "🔧 Step 4: Binding to port " << port << "..." << std::endl;
    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "❌ Failed to bind to port " << port << " (errno: " << errno << " - " << strerror(errno) << ")" << std::endl;
        if (errno == EADDRINUSE) {
            std::cerr << "   🚨 Port " << port << " is already in use!" << std::endl;
            std::cerr << "   💡 Try: sudo netstat -tlnp | grep " << port << std::endl;
            std::cerr << "   💡 Or try: sudo lsof -i :" << port << std::endl;
        } else if (errno == EACCES) {
            std::cerr << "   🚨 Permission denied! Try a port > 1024 or run as root" << std::endl;
        }
        close(serverSocket);
        serverSocket = -1;
        return false;
    }
    std::cout << "✅ Successfully bound to port " << port << std::endl;

    // Step 5: Start listening
    std::cout << "🔧 Step 5: Starting to listen (backlog: 10)..." << std::endl;
    if (listen(serverSocket, 10) < 0) {
        std::cerr << "❌ Failed to listen (errno: " << errno << " - " << strerror(errno) << ")" << std::endl;
        close(serverSocket);
        serverSocket = -1;
        return false;
    }
    std::cout << "✅ Socket is now listening" << std::endl;

    // Step 6: Verify socket status
    std::cout << "🔧 Step 6: Verifying socket status..." << std::endl;
    sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    if (getsockname(serverSocket, (struct sockaddr*)&addr, &addrLen) == 0) {
        std::cout << "✅ Socket is listening on " << inet_ntoa(addr.sin_addr) 
                  << ":" << ntohs(addr.sin_port) << std::endl;
    } else {
        std::cerr << "⚠️  Could not verify socket address (errno: " << errno << ")" << std::endl;
    }

    // Step 7: Set running flag
    running = true;
    std::cout << "✅ Server started successfully!" << std::endl;
    std::cout << "🚀 Hariya KV Server listening on port " << port << std::endl;
    std::cout << "💾 WAL-enabled persistent storage ready" << std::endl;
    std::cout << "🔗 Ready to accept connections" << std::endl;
    std::cout << "📍 Test connection with: telnet localhost " << port << std::endl;
    std::cout << "📍 Commands: GET, SET, DEL, EXISTS, KEYS, DBSIZE, FLUSHDB, PING, INFO, WALSTATS" << std::endl;
    
    return true;
}

void HariyaServer::run() {
    std::cout << "🏃 HariyaServer::run() called" << std::endl;
    
    if (!running) {
        std::cerr << "❌ Server not started. Call start() first." << std::endl;
        return;
    }
    
    if (serverSocket == -1) {
        std::cerr << "❌ Invalid server socket. Server may not have started properly." << std::endl;
        return;
    }
    
    std::cout << "⏳ Server running and waiting for connections on fd " << serverSocket << "..." << std::endl;
    
    while (running) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);

        std::cout << "🔄 Calling accept() on socket " << serverSocket << "..." << std::endl;
        int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientLen);
        
        if (clientSocket < 0) {
            if (running) {
                std::cerr << "❌ Failed to accept connection (errno: " << errno << " - " << strerror(errno) << ")" << std::endl;
                if (errno == EINTR) {
                    std::cout << "🔄 Accept interrupted by signal, continuing..." << std::endl;
                    continue;
                } else if (errno == EBADF) {
                    std::cerr << "❌ Bad file descriptor - server socket may be closed" << std::endl;
                    break;
                }
            } else {
                std::cout << "🛑 Server stopped, exiting accept loop" << std::endl;
            }
            continue;
        }

        std::cout << "🎉 New connection accepted from " 
                  << inet_ntoa(clientAddr.sin_addr) << ":" << ntohs(clientAddr.sin_port) 
                  << " (client fd: " << clientSocket << ")" << std::endl;

        try {
            std::thread clientThread(&HariyaServer::handleClient, this, clientSocket);
            clientThread.detach();
            std::cout << "🚀 Client handler thread started" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "❌ Failed to create client thread: " << e.what() << std::endl;
            close(clientSocket);
        }
    }
    
    std::cout << "🛑 Server run loop ended" << std::endl;
}

void HariyaServer::stop() {
    std::cout << "🛑 HariyaServer::stop() called" << std::endl;
    running = false;
    if (serverSocket != -1) {
        std::cout << "💾 Performing final WAL sync..." << std::endl;
        if (distributedStore_) {
            distributedStore_->sync();
        }
        
        std::cout << "🔌 Closing server socket " << serverSocket << "..." << std::endl;
        close(serverSocket);
        serverSocket = -1;
        std::cout << "✅ Server socket closed" << std::endl;
    }
}

int HariyaServer::getActiveConnections() const {
    return connectionManager.getActiveConnections();
}

void HariyaServer::handleClient(int clientSocket) {
    std::cout << "📞 Handling client connection (fd: " << clientSocket << ")" << std::endl;
    
    connectionManager.newConnection();
    int connectionId = connectionManager.getActiveConnections();

    std::cout << "🔗 Client " << connectionId << " connected! Active connections: "
              << connectionManager.getActiveConnections() << std::endl;

    std::string welcome = "🎉 Welcome to Hariya Distributed KV Server v2.0\n"
                         "Node: " + nodeId_ + "@" + hostname_ + ":" + std::to_string(port) + "\n"
                         "💾 Persistent storage with WAL\n"
                         "🌐 Distributed cluster with hashing\n"
                         "Type HELP or QUIT\n";
    
    MessageProtocol protocol;
    char buffer[1024];

    ssize_t sent = send(clientSocket, welcome.c_str(), welcome.length(), 0);
    if (sent < 0) {
        std::cerr << "❌ Failed to send welcome message (errno: " << errno << ")" << std::endl;
        goto cleanup;
    }
    std::cout << "✅ Welcome message sent to client " << connectionId << std::endl;

    while (true) {
        std::cout << "⏳ Waiting for data from client " << connectionId << "..." << std::endl;
        ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        
        if (bytesRead < 0) {
            std::cerr << "❌ Error reading from client " << connectionId << " (errno: " << errno << ")" << std::endl;
            break;
        } else if (bytesRead == 0) {
            std::cout << "🔌 Client " << connectionId << " closed connection" << std::endl;
            break;
        }

        buffer[bytesRead] = '\0';
        std::cout << "📨 Received " << bytesRead << " bytes from client " << connectionId << ": " << buffer;
        
        protocol.receiveData(std::string(buffer));

        MessageProtocol::Message msg("", "");
        while (protocol.getNextMessage(msg)) {
            std::cout << "🔄 Processing command from client " << connectionId << ": " << msg.command;
            if (!msg.payload.empty()) std::cout << " " << msg.payload;
            std::cout << std::endl;

            if (msg.command == "QUIT" || msg.command == "EXIT") {
                std::string goodbye = "👋 Goodbye from Hariya Distributed KV!\n";
                send(clientSocket, goodbye.c_str(), goodbye.length(), 0);
                std::cout << "👋 Client " << connectionId << " requested disconnect" << std::endl;
                goto cleanup;
            }
            else if (msg.command == "SYNC") {
                if (distributedStore_) {
                    distributedStore_->sync();
                }
                std::string response = "✅ OK - WAL synced to disk\n";
                send(clientSocket, response.c_str(), response.length(), 0);
                std::cout << "💾 WAL sync performed for client " << connectionId << std::endl;
                continue;
            }
            else if (msg.command == "PING") {
                std::string response = "🏓 PONG\n";
                send(clientSocket, response.c_str(), response.length(), 0);
                std::cout << "🏓 PING/PONG with client " << connectionId << std::endl;
                continue;
            }

            connectionManager.commandExecuted();
            
            std::string response;
            if (distributedStore_) {
                response = distributedStore_->processCommand(msg.command + " " + msg.payload);
            } else {
                response = "❌ ERROR: Distributed store not initialized\n";
            }
            
            ssize_t sent = send(clientSocket, response.c_str(), response.length(), 0);
            if (sent < 0) {
                std::cerr << "❌ Failed to send response to client " << connectionId << " (errno: " << errno << ")" << std::endl;
                break;
            }
            std::cout << "📤 Response sent to client " << connectionId << ": " << response;
        }
    }

cleanup:
    std::cout << "🧹 Cleaning up client " << connectionId << " connection" << std::endl;
    close(clientSocket);
    connectionManager.closeConnection();
    std::cout << "✅ Client " << connectionId << " disconnected! Active connections: " 
              << connectionManager.getActiveConnections() << std::endl;
}

// Add this debug method to your HariyaServer class
void HariyaServer::debugStatus() {
    std::cout << "🔍 DEBUG STATUS:" << std::endl;
    std::cout << "   running: " << (running ? "true" : "false") << std::endl;
    std::cout << "   serverSocket: " << serverSocket << std::endl;
    std::cout << "   port: " << port << std::endl;
    std::cout << "   nodeId_: " << nodeId_ << std::endl;
    std::cout << "   hostname_: " << hostname_ << std::endl;
    std::cout << "   distributedStore_: " << (distributedStore_ ? "initialized" : "null") << std::endl;
    
    // Check if socket is valid and listening
    if (serverSocket != -1) {
        sockaddr_in addr;
        socklen_t addrLen = sizeof(addr);
        if (getsockname(serverSocket, (struct sockaddr*)&addr, &addrLen) == 0) {
            std::cout << "   Socket bound to: " << inet_ntoa(addr.sin_addr) 
                      << ":" << ntohs(addr.sin_port) << std::endl;
        } else {
            std::cout << "   Socket binding status: ERROR (" << strerror(errno) << ")" << std::endl;
        }
        
        // Check socket options
        int optval;
        socklen_t optlen = sizeof(optval);
        if (getsockopt(serverSocket, SOL_SOCKET, SO_ACCEPTCONN, &optval, &optlen) == 0) {
            std::cout << "   Socket listening: " << (optval ? "YES" : "NO") << std::endl;
        }
    }
}

void HariyaServer::joinCluster(const std::vector<std::string>& seedNodes) {
    if (distributedStore_) {
        distributedStore_->joinCluster(seedNodes);
    }
}

void HariyaServer::addClusterNode(const std::string& nodeId, const std::string& host, int port) {
    if (distributedStore_) {
        distributedStore_->addNode(nodeId, host, port);
    }
}

void HariyaServer::removeClusterNode(const std::string& nodeId) {
    if (distributedStore_) {
        distributedStore_->removeNode(nodeId);
    }
}

std::string HariyaServer::generateNodeId() {
    return hostname_ + "_" + std::to_string(port) + "_" + std::to_string(time(nullptr));
}