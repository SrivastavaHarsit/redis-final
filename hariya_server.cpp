
// ========================================
// HARIYA KV SERVER - Main Server Implementation with WAL
// ========================================

#include "hariya_server.h"
#include <iostream>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// Make sure all member variables (port, serverSocket, running, kvStore, connectionManager) are declared in the HariyaServer class in hariya_server.h

// Implementation of HariyaServer methods
HariyaServer::HariyaServer(int p, const std::string& wal_path)
    : port(p), serverSocket(-1), running(true), kvStore(wal_path) {
    std::cout << "🔄 Server initialized with WAL persistence\n";
}

HariyaServer::~HariyaServer() {
    stop();
    std::cout << "💾 Flushing WAL to disk before shutdown...\n";
    kvStore.sync();
    std::cout << "✅ WAL flush complete\n";
}

bool HariyaServer::start() {
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "Failed to create socket\n";
        return false;
    }

    int opt = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port);

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Failed to bind to port " << port << "\n";
        return false;
    }

    if (listen(serverSocket, 10) < 0) {
        std::cerr << "Failed to listen\n";
        return false;
    }

    std::cout << "🚀 Hariya KV Server listening on port " << port << "\n";
    std::cout << "💾 WAL-enabled persistent storage ready\n";
    std::cout << "Ready to accept connections\n";
    std::cout << "Commands: GET, SET, DEL, EXISTS, KEYS, DBSIZE, FLUSHDB, PING, INFO, WALSTATS\n";
    return true;
}

void HariyaServer::run() {
    while (running) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);

        int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientSocket < 0) {
            if (running) {
                std::cerr << "Failed to accept connection\n";
            }
            continue;
        }

        std::thread clientThread(&HariyaServer::handleClient, this, clientSocket);
        clientThread.detach();
    }
}

void HariyaServer::stop() {
    running = false;
    if (serverSocket != -1) {
        std::cout << "💾 Performing final WAL sync...\n";
        kvStore.sync();

        close(serverSocket);
        serverSocket = -1;
        std::cout << "🛑 Server socket closed\n";
    }
}

int HariyaServer::getActiveConnections() const {
    return connectionManager.getActiveConnections();
}

void HariyaServer::handleClient(int clientSocket) {
    connectionManager.newConnection();
    int connectionId = connectionManager.getActiveConnections();

    std::cout << "Client " << connectionId << " connected! Active connections: " 
              << connectionManager.getActiveConnections() << "\n";

    std::string welcome = "Welcome to Hariya KV Server v1.0 (WAL-enabled)\n"
                         "Connection #" + std::to_string(connectionId) + "\n"
                         "💾 Persistent storage with Write-Ahead Logging\n"
                         "Type HELP for commands or QUIT to exit\n";
    send(clientSocket, welcome.c_str(), welcome.length(), 0);

    MessageProtocol protocol;
    char buffer[1024];

    while (true) {
        ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesRead <= 0) {
            break;
        }

        buffer[bytesRead] = '\0';
        protocol.receiveData(std::string(buffer));

        MessageProtocol::Message msg("", "");
        while (protocol.getNextMessage(msg)) {
            std::cout << "Client " << connectionId << " -> " << msg.command;
            if (!msg.payload.empty()) {
                std::cout << " " << msg.payload;
            }
            std::cout << "\n";

            if (msg.command == "QUIT" || msg.command == "EXIT") {
                std::string goodbye = "Goodbye from Hariya! 💾 Your data is safely persisted.\n";
                send(clientSocket, goodbye.c_str(), goodbye.length(), 0);
                goto cleanup;
            }

            if (msg.command == "WALSYNC" || msg.command == "SYNC") {
                kvStore.sync();
                std::string response = "OK - WAL synced to disk\n";
                send(clientSocket, response.c_str(), response.length(), 0);
                std::cout << "Client " << connectionId << " <- WAL SYNC performed\n";
                continue;
            }

            connectionManager.commandExecuted();
            std::string response = kvStore.processCommand(msg.command + " " + msg.payload);
            send(clientSocket, response.c_str(), response.length(), 0);

            std::cout << "Client " << connectionId << " <- " << response;

            static int writeCommandCount = 0;
            if (msg.command == "SET" || msg.command == "PUT" || 
                msg.command == "DEL" || msg.command == "DELETE" || 
                msg.command == "FLUSHDB" || msg.command == "CLEAR") {
                writeCommandCount++;
                if (writeCommandCount >= 10 || msg.command == "FLUSHDB" || msg.command == "CLEAR") {
                    kvStore.sync();
                    writeCommandCount = 0;
                    std::cout << "🔄 Auto-sync: WAL flushed to disk\n";
                }
            }
        }
    }

    cleanup:
    close(clientSocket);
    connectionManager.closeConnection();

    std::cout << "Client " << connectionId << " disconnected! Active connections: " 
              << connectionManager.getActiveConnections() << "\n";
}