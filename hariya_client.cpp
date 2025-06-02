// ========================================
// HARIYA KV CLIENT - Simple Client Implementation
// ========================================

#include <iostream>
#include <string>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>

class HariyaClient {
private:
    int clientSocket;
    std::string serverHost;
    int serverPort;
    bool connected;
    
public:
    HariyaClient(const std::string& host = "127.0.0.1", int port = 6379)
        : clientSocket(-1), serverHost(host), serverPort(port), connected(false) {}
    
    ~HariyaClient() {
        disconnect();
    }
    
    bool connect() {
        // Create socket
        clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (clientSocket == -1) {
            std::cerr << "❌ Failed to create socket" << std::endl;
            return false;
        }
        
        // Setup server address
        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(serverPort);
        
        if (inet_pton(AF_INET, serverHost.c_str(), &serverAddr.sin_addr) <= 0) {
            std::cerr << "❌ Invalid server address: " << serverHost << std::endl;
            close(clientSocket);
            return false;
        }
        
        // Connect to server
        if (::connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            std::cerr << "❌ Failed to connect to " << serverHost << ":" << serverPort << std::endl;
            close(clientSocket);
            return false;
        }
        
        connected = true;
        std::cout << "✅ Connected to Hariya server at " << serverHost << ":" << serverPort << std::endl;
        
        // Read welcome message
        readResponse();
        
        return true;
    }
    
    void disconnect() {
        if (connected && clientSocket != -1) {
            close(clientSocket);
            connected = false;
            std::cout << "🔌 Disconnected from server" << std::endl;
        }
    }
    
    bool sendCommand(const std::string& command) {
        if (!connected) {
            std::cerr << "❌ Not connected to server" << std::endl;
            return false;
        }
        
        std::string cmdWithNewline = command + "\n";
        ssize_t sent = send(clientSocket, cmdWithNewline.c_str(), cmdWithNewline.length(), 0);
        
        if (sent == -1) {
            std::cerr << "❌ Failed to send command" << std::endl;
            return false;
        }
        
        return true;
    }
    
    std::string readResponse() {
        if (!connected) {
            return "";
        }
        
        char buffer[4096];
        ssize_t received = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        
        if (received == -1) {
            std::cerr << "❌ Failed to receive response" << std::endl;
            return "";
        }
        
        if (received == 0) {
            std::cout << "🔌 Server closed connection" << std::endl;
            connected = false;
            return "";
        }
        
        buffer[received] = '\0';
        return std::string(buffer);
    }
    
    void runInteractive() {
        std::string command;
        std::cout << std::endl;
        std::cout << "🎯 Hariya KV Client - Interactive Mode" << std::endl;
        std::cout << "Type 'HELP' for commands or 'QUIT' to exit" << std::endl;
        std::cout << "================================================" << std::endl;
        
        while (connected) {
            std::cout << "hariya> ";
            std::getline(std::cin, command);
            
            if (command.empty()) {
                continue;
            }
            
            // Convert to uppercase for consistency
            std::string upperCmd = command;
            std::transform(upperCmd.begin(), upperCmd.end(), upperCmd.begin(), ::toupper);
            
            if (upperCmd == "QUIT" || upperCmd == "EXIT") {
                sendCommand("QUIT");
                break;
            }
            
            if (sendCommand(command)) {
                std::string response = readResponse();
                if (!response.empty()) {
                    std::cout << response;
                    if (response.back() != '\n') {
                        std::cout << std::endl;
                    }
                }
            }
        }
    }
    
    bool isConnected() const {
        return connected;
    }
};

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [host] [port]" << std::endl;
    std::cout << "  host - Server hostname or IP (default: 127.0.0.1)" << std::endl;
    std::cout << "  port - Server port number (default: 6379)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << "                    # Connect to localhost:6379" << std::endl;
    std::cout << "  " << program_name << " 192.168.1.100      # Connect to specific IP" << std::endl;
    std::cout << "  " << program_name << " localhost 8080     # Connect to custom port" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 6379;
    
    // Parse command line arguments
    if (argc > 1) {
        if (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        host = argv[1];
    }
    
    if (argc > 2) {
        try {
            port = std::stoi(argv[2]);
            if (port < 1 || port > 65535) {
                std::cerr << "❌ Error: Port must be between 1 and 65535" << std::endl;
                return 1;
            }
        } catch (const std::exception& e) {
            std::cerr << "❌ Error: Invalid port number '" << argv[2] << "'" << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }
    
    // Print client banner
    std::cout << "┌─────────────────────────────────────┐" << std::endl;
    std::cout << "│       📱 HARIYA KV CLIENT 📱        │" << std::endl;
    std::cout << "│         Command-line Client         │" << std::endl;
    std::cout << "│         Version 1.1.0               │" << std::endl;
    std::cout << "└─────────────────────────────────────┘" << std::endl;
    
    // Create and connect client
    HariyaClient client(host, port);
    
    if (!client.connect()) {
        return 1;
    }
    
    // Run interactive session
    client.runInteractive();
    
    std::cout << "👋 Goodbye!" << std::endl;
    return 0;
}