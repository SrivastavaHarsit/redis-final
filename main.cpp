// ========================================
// HARIYA KV SERVER - Main Entry Point
// ========================================

#include "hariya_server.h"
#include <iostream>
#include <csignal>
#include <memory>

// Global server instance for signal handling
std::unique_ptr<HariyaServer> g_server;

// Signal handler for graceful shutdown
void signalHandler(int signal) {
    std::cout << "\n🛑 Received signal " << signal << ", shutting down gracefully..." << std::endl;
    if (g_server) {
        g_server->stop();
    }
    exit(0);
}

// Print usage information
void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [port] [wal_file]" << std::endl;
    std::cout << "  port     - Port number to listen on (default: 6379)" << std::endl;
    std::cout << "  wal_file - Path to WAL file (default: data/hariya.wal)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << "                    # Use defaults" << std::endl;
    std::cout << "  " << program_name << " 8080               # Custom port" << std::endl;
    std::cout << "  " << program_name << " 6379 my.wal        # Custom port and WAL file" << std::endl;
}

int main(int argc, char* argv[]) {
    // Default configuration
    int port = 6379;
    std::string wal_file = "data/hariya.wal";
    
    // Parse command line arguments
    if (argc > 1) {
        if (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        
        try {
            port = std::stoi(argv[1]);
            if (port < 1 || port > 65535) {
                std::cerr << "❌ Error: Port must be between 1 and 65535" << std::endl;
                return 1;
            }
        } catch (const std::exception& e) {
            std::cerr << "❌ Error: Invalid port number '" << argv[1] << "'" << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }
    
    if (argc > 2) {
        wal_file = argv[2];
    }
    
    // Print startup banner
    std::cout << "┌─────────────────────────────────────┐" << std::endl;
    std::cout << "│        🚀 HARIYA KV SERVER 🚀       │" << std::endl;
    std::cout << "│     Redis-inspired K-V Database     │" << std::endl;
    std::cout << "│         Version 1.1.0 (WAL)        │" << std::endl;
    std::cout << "└─────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;
    
    // Print configuration
    std::cout << "🔧 Configuration:" << std::endl;
    std::cout << "   📡 Port: " << port << std::endl;
    std::cout << "   💾 WAL File: " << wal_file << std::endl;
    std::cout << std::endl;
    
    // Setup signal handlers for graceful shutdown
    signal(SIGINT, signalHandler);   // Ctrl+C
    signal(SIGTERM, signalHandler);  // Termination request
    #ifndef _WIN32
    signal(SIGPIPE, SIG_IGN);        // Ignore broken pipe (client disconnect)
    #endif
    
    try {
        // Create and start server
        g_server = std::make_unique<HariyaServer>(port, wal_file);
        
        if (!g_server->start()) {
            std::cerr << "❌ Failed to start server on port " << port << std::endl;
            return 1;
        }
        
        std::cout << "✅ Server started successfully!" << std::endl;
        std::cout << "📊 Connection statistics will be shown in real-time" << std::endl;
        std::cout << "🔄 Press Ctrl+C to shutdown gracefully" << std::endl;
        std::cout << "=" << std::string(50, '=') << std::endl;
        
        // Run server (blocks until shutdown)
        g_server->run();
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Server error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Server shutdown complete" << std::endl;
    return 0;
}