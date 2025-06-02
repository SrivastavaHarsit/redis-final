// ========================================
// MESSAGE PROTOCOL - Implementation
// Handles the parsing of network messages from clients
// ========================================

#include "message_protocol.h"
#include <algorithm>
#include <iostream>

MessageProtocol::MessageProtocol() {
    // Constructor initializes empty buffer and message queue
}

// Receives raw data from network and adds it to our buffer
// Then attempts to parse complete messages
void MessageProtocol::receiveData(const std::string& data) {
    buffer += data;  // Add new data to existing buffer
    parseMessages(); // Try to extract complete messages
}

// Returns the next complete message if available
bool MessageProtocol::getNextMessage(Message& message) {
    if (messageQueue.empty()) {
        return false;  // No messages available
    }
    
    // Get the message from front of queue
    message = messageQueue.front();
    messageQueue.pop();
    return true;
}

// Check if we have any messages ready for processing
bool MessageProtocol::hasMessages() const {
    return !messageQueue.empty();
}

// Clear all buffered data and pending messages
void MessageProtocol::clear() {
    buffer.clear();
    std::queue<Message> emptyQueue;
    messageQueue.swap(emptyQueue);  // Clear the queue
}

// CORE PARSING LOGIC: Extract complete messages from buffer
// Messages are separated by newlines (\n or \r\n)
void MessageProtocol::parseMessages() {
    size_t pos = 0;
    
    // Look for complete lines (ending with \n)
    while ((pos = buffer.find('\n')) != std::string::npos) {
        // Extract one complete line
        std::string line = buffer.substr(0, pos);
        
        // Remove the processed line from buffer
        buffer.erase(0, pos + 1);
        
        // Remove carriage return if present (handles \r\n)
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        
        // Skip empty lines
        if (line.empty()) {
            continue;
        }
        
        // Parse the command line and add to message queue
        Message message = parseCommandLine(line);
        messageQueue.push(message);
        
        // Debug output (can be removed in production)
        std::cout << "📨 Parsed message: '" << message.command << "' with payload: '" 
                  << message.payload << "'" << std::endl;
    }
}

// Parse a single command line into command and payload
// Examples: "GET mykey" -> command="GET", payload="mykey"
//          "SET mykey myvalue" -> command="SET", payload="mykey myvalue"
MessageProtocol::Message MessageProtocol::parseCommandLine(const std::string& line) {
    std::istringstream iss(line);
    std::string command;
    
    // Extract the first word as command
    iss >> command;
    
    // Convert command to uppercase for consistency
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);
    
    // Get the rest of the line as payload
    std::string payload;
    if (std::getline(iss, payload)) {
        // Remove leading whitespace from payload
        payload.erase(0, payload.find_first_not_of(" \t"));
    }
    
    return Message(command, payload);
}