// ========================================
// MESSAGE PROTOCOL - Header File
// Handles parsing and formatting of client-server messages
// ========================================

#ifndef MESSAGE_PROTOCOL_H
#define MESSAGE_PROTOCOL_H

#include <string>
#include <queue>
#include <sstream>

class MessageProtocol {
public:
    // Structure to hold a parsed command
    struct Message {
        std::string command;    // The main command (GET, SET, etc.)
        std::string payload;    // Additional data (key, value, etc.)
        
        Message(const std::string& cmd, const std::string& data) 
            : command(cmd), payload(data) {}
    };
    
private:
    std::string buffer;                    // Accumulates incoming data
    std::queue<Message> messageQueue;      // Queue of complete messages ready for processing
    
public:
    MessageProtocol();
    
    // Add incoming data to the buffer and parse complete messages
    void receiveData(const std::string& data);
    
    // Get the next complete message (returns false if no messages available)
    bool getNextMessage(Message& message);
    
    // Check if there are any complete messages waiting to be processed
    bool hasMessages() const;
    
    // Clear all buffered data and queued messages
    void clear();
    
private:
    // Parse the buffer and extract complete messages
    void parseMessages();
    
    // Extract command and payload from a complete message line
    Message parseCommandLine(const std::string& line);
};

#endif // MESSAGE_PROTOCOL_H