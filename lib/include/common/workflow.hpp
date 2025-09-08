#ifndef WORKFLOW_H
#define WORKFLOW_H

#include <cstdint>
enum class Workflow : uint8_t
{
  Accept,           // Accept a new connection
  Recv,             // Receive data
  Parse,            // Parse the request
  FindHandler,      // Find the appropriate handler
  GenerateResponse, // Generate the response
  RequestFlush,     // Request to flush the response
  Send,             // Send data
  RequestClose,     // Request to close the connection
  Closed,           // Closed the connection
};

#endif // WORKFLOW_H
