# Coroutine-Based HTTP Server Refactoring

## Overview

This refactoring transforms the io_uring HTTP server from a callback-based event-driven architecture to a **coroutine-based design with separated I/O and CPU threads**:

- **1 dedicated I/O thread**: Handles ALL io_uring operations (submissions/completions)
- **N-1 worker threads**: Shared coroutine pool for CPU-bound work (parsing, handlers, response generation)

This architecture provides better CPU utilization and separates concerns between I/O monitoring and computation.

## Architecture Changes

### Before: Multi-Worker Event Loop
- N worker threads, each with its own io_uring instance
- SO_REUSEPORT to distribute connections across workers  
- Each worker processes both I/O events and internal operations
- Callback-based state machine spread across multiple functions

### After: Single I/O Thread + Coroutine Pool
- **1 I/O thread**: Monitors a single io_uring instance for all connections
- **N-1 coroutine threads**: Process CPU-bound tasks cooperatively
- Each client connection has its own coroutine
- I/O thread signals coroutines when I/O completes
- Coroutines yield control during long-running operations

## Thread Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Server                              │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         ThreadPool (N-1 threads)                     │  │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐       │  │
│  │  │ Coroutine  │ │ Coroutine  │ │ Coroutine  │  ...  │  │
│  │  │  Thread 1  │ │  Thread 2  │ │  Thread 3  │       │  │
│  │  └────────────┘ └────────────┘ └────────────┘       │  │
│  │         ↑             ↑             ↑                │  │
│  │         └─────────────┴─────────────┘                │  │
│  │              signal_ready()                          │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│  ┌──────────────────┴───────────────────────────────────┐  │
│  │         I/O Thread (Worker)                          │  │
│  │  ┌──────────────────────────────────────────────┐   │  │
│  │  │         io_uring (single instance)           │   │  │
│  │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │   │  │
│  │  │  │ Acc │ │ Recv│ │ Send│ │ Recv│ │ Send│...│   │  │
│  │  │  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘   │   │  │
│  │  └──────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Server (`server.hpp`, `server.cpp`)
- Owns the shared `ThreadPool` with N-1 threads
- Creates a single listening socket (no SO_REUSEPORT needed)
- Creates and owns the single `Worker` instance
- Routes are stored in the Server and accessed by all coroutines

### 2. Coroutine Thread Pool (`coroutine_thread_pool.hpp`)
- Manages N-1 worker threads that execute coroutines
- Coroutines can yield with `co_yield true` (ready to resume) or `co_yield false` (waiting for I/O)
- `signal_ready()` method allows the I/O thread to wake suspended coroutines
- Efficiently schedules ready coroutines across available threads

### 3. Worker (I/O Thread) (`worker.hpp`, `worker.cpp`)
- Runs on a **single dedicated thread**
- Manages **one** io_uring instance for all connections
- Handles accept, recv, send, and close operations
- When I/O completes:
  - Calls `owner->get_coro_pool().signal_ready(coroutine_handle)`
  - Moves the coroutine from waiting → ready queue
- No CPU-bound processing - purely I/O event monitoring

### 4. Client Workflow Coroutine (`client_workflow.cpp`)
The coroutine that handles each client connection runs on the thread pool:

```cpp
Task client_workflow_coro(Worker* worker, PerClientStorage* client)
{
  // 1. Get coroutine handle and store it
  auto handle = co_await current_handle();
  client->coro_handle = handle;

  // 2. Post initial recv (executed by I/O thread)
  worker->post_recv(client);

  // 3. Main loop (runs on coroutine thread pool)
  while (!client->closing)
  {
    // Suspend: wait for I/O thread to signal recv completion
    co_yield false;

    // CPU-bound: Parse received data (yields occasionally for fairness)
    // ... parsing logic ...

    // CPU-bound: Find handlers for parsed requests
    // ... handler lookup ...

    // CPU-bound: Generate responses (yields before calling handler)
    // ... response generation ...

    // Tell I/O thread to send responses
    while (!client->requests_ready.empty() && !client->closing)
    {
      worker->post_send(client);
      co_yield false; // Wait for I/O thread to signal send completion
      
      if (client->served >= client->keepalive_limit)
      {
        client->closing = true;
        break;
      }
    }

    // Continue loop: post next recv
    if (!client->closing)
    {
      worker->post_recv(client);
    }
  }

  // 4. Close connection via I/O thread
  worker->post_close(client->fd);
  co_return;
}
```

### 5. Flow of a Request

```
[I/O Thread]                          [Coroutine Pool Thread]
     │                                        │
  Accept ────────────────────────────────────>│ Spawn coroutine
     │                                        │ Post recv
     │                                        │ Suspend (co_yield false)
     │                                        │
  io_uring recv                              │ (waiting)
     │                                        │
  Recv complete                               │
     │                                        │
  signal_ready() ────────────────────────────>│ Resume!
     │                                        │ Parse data
     │                                        │ (yield occasionally)
     │                                        │ Find handler
     │                                        │ Generate response
     │                                        │ Post send
     │<─────────────────────────────────────── Suspend (co_yield false)
     │                                        │
  io_uring send                              │ (waiting)
     │                                        │
  Send complete                               │
     │                                        │
  signal_ready() ────────────────────────────>│ Resume!
     │                                        │ Check keepalive
     │                                        │ Post recv or close
     │<───────────────────────────────────────│
     │                                        │
  (loop continues)
```

### 6. Modified/Created Files

#### `lib/include/server/server.hpp`
- Added `ThreadPool coro_pool_` (N-1 threads)
- Changed to single `Worker` instance instead of vector
- Removed multi-listener/multi-worker setup
- Added `get_coro_pool()` accessor for Worker to use
- `run()` method moved to implementation file

#### `lib/src/server/server.cpp` (NEW)
- Implements `Server::run()`
- Creates single listening socket
- Creates single Worker instance
- Worker runs on main thread (could be on dedicated thread if needed)

#### `lib/include/server/worker.hpp`
- Removed `ThreadPool coro_pool_` member (uses Server's pool now)
- Worker is now a single-instance I/O handler

#### `lib/src/server/linux/worker.cpp`
- Removed coroutine pool initialization
- Uses `owner->get_coro_pool()` to spawn and signal coroutines
- `handle_accept()`: spawns client coroutine on Server's pool
- `handle_recv()`: signals coroutine via Server's pool
- `handle_send()`: signals coroutine via Server's pool
- Removed internal event handling (Parse, FindHandler, GenerateResponse)

#### `lib/include/common/per_client_storage.hpp`
- Added `std::optional<std::coroutine_handle<Task::promise_type>> coro_handle`
- Removed workflow callback methods

#### `lib/src/common/per_client_storage.cpp`
- Simplified to only contain `reset()` method
- All workflow logic moved to coroutine

#### `lib/include/server/client_workflow.hpp` (NEW)
- Declares `client_workflow_coro()` function

#### `lib/src/server/linux/client_workflow.cpp` (NEW)
- Implements complete client workflow as a coroutine
- Handles parsing, handler lookup, response generation, and sending
- Yields periodically during CPU-intensive operations
- **Runs on the coroutine thread pool (N-1 threads)**

## Benefits

### 1. **Better Resource Utilization**
- Single I/O thread efficiently monitors all connections via one io_uring
- N-1 threads dedicated to CPU-bound work (parsing, handlers, response generation)
- No idle I/O threads waiting for work

### 2. **Improved Scalability**
- Single io_uring can handle more connections efficiently
- CPU threads process work from any connection
- Better load balancing across CPU cores

### 3. **Clearer Separation of Concerns**
- I/O thread: purely event monitoring and signaling
- Coroutine threads: purely computation
- Easier to reason about and debug

### 4. **Improved Readability**
- Linear control flow from accept to close
- Easy to understand the complete lifecycle of a client connection
- No need to trace through multiple callback functions

### 5. **Better Fairness**
- Coroutines yield during long-running operations (parsing, response generation)
- Other clients can make progress while one is processing
- Prevents client starvation under high load

### 6. **Simplified Synchronization**
- I/O thread owns all io_uring operations
- Coroutines communicate with I/O thread via post_* methods
- Thread pool handles coroutine scheduling automatically

### 7. **Easier to Extend**
- Adding new workflow steps is straightforward
- Just add code to the coroutine in the appropriate place
- No need to add new workflow enum values and handlers

## Workflow Diagram

```
Accept Connection
    ↓
Spawn Coroutine ─────────────────────────────┐
    ↓                                        │
Post Recv                                    │
    ↓                                        │
Suspend (co_yield false) ←──────────┐        │
    ↓                               │        │
I/O Completion Event                │        │
    ↓                               │        │
Signal Coroutine Ready              │        │
    ↓                               │        │
Resume Coroutine                    │        │
    ↓                               │        │
Parse Data (yield periodically)     │        │
    ↓                               │        │
Find Handler                        │        │
    ↓                               │        │
Generate Response (yield periodically)       │
    ↓                               │        │
Post Send                           │        │
    ↓                               │        │
Suspend (co_yield false)            │        │
    ↓                               │        │
Send Completion Event               │        │
    ↓                               │        │
Signal Coroutine Ready              │        │
    ↓                               │        │
Resume Coroutine                    │        │
    ↓                               │        │
Check Keepalive Limit               │        │
    ↓                               │        │
More requests? ──Yes─→ Post Recv────┘        │
    │                                        │
   No                                        │
    ↓                                        │
Close Connection                             │
    ↓                                        │
Coroutine Completes (co_return) ─────────────┘
```

## Testing

The refactored server should maintain the same external behavior:
- HTTP/1.1 keep-alive support
- Multiple concurrent connections
- Request parsing and routing
- Response generation and sending

Test with:
```bash
# Build
cd build && make -j$(nproc)

# Run server
./bin/iocontext_http_server

# Test with curl
curl http://localhost:8080/

# Load test
wrk -t4 -c100 -d30s http://localhost:8080/
```

## Future Enhancements

1. **Async Handler Execution**: Allow request handlers to be coroutines themselves
2. **Streaming Responses**: Generate and send response chunks incrementally
3. **Pipelined Requests**: Process multiple requests from the same client concurrently
4. **HTTP/2 Support**: Leverage coroutines for multiplexed stream handling
5. **Timeouts**: Add timeout support using coroutine-aware timers

## Performance Considerations

- **Single-threaded pool per worker**: Avoids synchronization overhead
- **Cooperative multitasking**: Coroutines yield voluntarily, no preemption overhead
- **Zero-copy I/O**: Still uses io_uring's efficient buffer management
- **Minimal allocations**: Coroutine frames allocated once per connection

## Migration Notes

If you need to maintain backward compatibility or migrate gradually:
1. Keep both implementations in separate files
2. Use preprocessor flags to switch between them
3. Run both in parallel during testing
4. Gradually migrate clients to the coroutine version
