#ifndef CLIENT_WORKFLOW_H
#define CLIENT_WORKFLOW_H

#include "common/coroutine_thread_pool.hpp"
#include "common/per_client_storage.hpp"
#include "common/workflow.hpp"
#include "server/worker.hpp"

// Forward declarations
class Worker;
class PerClientStorage;

// Coroutine that manages the entire client lifecycle
Task client_workflow_coro(Worker* worker, PerClientStorage* client);

#endif // CLIENT_WORKFLOW_H
