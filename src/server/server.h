#pragma once

#include "execution_engine/catalog/catalog.h"
#include "execution_engine/executor/executor.h"
#include "execution_engine/executor/seq_scan.h"
#include "execution_engine/executor/filter.h"
#include "execution_engine/executor/index_scan.h"
#include "storage_engine/buffer/buffer_pool.h"
#include "storage_engine/disk_manager/disk_manager.h"
#include "storage_engine/serializer/serializer.h"

#include <string>
#include <memory>
#include <atomic>
#include <functional>

// ============================================================================
// TCP Server — epoll-based, non-blocking, single-threaded event loop
//
// Wire protocol (length-prefixed JSON):
//   Request:   [4 bytes big-endian length] [JSON payload]
//   Response:  [4 bytes big-endian length] [JSON payload]
//
// Request JSON:
//   { "cmd": "insert", "collection": "users", "document": {...} }
//   { "cmd": "find",   "collection": "users", "filter": {...} }
//   { "cmd": "delete", "collection": "users", "filter": {...} }
//   { "cmd": "update", "collection": "users", "filter": {...}, "update": {...} }
//   { "cmd": "count",  "collection": "users" }
//   { "cmd": "createCollection", "name": "users" }
//   { "cmd": "dropCollection",   "name": "users" }
//   { "cmd": "createIndex", "collection": "users", "field": "name" }
//   { "cmd": "listCollections" }
//   { "cmd": "ping" }
//
// Response JSON:
//   { "ok": true, "result": ... }
//   { "ok": false, "error": "..." }
// ============================================================================

class Server {
public:
    Server(const std::string& db_file, int port = 6379);
    ~Server();

    // Start the server (blocks until stopped)
    void Start();

    // Stop the server
    void Stop();

private:
    // Set a socket to non-blocking mode
    void SetNonBlocking(int fd);

    // Handle a new connection
    void AcceptConnection();

    // Read data from a client
    void HandleClient(int client_fd);

    // Process a complete request and return response JSON
    std::string ProcessCommand(const std::string& request_json);

    // ---- JSON helpers (reuse BsonDocument as quick parser) ----
    BsonDocument ParseJSON(const std::string& json);
    std::string DocToJSON(const BsonDocument& doc);

    // ---- Engine components ----
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<Catalog> catalog_;

    // ---- Network state ----
    int server_fd_;
    int epoll_fd_;
    int port_;
    std::atomic<bool> running_;

    // ---- Per-client read buffers ----
    struct ClientBuffer {
        std::string data;
    };
    std::unordered_map<int, ClientBuffer> client_buffers_;
};
