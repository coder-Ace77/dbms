#include "server.h"

#include <iostream>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <algorithm>

#define MAX_EVENTS 64
#define READ_BUF_SIZE 8192

// ANSI
#define S_GREEN  "\033[32m"
#define S_CYAN   "\033[36m"
#define S_DIM    "\033[2m"
#define S_RESET  "\033[0m"

// Global pointer for signal handler
static Server* g_server_instance = nullptr;

static void sigint_handler(int) {
    if (g_server_instance) g_server_instance->Stop();
}

// ============================================================================
// Constructor / Destructor
// ============================================================================

Server::Server(const std::string& db_file, int port)
    : server_fd_(-1), epoll_fd_(-1), port_(port), running_(false) {

    DBConfigs config;
    config.db_file_name = db_file;
    config.page_size = 4096;

    disk_manager_ = std::make_unique<DiskManager>(config);
    bpm_ = std::make_unique<BufferPoolManager>(256, disk_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get());

    // Reserve page 0 / load catalog
    if (disk_manager_->GetFileSize() == 0) {
        page_id_t pid;
        Page* p = bpm_->NewPage(&pid);
        if (p) {
            std::memset(p->GetData(), 0, 4096);
            bpm_->UnpinPage(pid, true);
        }
    } else {
        catalog_->LoadCatalog();
    }
}

Server::~Server() {
    catalog_->SaveCatalog();
    bpm_->FlushAllPages();

    for (auto& [fd, _] : client_buffers_) {
        close(fd);
    }
    if (epoll_fd_ != -1) close(epoll_fd_);
    if (server_fd_ != -1) close(server_fd_);
}

// ============================================================================
// Non-blocking helper
// ============================================================================

void Server::SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ============================================================================
// Start — bind, listen, epoll event loop
// ============================================================================

void Server::Start() {
    // Register signal handlers
    g_server_instance = this;
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    // Create socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    // SO_REUSEADDR
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port_));
    }

    // Listen
    if (listen(server_fd_, 128) < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to listen");
    }

    SetNonBlocking(server_fd_);

    // Create epoll
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to create epoll");
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev);

    running_ = true;

    std::cout << S_GREEN "DocDB Server started on port " << port_ << S_RESET << std::endl;
    std::cout << S_DIM "Waiting for connections..." S_RESET << std::endl;

    // Event loop
    struct epoll_event events[MAX_EVENTS];
    while (running_) {
        int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);  // 1s timeout
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_fd_) {
                AcceptConnection();
            } else {
                HandleClient(events[i].data.fd);
            }
        }
    }

    // Cleanup
    catalog_->SaveCatalog();
    bpm_->FlushAllPages();
    std::cout << S_GREEN "\nServer stopped. Data saved." S_RESET << std::endl;
}

void Server::Stop() {
    running_ = false;
}

// ============================================================================
// AcceptConnection
// ============================================================================

void Server::AcceptConnection() {
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &len);
        if (client_fd < 0) break;  // No more pending connections

        SetNonBlocking(client_fd);

        // TCP_NODELAY for low latency
        int flag = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;  // Edge-triggered
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

        client_buffers_[client_fd] = ClientBuffer{};
    }
}

// ============================================================================
// HandleClient — read data, frame messages, process commands
// ============================================================================

void Server::HandleClient(int client_fd) {
    char buf[READ_BUF_SIZE];

    while (true) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                // Client disconnected
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                close(client_fd);
                client_buffers_.erase(client_fd);
                return;
            }
            break;  // EAGAIN — no more data right now
        }

        client_buffers_[client_fd].data.append(buf, n);
    }

    // Process complete messages from the buffer
    auto& buffer = client_buffers_[client_fd].data;

    while (buffer.size() >= 4) {
        // Read 4-byte big-endian length prefix
        uint32_t msg_len = 0;
        msg_len |= (static_cast<uint8_t>(buffer[0]) << 24);
        msg_len |= (static_cast<uint8_t>(buffer[1]) << 16);
        msg_len |= (static_cast<uint8_t>(buffer[2]) << 8);
        msg_len |= (static_cast<uint8_t>(buffer[3]));

        if (msg_len > 1024 * 1024) {
            // Protect against absurd messages
            close(client_fd);
            client_buffers_.erase(client_fd);
            return;
        }

        if (buffer.size() < 4 + msg_len) break;  // Incomplete message

        std::string request_json = buffer.substr(4, msg_len);
        buffer.erase(0, 4 + msg_len);

        // Process and send response
        std::string response = ProcessCommand(request_json);

        // Send length-prefixed response
        uint32_t resp_len = static_cast<uint32_t>(response.size());
        uint8_t header[4];
        header[0] = (resp_len >> 24) & 0xFF;
        header[1] = (resp_len >> 16) & 0xFF;
        header[2] = (resp_len >> 8) & 0xFF;
        header[3] = resp_len & 0xFF;

        // Write header + payload (blocking is fine for responses)
        write(client_fd, header, 4);
        write(client_fd, response.data(), response.size());
    }
}

// ============================================================================
// JSON helpers — simple builders (we avoid pulling in a JSON lib)
// ============================================================================

static std::string EscapeJSON(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '"') out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\n') out += "\\n";
        else out += c;
    }
    return out;
}

std::string Server::DocToJSON(const BsonDocument& doc) {
    std::ostringstream ss;
    ss << "{";
    bool first = true;
    for (const auto& [key, val] : doc.elements) {
        if (!first) ss << ",";
        first = false;
        ss << "\"" << EscapeJSON(key) << "\":";
        if (std::holds_alternative<std::string>(val)) {
            ss << "\"" << EscapeJSON(std::get<std::string>(val)) << "\"";
        } else if (std::holds_alternative<int32_t>(val)) {
            ss << std::get<int32_t>(val);
        } else if (std::holds_alternative<int64_t>(val)) {
            ss << std::get<int64_t>(val);
        } else if (std::holds_alternative<double>(val)) {
            ss << std::get<double>(val);
        } else if (std::holds_alternative<bool>(val)) {
            ss << (std::get<bool>(val) ? "true" : "false");
        } else {
            ss << "null";
        }
    }
    ss << "}";
    return ss.str();
}

// Reuse the CLI's JSON parser logic (flat objects)
BsonDocument Server::ParseJSON(const std::string& json) {
    BsonDocument doc;
    std::string s = json;

    // Trim whitespace
    size_t start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return doc;
    s = s.substr(start);
    size_t end = s.find_last_not_of(" \t\n\r");
    s = s.substr(0, end + 1);

    if (s.empty() || s[0] != '{') return doc;
    s = s.substr(1, s.size() - 2);

    // Trim again
    start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return doc;
    s = s.substr(start);
    end = s.find_last_not_of(" \t\n\r");
    s = s.substr(0, end + 1);
    if (s.empty()) return doc;

    size_t pos = 0;
    while (pos < s.size()) {
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == ',' || s[pos] == '\t' || s[pos] == '\n')) pos++;
        if (pos >= s.size()) break;

        if (s[pos] != '"') break;
        pos++;
        size_t key_end = s.find('"', pos);
        if (key_end == std::string::npos) break;
        std::string key = s.substr(pos, key_end - pos);
        pos = key_end + 1;

        while (pos < s.size() && (s[pos] == ' ' || s[pos] == ':')) pos++;
        if (pos >= s.size()) break;

        if (s[pos] == '"') {
            pos++;
            size_t val_end = s.find('"', pos);
            if (val_end == std::string::npos) break;
            doc.Add(key, s.substr(pos, val_end - pos));
            pos = val_end + 1;
        } else if (s[pos] == '{') {
            // Nested object — find matching brace
            int depth = 0;
            size_t obj_start = pos;
            for (; pos < s.size(); pos++) {
                if (s[pos] == '{') depth++;
                if (s[pos] == '}') { depth--; if (depth == 0) { pos++; break; } }
            }
            std::string nested = s.substr(obj_start, pos - obj_start);
            auto sub = std::make_shared<BsonDocument>(ParseJSON(nested));
            doc.Add(key, sub);
        } else if (s[pos] == 't' || s[pos] == 'f') {
            if (s.substr(pos, 4) == "true") { doc.Add(key, true); pos += 4; }
            else if (s.substr(pos, 5) == "false") { doc.Add(key, false); pos += 5; }
        } else if (s[pos] == '-' || std::isdigit(s[pos])) {
            size_t num_start = pos;
            bool is_double = false;
            if (s[pos] == '-') pos++;
            while (pos < s.size() && (std::isdigit(s[pos]) || s[pos] == '.')) {
                if (s[pos] == '.') is_double = true;
                pos++;
            }
            std::string num_str = s.substr(num_start, pos - num_start);
            if (is_double) {
                doc.Add(key, std::stod(num_str));
            } else {
                int64_t v = std::stoll(num_str);
                if (v >= INT32_MIN && v <= INT32_MAX) doc.Add(key, static_cast<int32_t>(v));
                else doc.Add(key, v);
            }
        }
    }
    return doc;
}

// ============================================================================
// ProcessCommand — route a JSON request to the engine
// ============================================================================

std::string Server::ProcessCommand(const std::string& request_json) {
    try {
        BsonDocument req = ParseJSON(request_json);

        auto cmd_it = req.elements.find("cmd");
        if (cmd_it == req.elements.end() || !std::holds_alternative<std::string>(cmd_it->second)) {
            return R"({"ok":false,"error":"missing 'cmd' field"})";
        }
        std::string cmd = std::get<std::string>(cmd_it->second);

        // ---- ping ----
        if (cmd == "ping") {
            return R"({"ok":true,"result":"pong"})";
        }

        // ---- listCollections ----
        if (cmd == "listCollections") {
            auto names = catalog_->ListCollections();
            std::ostringstream ss;
            ss << R"({"ok":true,"result":[)";
            for (size_t i = 0; i < names.size(); i++) {
                if (i > 0) ss << ",";
                ss << "\"" << EscapeJSON(names[i]) << "\"";
            }
            ss << "]}";
            return ss.str();
        }

        // ---- createCollection ----
        if (cmd == "createCollection") {
            auto it = req.elements.find("name");
            if (it == req.elements.end()) return R"({"ok":false,"error":"missing 'name'"})";
            std::string name = std::get<std::string>(it->second);
            bool ok = catalog_->CreateCollection(name);
            if (ok) {
                catalog_->SaveCatalog();
                bpm_->FlushAllPages();
                return R"({"ok":true})";
            }
            return R"({"ok":false,"error":"collection already exists"})";
        }

        // ---- dropCollection ----
        if (cmd == "dropCollection") {
            auto it = req.elements.find("name");
            if (it == req.elements.end()) return R"({"ok":false,"error":"missing 'name'"})";
            std::string name = std::get<std::string>(it->second);
            catalog_->DropCollection(name);
            catalog_->SaveCatalog();
            bpm_->FlushAllPages();
            return R"({"ok":true})";
        }

        // Commands that need a collection
        auto coll_it = req.elements.find("collection");
        if (coll_it == req.elements.end() || !std::holds_alternative<std::string>(coll_it->second)) {
            return R"({"ok":false,"error":"missing 'collection' field"})";
        }
        std::string coll_name = std::get<std::string>(coll_it->second);

        // Auto-create collection if it doesn't exist
        if (!catalog_->GetCollection(coll_name)) {
            catalog_->CreateCollection(coll_name);
        }
        CollectionInfo* coll = catalog_->GetCollection(coll_name);
        if (!coll) return R"({"ok":false,"error":"collection not found"})";

        // ---- insert ----
        if (cmd == "insert") {
            auto doc_it = req.elements.find("document");
            if (doc_it == req.elements.end()) return R"({"ok":false,"error":"missing 'document'"})";

            // The document is stored as a nested BsonDocument
            BsonDocument insert_doc;
            if (std::holds_alternative<std::shared_ptr<BsonDocument>>(doc_it->second)) {
                insert_doc = *std::get<std::shared_ptr<BsonDocument>>(doc_it->second);
            } else {
                return R"({"ok":false,"error":"'document' must be an object"})";
            }

            RecordID rid = coll->heap_file->InsertRecord(insert_doc);

            // Update indexes
            for (auto& idx : coll->indexes) {
                auto fit = insert_doc.elements.find(idx.field_name);
                if (fit != insert_doc.elements.end()) {
                    std::string key_str;
                    if (std::holds_alternative<std::string>(fit->second))
                        key_str = std::get<std::string>(fit->second);
                    else if (std::holds_alternative<int32_t>(fit->second))
                        key_str = std::to_string(std::get<int32_t>(fit->second));
                    if (!key_str.empty()) idx.btree->Insert(key_str, rid);
                }
            }

            std::ostringstream ss;
            ss << R"({"ok":true,"page":)" << rid.page_id << R"(,"slot":)" << rid.slot_id << "}";

            bpm_->FlushAllPages();
            return ss.str();
        }

        // ---- find ----
        if (cmd == "find") {
            std::vector<Predicate> predicates;
            auto filter_it = req.elements.find("filter");
            if (filter_it != req.elements.end() &&
                std::holds_alternative<std::shared_ptr<BsonDocument>>(filter_it->second)) {
                auto& filter_doc = *std::get<std::shared_ptr<BsonDocument>>(filter_it->second);
                for (auto& [k, v] : filter_doc.elements) {
                    Predicate p;
                    p.field_name = k;
                    p.op = CompareOp::EQ;
                    p.value = v;
                    predicates.push_back(p);
                }
            }

            std::ostringstream ss;
            ss << R"({"ok":true,"result":[)";

            if (predicates.empty()) {
                SeqScanExecutor scan(coll->heap_file.get());
                scan.Init();
                Tuple tuple;
                bool first = true;
                while (scan.Next(&tuple)) {
                    if (!first) ss << ",";
                    first = false;
                    ss << DocToJSON(tuple.doc);
                }
                scan.Close();
            } else {
                auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
                FilterExecutor filter(std::move(child), predicates);
                filter.Init();
                Tuple tuple;
                bool first = true;
                while (filter.Next(&tuple)) {
                    if (!first) ss << ",";
                    first = false;
                    ss << DocToJSON(tuple.doc);
                }
                filter.Close();
            }

            ss << "]}";
            return ss.str();
        }

        // ---- count ----
        if (cmd == "count") {
            SeqScanExecutor scan(coll->heap_file.get());
            scan.Init();
            Tuple tuple;
            int count = 0;
            while (scan.Next(&tuple)) count++;
            scan.Close();

            std::ostringstream ss;
            ss << R"({"ok":true,"count":)" << count << "}";
            return ss.str();
        }

        // ---- delete ----
        if (cmd == "delete") {
            auto filter_it = req.elements.find("filter");
            if (filter_it == req.elements.end())
                return R"({"ok":false,"error":"missing 'filter'"})";

            std::vector<Predicate> predicates;
            if (std::holds_alternative<std::shared_ptr<BsonDocument>>(filter_it->second)) {
                auto& filter_doc = *std::get<std::shared_ptr<BsonDocument>>(filter_it->second);
                for (auto& [k, v] : filter_doc.elements) {
                    Predicate p;
                    p.field_name = k;
                    p.op = CompareOp::EQ;
                    p.value = v;
                    predicates.push_back(p);
                }
            }

            auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
            FilterExecutor filter(std::move(child), predicates);
            filter.Init();

            std::vector<RecordID> to_delete;
            Tuple tuple;
            while (filter.Next(&tuple)) to_delete.push_back(tuple.rid);
            filter.Close();

            int deleted = 0;
            for (auto& rid : to_delete) {
                if (coll->heap_file->DeleteRecord(rid)) deleted++;
            }

            std::ostringstream ss;
            ss << R"({"ok":true,"deleted":)" << deleted << "}";

            bpm_->FlushAllPages();
            return ss.str();
        }

        // ---- update ----
        if (cmd == "update") {
            auto filter_it = req.elements.find("filter");
            auto update_it = req.elements.find("update");
            if (filter_it == req.elements.end() || update_it == req.elements.end())
                return R"({"ok":false,"error":"missing 'filter' or 'update'"})";

            std::vector<Predicate> predicates;
            BsonDocument update_doc;
            if (std::holds_alternative<std::shared_ptr<BsonDocument>>(filter_it->second)) {
                auto& fd = *std::get<std::shared_ptr<BsonDocument>>(filter_it->second);
                for (auto& [k, v] : fd.elements) {
                    Predicate p; p.field_name = k; p.op = CompareOp::EQ; p.value = v;
                    predicates.push_back(p);
                }
            }
            if (std::holds_alternative<std::shared_ptr<BsonDocument>>(update_it->second)) {
                update_doc = *std::get<std::shared_ptr<BsonDocument>>(update_it->second);
            }

            auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
            FilterExecutor filter(std::move(child), predicates);
            filter.Init();

            std::vector<std::pair<RecordID, BsonDocument>> to_update;
            Tuple tuple;
            while (filter.Next(&tuple)) {
                BsonDocument merged = tuple.doc;
                for (auto& [k, v] : update_doc.elements) merged.elements[k] = v;
                to_update.push_back({tuple.rid, merged});
            }
            filter.Close();

            int updated = 0;
            for (auto& [rid, new_doc] : to_update) {
                coll->heap_file->UpdateRecord(rid, new_doc);
                updated++;
            }

            std::ostringstream ss;
            ss << R"({"ok":true,"updated":)" << updated << "}";

            bpm_->FlushAllPages();
            return ss.str();
        }

        // ---- createIndex ----
        if (cmd == "createIndex") {
            auto field_it = req.elements.find("field");
            if (field_it == req.elements.end())
                return R"({"ok":false,"error":"missing 'field'"})";
            std::string field = std::get<std::string>(field_it->second);
            catalog_->CreateIndex(coll_name, field);
            catalog_->SaveCatalog();
            bpm_->FlushAllPages();
            return R"({"ok":true})";
        }

        return R"({"ok":false,"error":"unknown command: )" + cmd + "\"}";

    } catch (const std::exception& e) {
        return std::string(R"({"ok":false,"error":")") + EscapeJSON(e.what()) + "\"}";
    }
}
