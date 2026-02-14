#include "cli.h"
#include <iostream>
#include <algorithm>
#include <cctype>
#include <regex>

// ANSI color codes
#define CLR_RESET   "\033[0m"
#define CLR_BOLD    "\033[1m"
#define CLR_GREEN   "\033[32m"
#define CLR_CYAN    "\033[36m"
#define CLR_YELLOW  "\033[33m"
#define CLR_RED     "\033[31m"
#define CLR_MAGENTA "\033[35m"
#define CLR_DIM     "\033[2m"

// ============================================================================
// Constructor / Destructor
// ============================================================================

CLI::CLI(const std::string& db_file) : running_(true) {
    DBConfigs config;
    config.db_file_name = db_file;
    config.page_size = 4096;

    disk_manager_ = std::make_unique<DiskManager>(config);
    bpm_ = std::make_unique<BufferPoolManager>(128, disk_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get());
    lock_manager_ = std::make_unique<LockManager>();
    txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get());

    // Check if this is a fresh database (no pages on disk yet)
    if (disk_manager_->GetFileSize() == 0) {
        // Fresh DB — reserve page 0 for catalog metadata
        page_id_t pid;
        Page* p = bpm_->NewPage(&pid);  // pid will be 0
        if (p) {
            std::memset(p->GetData(), 0, 4096);
            bpm_->UnpinPage(pid, true);
        }
    } else {
        // Existing DB — load catalog from page 0
        catalog_->LoadCatalog();
    }
}

CLI::~CLI() {
    // Save catalog metadata and flush everything to disk
    catalog_->SaveCatalog();
    bpm_->FlushAllPages();
}

// ============================================================================
// Utility
// ============================================================================

std::string CLI::Trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

std::string CLI::ExtractBetween(const std::string& s, char open, char close) {
    size_t start = s.find(open);
    if (start == std::string::npos) return "";
    
    int depth = 0;
    size_t end = start;
    for (size_t i = start; i < s.size(); i++) {
        if (s[i] == open) depth++;
        if (s[i] == close) depth--;
        if (depth == 0) { end = i; break; }
    }
    
    if (end <= start) return "";
    return s.substr(start, end - start + 1);
}

// ============================================================================
// JSON Mini-Parser
// ============================================================================

BsonDocument CLI::ParseJSON(const std::string& json) {
    BsonDocument doc;
    std::string s = Trim(json);

    if (s.empty() || s[0] != '{') return doc;

    // Remove outer braces
    s = s.substr(1, s.size() - 2);
    s = Trim(s);
    if (s.empty()) return doc;

    // Parse key-value pairs
    size_t pos = 0;
    while (pos < s.size()) {
        // Skip whitespace and commas
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == ',' || s[pos] == '\t' || s[pos] == '\n')) pos++;
        if (pos >= s.size()) break;

        // Read key (must be quoted)
        if (s[pos] != '"') break;
        pos++;
        size_t key_end = s.find('"', pos);
        if (key_end == std::string::npos) break;
        std::string key = s.substr(pos, key_end - pos);
        pos = key_end + 1;

        // Skip colon
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == ':')) pos++;
        if (pos >= s.size()) break;

        // Read value
        if (s[pos] == '"') {
            // String value
            pos++;
            size_t val_end = s.find('"', pos);
            if (val_end == std::string::npos) break;
            std::string val = s.substr(pos, val_end - pos);
            doc.Add(key, val);
            pos = val_end + 1;
        } else if (s[pos] == 't' || s[pos] == 'f') {
            // Boolean
            if (s.substr(pos, 4) == "true") {
                doc.Add(key, true);
                pos += 4;
            } else if (s.substr(pos, 5) == "false") {
                doc.Add(key, false);
                pos += 5;
            }
        } else if (s[pos] == '-' || std::isdigit(s[pos])) {
            // Number — determine if int or double
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
                int64_t val = std::stoll(num_str);
                if (val >= INT32_MIN && val <= INT32_MAX) {
                    doc.Add(key, static_cast<int32_t>(val));
                } else {
                    doc.Add(key, val);
                }
            }
        } else if (s[pos] == '{') {
            // Nested document
            std::string nested = ExtractBetween(s.substr(pos), '{', '}');
            auto sub_doc = std::make_shared<BsonDocument>(ParseJSON(nested));
            doc.Add(key, sub_doc);
            pos += nested.size();
        }
    }

    return doc;
}

// ============================================================================
// Print Document
// ============================================================================

void CLI::PrintDoc(const BsonDocument& doc) {
    std::cout << CLR_DIM "{ " CLR_RESET;
    bool first = true;
    for (const auto& [key, val] : doc.elements) {
        if (!first) std::cout << CLR_DIM ", " CLR_RESET;
        first = false;
        std::cout << CLR_CYAN "\"" << key << "\"" CLR_RESET << ": ";
        if (std::holds_alternative<std::string>(val)) {
            std::cout << CLR_GREEN "\"" << std::get<std::string>(val) << "\"" CLR_RESET;
        } else if (std::holds_alternative<int32_t>(val)) {
            std::cout << CLR_YELLOW << std::get<int32_t>(val) << CLR_RESET;
        } else if (std::holds_alternative<int64_t>(val)) {
            std::cout << CLR_YELLOW << std::get<int64_t>(val) << CLR_RESET;
        } else if (std::holds_alternative<double>(val)) {
            std::cout << CLR_YELLOW << std::get<double>(val) << CLR_RESET;
        } else if (std::holds_alternative<bool>(val)) {
            std::cout << CLR_MAGENTA << (std::get<bool>(val) ? "true" : "false") << CLR_RESET;
        } else if (std::holds_alternative<std::shared_ptr<BsonDocument>>(val)) {
            PrintDoc(*std::get<std::shared_ptr<BsonDocument>>(val));
        } else {
            std::cout << "null";
        }
    }
    std::cout << CLR_DIM " }" CLR_RESET << std::endl;
}

// ============================================================================
// Parse predicates from a filter document (all fields become EQ predicates)
// ============================================================================

std::vector<Predicate> CLI::ParseFilter(const BsonDocument& filter_doc) {
    std::vector<Predicate> predicates;
    for (const auto& [key, val] : filter_doc.elements) {
        Predicate pred;
        pred.field_name = key;
        pred.op = CompareOp::EQ;
        pred.value = val;
        predicates.push_back(pred);
    }
    return predicates;
}

// ============================================================================
// Command Handlers
// ============================================================================

void CLI::HandleShowCollections() {
    auto names = catalog_->ListCollections();
    if (names.empty()) {
        std::cout << CLR_DIM "  (no collections)" CLR_RESET << std::endl;
        return;
    }
    for (const auto& name : names) {
        std::cout << "  " CLR_CYAN << name << CLR_RESET << std::endl;
    }
}

void CLI::HandleUse(const std::string& collection_name) {
    std::string name = Trim(collection_name);
    if (name.empty()) {
        std::cout << CLR_RED "Error: collection name required" CLR_RESET << std::endl;
        return;
    }

    // Auto-create if it doesn't exist
    if (!catalog_->GetCollection(name)) {
        catalog_->CreateCollection(name);
    }
    current_collection_ = name;
    std::cout << CLR_GREEN "Switched to collection '" << name << "'" CLR_RESET << std::endl;
}

void CLI::HandleInsert(const std::string& json_str) {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected. Use 'use <name>' first." CLR_RESET << std::endl;
        return;
    }

    CollectionInfo* coll = catalog_->GetCollection(current_collection_);
    if (!coll) {
        std::cout << CLR_RED "Error: collection not found." CLR_RESET << std::endl;
        return;
    }

    BsonDocument doc = ParseJSON(json_str);
    if (doc.elements.empty()) {
        std::cout << CLR_RED "Error: invalid or empty document." CLR_RESET << std::endl;
        return;
    }

    try {
        RecordID rid = coll->heap_file->InsertRecord(doc);
        std::cout << CLR_GREEN "Inserted 1 document " CLR_RESET
                  << CLR_DIM "(page=" << rid.page_id << ", slot=" << rid.slot_id << ")" CLR_RESET << std::endl;

        // Update indexes
        for (auto& idx : coll->indexes) {
            auto it = doc.elements.find(idx.field_name);
            if (it != doc.elements.end()) {
                std::string key_str;
                if (std::holds_alternative<std::string>(it->second)) {
                    key_str = std::get<std::string>(it->second);
                } else if (std::holds_alternative<int32_t>(it->second)) {
                    key_str = std::to_string(std::get<int32_t>(it->second));
                }
                if (!key_str.empty()) {
                    idx.btree->Insert(key_str, rid);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cout << CLR_RED "Error: " << e.what() << CLR_RESET << std::endl;
    }
}

void CLI::HandleFind(const std::string& filter_str) {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    CollectionInfo* coll = catalog_->GetCollection(current_collection_);
    if (!coll) {
        std::cout << CLR_RED "Error: collection not found." CLR_RESET << std::endl;
        return;
    }

    try {
        if (filter_str.empty() || Trim(filter_str) == "{}") {
            // No filter — full scan
            SeqScanExecutor scan(coll->heap_file.get());
            scan.Init();
            Tuple tuple;
            int count = 0;
            while (scan.Next(&tuple)) {
                PrintDoc(tuple.doc);
                count++;
            }
            scan.Close();
            std::cout << CLR_DIM "(" << count << " documents)" CLR_RESET << std::endl;
        } else {
            // Filtered scan
            BsonDocument filter_doc = ParseJSON(filter_str);
            auto predicates = ParseFilter(filter_doc);

            auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
            FilterExecutor filter(std::move(child), predicates);
            filter.Init();
            Tuple tuple;
            int count = 0;
            while (filter.Next(&tuple)) {
                PrintDoc(tuple.doc);
                count++;
            }
            filter.Close();
            std::cout << CLR_DIM "(" << count << " documents)" CLR_RESET << std::endl;
        }
    } catch (const std::exception& e) {
        std::cout << CLR_RED "Error: " << e.what() << CLR_RESET << std::endl;
    }
}

void CLI::HandleDelete(const std::string& filter_str) {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    CollectionInfo* coll = catalog_->GetCollection(current_collection_);
    if (!coll) return;

    try {
        BsonDocument filter_doc = ParseJSON(filter_str);
        auto predicates = ParseFilter(filter_doc);

        // Find matching records first
        auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
        FilterExecutor filter(std::move(child), predicates);
        filter.Init();

        std::vector<RecordID> to_delete;
        Tuple tuple;
        while (filter.Next(&tuple)) {
            to_delete.push_back(tuple.rid);
        }
        filter.Close();

        // Delete them
        int deleted = 0;
        for (const auto& rid : to_delete) {
            if (coll->heap_file->DeleteRecord(rid)) {
                deleted++;
            }
        }

        std::cout << CLR_GREEN "Deleted " << deleted << " document(s)" CLR_RESET << std::endl;
    } catch (const std::exception& e) {
        std::cout << CLR_RED "Error: " << e.what() << CLR_RESET << std::endl;
    }
}

void CLI::HandleUpdate(const std::string& filter_str, const std::string& update_str) {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    CollectionInfo* coll = catalog_->GetCollection(current_collection_);
    if (!coll) return;

    try {
        BsonDocument filter_doc = ParseJSON(filter_str);
        BsonDocument update_doc = ParseJSON(update_str);
        auto predicates = ParseFilter(filter_doc);

        // Find matching records
        auto child = std::make_unique<SeqScanExecutor>(coll->heap_file.get());
        FilterExecutor filter(std::move(child), predicates);
        filter.Init();

        std::vector<std::pair<RecordID, BsonDocument>> to_update;
        Tuple tuple;
        while (filter.Next(&tuple)) {
            // Merge update fields into existing doc
            BsonDocument merged = tuple.doc;
            for (auto& [key, val] : update_doc.elements) {
                merged.elements[key] = val;
            }
            to_update.push_back({tuple.rid, merged});
        }
        filter.Close();

        int updated = 0;
        for (auto& [rid, new_doc] : to_update) {
            coll->heap_file->UpdateRecord(rid, new_doc);
            updated++;
        }

        std::cout << CLR_GREEN "Updated " << updated << " document(s)" CLR_RESET << std::endl;
    } catch (const std::exception& e) {
        std::cout << CLR_RED "Error: " << e.what() << CLR_RESET << std::endl;
    }
}

void CLI::HandleCreateIndex(const std::string& field_name) {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    std::string field = Trim(field_name);
    // Remove quotes if present
    if (field.size() >= 2 && field[0] == '"' && field.back() == '"') {
        field = field.substr(1, field.size() - 2);
    }

    if (catalog_->CreateIndex(current_collection_, field)) {
        std::cout << CLR_GREEN "Index created on '" << field << "'" CLR_RESET << std::endl;
    }
}

void CLI::HandleCount() {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    CollectionInfo* coll = catalog_->GetCollection(current_collection_);
    if (!coll) return;

    SeqScanExecutor scan(coll->heap_file.get());
    scan.Init();
    int count = 0;
    Tuple tuple;
    while (scan.Next(&tuple)) { count++; }
    scan.Close();

    std::cout << CLR_YELLOW << count << CLR_RESET << std::endl;
}

void CLI::HandleDrop() {
    if (current_collection_.empty()) {
        std::cout << CLR_RED "Error: no collection selected." CLR_RESET << std::endl;
        return;
    }

    std::string name = current_collection_;
    if (catalog_->DropCollection(name)) {
        std::cout << CLR_GREEN "Dropped collection '" << name << "'" CLR_RESET << std::endl;
        current_collection_.clear();
    }
}

void CLI::HandleHelp() {
    std::cout << CLR_BOLD "\n  DocDB Shell Commands\n" CLR_RESET << std::endl;
    std::cout << CLR_CYAN "  show collections" CLR_RESET "           — List all collections" << std::endl;
    std::cout << CLR_CYAN "  use <name>" CLR_RESET "                 — Switch to (or create) a collection" << std::endl;
    std::cout << CLR_CYAN "  db.insert({...})" CLR_RESET "           — Insert a JSON document" << std::endl;
    std::cout << CLR_CYAN "  db.find()" CLR_RESET "                  — Find all documents" << std::endl;
    std::cout << CLR_CYAN "  db.find({...})" CLR_RESET "             — Find with filter (equality match)" << std::endl;
    std::cout << CLR_CYAN "  db.delete({...})" CLR_RESET "           — Delete matching documents" << std::endl;
    std::cout << CLR_CYAN "  db.update({filter}, {doc})" CLR_RESET " — Update matching documents" << std::endl;
    std::cout << CLR_CYAN "  db.createIndex(\"field\")" CLR_RESET "    — Create B+ Tree index on a field" << std::endl;
    std::cout << CLR_CYAN "  db.count()" CLR_RESET "                 — Count documents in collection" << std::endl;
    std::cout << CLR_CYAN "  db.drop()" CLR_RESET "                  — Drop current collection" << std::endl;
    std::cout << CLR_CYAN "  help" CLR_RESET "                       — Show this help" << std::endl;
    std::cout << CLR_CYAN "  exit / quit" CLR_RESET "                — Exit the shell\n" << std::endl;
}

// ============================================================================
// Main REPL
// ============================================================================

void CLI::Run() {
    std::cout << CLR_BOLD CLR_GREEN;
    std::cout << R"(
    ____             ____  ____
   / __ \____  _____/ __ \/ _db_ )
  / / / / __ \/ ___/ / / / __  |
 / /_/ / /_/ / /__/ /_/ / /_/ /
/_____/\____/\___/_____/_____/
)";
    std::cout << CLR_RESET;
    std::cout << CLR_DIM "  Document Store Engine v1.0" CLR_RESET << std::endl;
    std::cout << CLR_DIM "  Type 'help' for commands, 'exit' to quit.\n" CLR_RESET << std::endl;

    std::string line;
    while (running_) {
        // Print prompt
        if (current_collection_.empty()) {
            std::cout << CLR_BOLD "docdb" CLR_RESET "> ";
        } else {
            std::cout << CLR_BOLD "docdb" CLR_RESET ":" CLR_CYAN << current_collection_ << CLR_RESET "> ";
        }

        if (!std::getline(std::cin, line)) {
            break;  // EOF
        }

        std::string cmd = Trim(line);
        if (cmd.empty()) continue;

        // ---- Route commands ----
        if (cmd == "exit" || cmd == "quit") {
            std::cout << CLR_GREEN "Saving data..." CLR_RESET << std::endl;
            running_ = false;

        } else if (cmd == "help") {
            HandleHelp();

        } else if (cmd == "show collections") {
            HandleShowCollections();

        } else if (cmd.substr(0, 3) == "use") {
            HandleUse(cmd.substr(3));

        } else if (cmd.substr(0, 10) == "db.insert(") {
            // Extract JSON between outer parens
            std::string inner = cmd.substr(10);
            if (!inner.empty() && inner.back() == ')') inner.pop_back();
            std::string json = ExtractBetween(inner, '{', '}');
            HandleInsert(json);

        } else if (cmd == "db.find()" || cmd == "db.find({})") {
            HandleFind("");

        } else if (cmd.substr(0, 8) == "db.find(") {
            std::string inner = cmd.substr(8);
            if (!inner.empty() && inner.back() == ')') inner.pop_back();
            std::string json = ExtractBetween(inner, '{', '}');
            HandleFind(json);

        } else if (cmd.substr(0, 10) == "db.delete(") {
            std::string inner = cmd.substr(10);
            if (!inner.empty() && inner.back() == ')') inner.pop_back();
            std::string json = ExtractBetween(inner, '{', '}');
            HandleDelete(json);

        } else if (cmd.substr(0, 10) == "db.update(") {
            // db.update({filter}, {update})
            std::string inner = cmd.substr(10);
            if (!inner.empty() && inner.back() == ')') inner.pop_back();

            // Find first JSON object
            std::string first_json = ExtractBetween(inner, '{', '}');
            // Find second JSON object after the first
            size_t after_first = inner.find(first_json) + first_json.size();
            std::string rest = inner.substr(after_first);
            std::string second_json = ExtractBetween(rest, '{', '}');

            if (first_json.empty() || second_json.empty()) {
                std::cout << CLR_RED "Usage: db.update({filter}, {newFields})" CLR_RESET << std::endl;
            } else {
                HandleUpdate(first_json, second_json);
            }

        } else if (cmd.substr(0, 15) == "db.createIndex(") {
            std::string inner = cmd.substr(15);
            if (!inner.empty() && inner.back() == ')') inner.pop_back();
            HandleCreateIndex(inner);

        } else if (cmd == "db.count()") {
            HandleCount();

        } else if (cmd == "db.drop()") {
            HandleDrop();

        } else {
            std::cout << CLR_RED "Unknown command: " CLR_RESET << cmd << std::endl;
            std::cout << CLR_DIM "Type 'help' for available commands." CLR_RESET << std::endl;
        }
    }
}
