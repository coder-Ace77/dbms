#pragma once

#include "storage_engine/common/bson_types.h"
#include "storage_engine/page/slotted_page.h"
#include <functional>
#include <string>

// ============================================================================
// Tuple — a single result row from the executor pipeline
// ============================================================================
struct Tuple {
    RecordID rid;
    BsonDocument doc;
};

// ============================================================================
// Executor — abstract Volcano Iterator interface
//
//   Init()  → prepare for iteration
//   Next()  → return the next tuple, false when done
//   Close() → clean up
// ============================================================================
class Executor {
public:
    virtual ~Executor() = default;
    virtual void Init() = 0;
    virtual bool Next(Tuple* tuple) = 0;
    virtual void Close() = 0;
};

// ============================================================================
// Predicate — a simple field comparison for filtering
// ============================================================================
enum class CompareOp {
    EQ,   // ==
    NE,   // !=
    LT,   // <
    LE,   // <=
    GT,   // >
    GE    // >=
};

struct Predicate {
    std::string field_name;
    CompareOp op;
    BsonValue value;

    // Evaluate the predicate against a document
    bool Evaluate(const BsonDocument& doc) const;
};
