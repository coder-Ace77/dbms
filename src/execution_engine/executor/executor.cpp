#include "execution_engine/executor/executor.h"
#include <iostream>

// ============================================================================
// Predicate::Evaluate
// ============================================================================

bool Predicate::Evaluate(const BsonDocument& doc) const {
    auto it = doc.elements.find(field_name);
    if (it == doc.elements.end()) {
        return false;  // Field not found
    }

    const BsonValue& doc_val = it->second;

    // Compare strings
    if (std::holds_alternative<std::string>(value) && std::holds_alternative<std::string>(doc_val)) {
        const std::string& a = std::get<std::string>(doc_val);
        const std::string& b = std::get<std::string>(value);
        switch (op) {
            case CompareOp::EQ: return a == b;
            case CompareOp::NE: return a != b;
            case CompareOp::LT: return a < b;
            case CompareOp::LE: return a <= b;
            case CompareOp::GT: return a > b;
            case CompareOp::GE: return a >= b;
        }
    }

    // Compare int32
    if (std::holds_alternative<int32_t>(value) && std::holds_alternative<int32_t>(doc_val)) {
        int32_t a = std::get<int32_t>(doc_val);
        int32_t b = std::get<int32_t>(value);
        switch (op) {
            case CompareOp::EQ: return a == b;
            case CompareOp::NE: return a != b;
            case CompareOp::LT: return a < b;
            case CompareOp::LE: return a <= b;
            case CompareOp::GT: return a > b;
            case CompareOp::GE: return a >= b;
        }
    }

    // Compare int64
    if (std::holds_alternative<int64_t>(value) && std::holds_alternative<int64_t>(doc_val)) {
        int64_t a = std::get<int64_t>(doc_val);
        int64_t b = std::get<int64_t>(value);
        switch (op) {
            case CompareOp::EQ: return a == b;
            case CompareOp::NE: return a != b;
            case CompareOp::LT: return a < b;
            case CompareOp::LE: return a <= b;
            case CompareOp::GT: return a > b;
            case CompareOp::GE: return a >= b;
        }
    }

    // Compare double
    if (std::holds_alternative<double>(value) && std::holds_alternative<double>(doc_val)) {
        double a = std::get<double>(doc_val);
        double b = std::get<double>(value);
        switch (op) {
            case CompareOp::EQ: return a == b;
            case CompareOp::NE: return a != b;
            case CompareOp::LT: return a < b;
            case CompareOp::LE: return a <= b;
            case CompareOp::GT: return a > b;
            case CompareOp::GE: return a >= b;
        }
    }

    // Compare bool (only EQ/NE make sense)
    if (std::holds_alternative<bool>(value) && std::holds_alternative<bool>(doc_val)) {
        bool a = std::get<bool>(doc_val);
        bool b = std::get<bool>(value);
        switch (op) {
            case CompareOp::EQ: return a == b;
            case CompareOp::NE: return a != b;
            default: return false;
        }
    }

    return false;  // Type mismatch
}
