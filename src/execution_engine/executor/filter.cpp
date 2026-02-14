#include "filter.h"

// ============================================================================
// FilterExecutor
// ============================================================================

FilterExecutor::FilterExecutor(std::unique_ptr<Executor> child, std::vector<Predicate> predicates)
    : child_(std::move(child)), predicates_(std::move(predicates)) {}

void FilterExecutor::Init() {
    child_->Init();
}

bool FilterExecutor::Next(Tuple* tuple) {
    while (child_->Next(tuple)) {
        // Check all predicates (AND logic)
        bool pass = true;
        for (const auto& pred : predicates_) {
            if (!pred.Evaluate(tuple->doc)) {
                pass = false;
                break;
            }
        }
        if (pass) return true;
    }
    return false;
}

void FilterExecutor::Close() {
    child_->Close();
}
