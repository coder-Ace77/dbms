#pragma once

#include "executor.h"
#include <memory>
#include <vector>

// ============================================================================
// Filter — wraps a child executor and applies predicates
// ============================================================================
class FilterExecutor : public Executor {
public:
    FilterExecutor(std::unique_ptr<Executor> child, std::vector<Predicate> predicates);

    void Init() override;
    bool Next(Tuple* tuple) override;
    void Close() override;

private:
    std::unique_ptr<Executor> child_;
    std::vector<Predicate> predicates_;
};
