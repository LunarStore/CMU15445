#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
    child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    child_executor_->Init();
    Tuple tp;
    RID rid;
    std::priority_queue<TopNKey, 
        std::vector<TopNKey>, TopNKey> tmep;
    std::stack<TopNKey>().swap(stack_);

    while(child_executor_->Next(&tp, &rid)) {
        TopNKey tk = MakeTopNKey(&tp, &rid);
        TopNKey cmp;

        if(tmep.size() < plan_->GetN()) {
            tmep.push(tk);
        } else if (!cmp(tmep.top(), tk)) {
            tmep.pop();
            tmep.push(tk);
        }
    }

    while (!tmep.empty()) {
        TopNKey tk = tmep.top();
        tmep.pop();
        stack_.push(tk);
    }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while(!stack_.empty()) {
        TopNKey tk = stack_.top();
        stack_.pop();
        *tuple = tk.tuple_;
        *rid = tk.rid_;

        return true;
    }
    return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return stack_.size();
};

}  // namespace bustub
