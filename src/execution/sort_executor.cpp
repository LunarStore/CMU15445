#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
    child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    child_executor_->Init();
    Tuple tp;
    RID rid;
    std::priority_queue<SortKey, 
        std::vector<SortKey>, SortKey> tmp;
    data_.swap(tmp);
    while(child_executor_->Next(&tp, &rid)) {
        SortKey sk = MakeSortKey(&tp, &rid);

        data_.push(sk);
    }
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while(!data_.empty()) {
        SortKey sk = data_.top();
        data_.pop();
        *tuple = sk.tuple_;
        *rid = sk.rid_;

        return true;
    }
    return false;
}

}  // namespace bustub
