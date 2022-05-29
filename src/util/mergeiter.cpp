/*
 *
 */

#include "util/mergeiter.hpp"

namespace lsm { namespace iter {

MergeIterator::MergeIterator(std::vector<std::unique_ptr<GenericIterator<Record>>> &iters_to_merge, const CompareFunc cmp)
{
    this->cmp = HeapCompareFunc{cmp};
    this->merge_heap = std::make_unique<std::priority_queue<std::pair<Record, size_t>, std::vector<std::pair<Record, size_t>>, HeapCompareFunc>>(this->cmp);
    this->iterators = std::move(iters_to_merge);

    // load the first record from each iterator into the priority queue.
    for (size_t i=0; i<this->iterators.size(); i++) {
        if (iterators[i]->next()) {
            auto rec = iterators[i]->get_item();
            this->merge_heap->push({rec, i});
        }
    }

    this->at_end = this->merge_heap->size() == 0;
    this->current_record = Record();
}


bool MergeIterator::next()
{
    while (!this->merge_heap->empty()) {
        auto next = this->merge_heap->top(); this->merge_heap->pop(); 
        this->current_record = next.first;

        if (this->iterators[next.second]->next()) {
            this->merge_heap->push({iterators[next.second]->get_item(), next.second});
        } 

        return true;
    } 

    this->at_end = true;
    return false;
}


Record MergeIterator::get_item()
{
    return this->current_record;
}


bool MergeIterator::supports_rewind()
{
    return false;
}


IteratorPosition MergeIterator::save_position()
{
    return 0;
}


void MergeIterator::rewind(IteratorPosition position)
{
    return;
}



void MergeIterator::end_scan()
{
    for (size_t i=0; i<this->iterators.size(); i++) {
        this->iterators[i]->end_scan();
    }
}


MergeIterator::~MergeIterator()
{
    this->end_scan();
}

}}
