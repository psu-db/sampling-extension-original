/*
*
*/

#ifndef H_MERGEITER
#define H_MERGEITER

#include <vector>
#include <queue>
#include <functional>

#include "catalog/schema.hpp"
#include "util/iterator.hpp"
#include "io/record.hpp"

using lsm::io::Record;

namespace lsm { namespace iter {


class MergeIterator : GenericIterator<io::Record> { 
public:
    /*
     * Create a new MergeIterator from the record iterators in iters_to_merge,
     * which returns elements one at a time from the input iters_to_merge,
     * until all records in all the iteraters have been returned exactly once,
     * in sorted order.
     *
     * Note that these input iterators must return data in sorted order,
     * according to the provided cmp function, otherwise the output of this
     * iterator will not be sorted properly. The cmp function should be the
     * RECORD comparison function, not the KEY comparison function, where the
     * distinction is relevant.
     */
    MergeIterator(std::vector<std::unique_ptr<GenericIterator<Record>>>
                  &iters_to_merge, const catalog::RecordCmpFunc cmp);

    /*
     * Determine if another record in the iterator exists, returns True if so
     * and false if not.
     */
    bool next() override;

    /*
     * Return the next item in sorted order from the iterator. The value returned
     * by this method will change each time next is called. When no more items remain,
     * it will return an invalid record.
     */
    Record get_item() override;

    bool supports_rewind() override;
    IteratorPosition save_position() override;
    void rewind(IteratorPosition /*position*/) override;

    size_t element_count() override;
    bool supports_element_count() override;

    void end_scan() override;

    ~MergeIterator();
private:
    struct HeapCompareFunc {
        catalog::RecordCmpFunc rec_cmp;
        bool operator()(std::pair<Record, size_t> a, std::pair<Record, size_t> b) {
            return this->rec_cmp(a.first.get_data(), b.first.get_data()) > 0;
        }
    };

    HeapCompareFunc cmp;
    std::vector<std::unique_ptr<GenericIterator<Record>>> iterators;

    std::unique_ptr<std::priority_queue<std::pair<Record, size_t>, std::vector<std::pair<Record, size_t>>, HeapCompareFunc>> merge_heap;

    bool at_end;
    Record current_record;
    PageNum page_count;

};


}}
#endif
