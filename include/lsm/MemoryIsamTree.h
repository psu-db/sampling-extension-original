/*
 *
 */

#pragma once

#include "util/base.h"
#include "util/types.h"
#include "util/record.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "lsm/MemTable.h"

namespace lsm {

constexpr size_t MemISAMTreeInternalNodeSize = 64;

class MemISAMTree {
public:
    static MemISAMTree *create(MemTable *mtable, gsl_rng *rng);
    static MemISAMTree *create(MemISAMTree *tree1, MemISAMTree *tree2, gsl_rng *rng);
    static MemISAMTree *create(MemTable *mtable, MemISAMTree *tree, gsl_rng *rng);

    ~MemISAMTree();

    /*
     * Returns the first leaf page pid within the tree that contains a key
     * greater than or equal to the specified boundary key. Returns INVALID_PID
     * if no pages satisfy this constraint.
     */
    char *get_lower_bound(const char *key);

    /*
     * Returns the last leaf page pid within the tree that contains a key less
     * than or equal to the specified boundary key. Returns INVALID_PID if no
     * pages satisfy this constraint.
     */
    char *get_upper_bound(const char *key);

    std::pair<const char*, const char*> get_bounds(const char *lower_key, const char *upper_key, char *buffer);

    const char *sample_record(const char *start_ptr, size_t record_idx);

    /*
     * Returns the newest record within this tree with the specified key, and a
     * timestamp no greater than time.
     */
    char *get(const char *key);

    /*
     * Searches the tree for a tombstone record for the specified key/value
     * pair active at Timestamp time. If no such tombstone exists, returns an
     * invalid record, otherwise return the tombstone.
     */
    char *check_tombstone(const char *key, const char *val);

    /*
     * Returns an iterator over all of the leaf pages within this ISAM Tree.
     * Each get_item() call will return a pointer to a buffered page, which
     * must then be manually iterated over to get the records.
     */
    inline char *start_scan() {
        return nullptr;
    };

    /*
     * Returns the number of records contained within the leaf nodes of this
     * ISAM Tree.
     */
    inline size_t get_record_count() {
        return this->rec_cnt;
    }

    /*
     * Returns the number of leaf pages within this tree
     */
    inline PageNum get_leaf_page_count() {
        return this->last_data_page - this->first_data_page + 1;
    }

    /*
     * Returns the number of chars of memory used by auxiliary structures
     * associated with this ISAM tree.
     */
    inline size_t memory_utilization() {
        return this->tombstone_bloom_filter->mem_utilization();
    }

    /*
     * Returns the number of tombstone records within this level
     */
    inline size_t tombstone_count() {
        return this->tombstone_cnt;
    }

private:
    BloomFilter *tombstone_bloom_filter;
    PageNum first_data_page;
    PageNum last_data_page;
    size_t rec_cnt;
    char *data;
    
    size_t tombstone_cnt; // number of tombstones within the ISAM Tree

    MemISAMTree(char *data, BloomFilter *tombstone_filter);

    PageNum search_internal_node_lower(PageNum pnum, const char *key);
    PageNum search_internal_node_upper(PageNum pnum, const char *key);
    char *search_leaf_page(PageNum pnum, const char *key, size_t *idx=nullptr);

    static int initial_page_allocation(PageNum page_cnt, size_t tombstone_count, PageNum *first_leaf, PageNum *first_internal, PageNum *meta);
    static PageNum generate_internal_levels(PageNum first_leaf_page, size_t final_leaf_rec_cnt, char *buffer, size_t buffer_sz);

    static BloomFilter *initialize(char *sorted_run1, size_t run1_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(char *sorted_run1, size_t run1_rec_cnt, char *sorted_run2, size_t run2_rec_cnt, size_t tombstone_count, gsl_rng *rng);

    static constexpr size_t internal_record_size = key_size + MAXALIGN(sizeof(PageNum));

    static inline void build_internal_record(char *buffer, const char *key, PageNum target_page) {
        memcpy(buffer, key, key_size);
        memcpy(buffer + key_size, &target_page, sizeof(PageNum));
    }

    static inline const char *get_internal_record(const char *internal_page_buffer, size_t idx) {
        return internal_page_buffer + internal_record_size * idx;
    }

    static inline const char *get_internal_key(const char *buffer) {
        return buffer;
    }

    static inline PageNum get_internal_value(const char *buffer) {
        return *((PageNum *) buffer + key_size);
    }


    inline size_t max_leaf_record_idx(PageNum pnum) {
        return (pnum == this->last_data_page) ? rec_cnt % PAGE_SIZE : (PAGE_SIZE / record_size);
    }

    static inline char *copy_of(const char *record) {
        char *copy = (char *) aligned_alloc(CACHELINE_SIZE, record_size);
        memcpy(copy, record, record_size);
        return copy;
    }
};
}
