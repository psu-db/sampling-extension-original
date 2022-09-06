/*
 *
 */

#ifndef H_ISAMTREE
#define H_ISAMTREE

#include "util/base.h"
#include "util/types.h"
#include "util/kvp.h"
#include "util/bf_config.h"
#include "io/pagedfile.hpp"
#include "ds/BloomFilter.h"
#include "lsm/MemTable.h"

namespace lsm { namespace ds {

struct ISAMTreeMetaHeader {
    PageNum root_node;
    PageNum first_data_page;
    PageNum last_data_page;
    size_t tombstone_count;
    size_t record_count;
};

struct ISAMTreeInternalNodeHeader {
    PageNum next_sibling;
    PageNum prev_sibling;
    size_t leaf_rec_cnt; // number of records in leaf nodes under this node
    size_t internal_rec_nt; // number of internal records in this node
};

constexpr PageOffset ISAMTreeInternalNodeHeaderSize = MAXALIGN(sizeof(ISAMTreeInternalNodeHeader));

const PageNum BTREE_META_PNUM = 1;

class ISAMTree {
public:
    static ISAMTree *create(PagedFile *pfile, ISAMTree *tree1, ISAMTree *tree2, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, MemTable *mtable, ISAMTree *tree2, gsl_rng *rng);
    //static ISAMTree *create(PagedFile *pfile, MemTable *mtable, MemISAM *tree, gsl_rng *rng);
    //static ISAMTree *create(PagedFile *pfile, MemISAM *tree1, MemISAM *tree2, gsl_rng *rng);
    //static ISAMTree *create(PagedFile *pfile, MemISAM *tree1, ISAMTree *tree2, gsl_rng *rng);

    ~ISAMTree();

    /*
     * Returns the first leaf page pid within the tree that contains a key
     * greater than or equal to the specified boundary key. Returns INVALID_PID
     * if no pages satisfy this constraint.
     */
    PageNum get_lower_bound(byte *key);

    /*
     * Returns the last leaf page pid within the tree that contains a key less
     * than or equal to the specified boundary key. Returns INVALID_PID if no
     * pages satisfy this constraint.
     */
    PageNum get_upper_bound(byte *key);

    /*
     * Returns the newest record within this tree with the specified key, and a
     * timestamp no greater than time.
     */
    byte *get(byte *key);

    /*
     * Searches the tree for a tombstone record for the specified key/value
     * pair active at Timestamp time. If no such tombstone exists, returns an
     * invalid record, otherwise return the tombstone.
     */
    byte *get_tombstone(byte *key, byte *val);

    /*
     * Returns an iterator over all of the leaf pages within this ISAM Tree.
     * Each get_item() call will return a pointer to a buffered page, which
     * must then be manually iterated over to get the records.
     */
    inline PagedFileIterator *start_scan() {
        return this->pfile->start_scan(this->first_data_page, this->last_data_page);
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
     * Returns a pointer to the file representing this ISAM Tree object.
     */
    inline PagedFile *get_pfile() {
        return this->pfile;
    }

    /*
     * Returns the number of bytes of memory used by auxiliary structures
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
    ISAMTreeMetaHeader *get_metapage();

    PagedFile *pfile;
    BloomFilter *tombstone_bloom_filter;
    PageNum root_page;
    PageNum first_data_page;
    PageNum last_data_page;
    size_t rec_cnt;
    byte *internal_buffer;
    
    size_t tombstone_cnt; // number of tombstones within the ISAM Tree

    ISAMTree(PagedFile *pfile, BloomFilter *tombstone_filter);

    PageNum search_internal_node_lower(PageNum pnum, byte *key);
    PageNum search_internal_node_upper(PageNum pnum, byte *key);
    byte *search_leaf_page(PageNum pnum, byte *key, size_t *idx=nullptr);

    static int initial_page_allocation(PagedFile *pfile, PageNum page_cnt, size_t tombstone_count, PageNum *first_leaf, PageNum *first_internal, PageNum *meta);
    static PageNum generate_internal_levels(PagedFile *pfile, PageNum first_leaf_page, size_t final_leaf_rec_cnt, byte *buffer, size_t buffer_sz);

    static BloomFilter *initialize(PagedFile *pfile, PagedFileIterator *iter1, size_t iter1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(PagedFile *pfile, byte *sorted_run1, size_t run1_rec_cnt, byte *sorted_run2, size_t run2_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(PagedFile *pfile, byte *sorted_run1, size_t run1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng);

    static constexpr size_t internal_record_size = KEYLEN + MAXALIGN(sizeof(PageNum));

    static inline void build_internal_record(byte *buffer, byte *key, PageNum target_page) {
        memcpy(buffer, key, KEYLEN);
        memcpy(buffer + KEYLEN, &target_page, sizeof(PageNum));
    }

    static inline byte *get_internal_record(byte *internal_page_buffer, size_t idx) {
        return internal_page_buffer + ISAMTreeInternalNodeHeaderSize + internal_record_size * idx;
    }

    static inline byte *get_internal_key(byte *buffer) {
        return buffer;
    }

    static inline PageNum get_internal_value(byte *buffer) {
        return *((PageNum *) buffer + KEYLEN);
    }


    inline size_t max_leaf_record_idx(PageNum pnum) {
        return (pnum == this->last_data_page) ? rec_cnt % PAGE_SIZE : (PAGE_SIZE / RECORDLEN);
    }

    static inline byte *copy_of(byte *record) {
        byte *copy = (byte *) aligned_alloc(CACHELINE_SIZE, RECORDLEN);
        memcpy(copy, record, RECORDLEN);
        return copy;
    }
};

}}

#endif
