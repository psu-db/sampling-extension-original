/*
 *
 */

#pragma once

#include "util/base.h"
#include "util/types.h"
#include "util/record.h"
#include "util/bf_config.h"
#include "io/PagedFile.h"
#include "ds/BloomFilter.h"
#include "lsm/MemTable.h"
#include "lsm/MemoryIsamTree.h"

namespace lsm { 

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
const PageNum BTREE_FIRST_LEAF_PNUM = 2;

const size_t ISAM_INIT_BUFFER_SIZE = 1; // measured in pages
const size_t ISAM_RECORDS_PER_LEAF = PAGE_SIZE / record_size;

class ISAMTree {
public:
    static ISAMTree *create(PagedFile *pfile, MemTable *mtable, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, ISAMTree *tree1, ISAMTree *tree2, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, MemTable *mtable, ISAMTree *tree2, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, MemTable *mtable, MemISAMTree *tree, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, MemISAMTree *tree1, MemISAMTree *tree2, gsl_rng *rng);
    static ISAMTree *create(PagedFile *pfile, MemISAMTree *tree1, ISAMTree *tree2, gsl_rng *rng);

    ~ISAMTree();

    /*
     * Returns the first leaf page pid within the tree that contains a key
     * greater than or equal to the specified boundary key. Returns INVALID_PID
     * if no pages satisfy this constraint.
     *
     * buffer is a thread-local buffer to be used for IO, to avoid any
     * inter-thread contention on internal buffering state. This must be
     * aligned to SECTOR_SIZE and be at least PAGE_SIZE in length. Its contents
     * will vary over the function call and are not defined. Any data within it
     * will be clobbered by this function.
     */
    PageNum get_lower_bound(const char *key, char *buffer);

    /*
     * Returns the last leaf page pid within the tree that contains a key less
     * than or equal to the specified boundary key. Returns INVALID_PID if no
     * pages satisfy this constraint.
     *
     * buffer is a thread-local buffer to be used for IO, to avoid any
     * inter-thread contention on internal buffering state. This must be
     * aligned to SECTOR_SIZE and be at least PAGE_SIZE in length. Its contents
     * will vary over the function call and are not defined. Any data within it
     * will be clobbered by this function.
     */
    PageNum get_upper_bound(const char *key, char *buffer);

    /*
     * Returns the first leaf page pnum within the tree that contains a key
     * greater than or equal to the specified lower key boundary, and the last
     * page page pnum that contains a key less than or equal to upper key.
     *
     * buffer1 and buffer2 are thread-local buffers to be used for IO. They must
     * be SECTOR_SIZE aligned and at least PAGE_SIZE in length. Their contents
     * will vary over the function call and are not defined at any point in time,
     * and any data within them prior to this call will be lost.
     *
     * Two buffers are required here because at some points during function
     * execution, two different pages must be kept in memory at once.
     */
    std::pair<PageNum, PageNum> get_bounds(const char *lower_key, const char *upper_key, char *buffer);

    /*
     * Reads a random record from within the specified page range and returns a
     * pointer to it. Note that the record in question will be stored within
     * buffer, and so must be copied out or used prior to using the buffer for
     * anything else. If upper_pnum is the last leaf page of the tree, it is
     * possible for this function to sample a record that doesn't exist, in
     * which case it will return nullptr.
     */
    const char *sample_record(PageNum lower_pnum, PageNum upper_pnum, 
                              char *buffer, gsl_rng *rng);

    /*
     * Returns the newest record within this tree with the specified key.
     *
     * buffer is a thread-local buffer to be used for IO, to avoid any
     * inter-thread contention on internal buffering state. This must be
     * aligned to SECTOR_SIZE and be at least PAGE_SIZE in length. Its contents
     * will vary over the function call and are not defined. Any data within it
     * will be clobbered by this function.
     */
    char *get(const char *key, char *buffer);

    /*
     * Searches the tree for a tombstone record for the specified key/value
     * pair active at Timestamp time. If no such tombstone exists, returns an
     * invalid record, otherwise return the tombstone.
     *
     * buffer is a thread-local buffer to be used for IO, to avoid any
     * inter-thread contention on internal buffering state. This must be
     * aligned to SECTOR_SIZE and be at least PAGE_SIZE in length. Its contents
     * will vary over the function call and are not defined. Any data within it
     * will be clobbered by this function.
     */
    char *get_tombstone(const char *key, const char *val, char *buffer);

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
    ISAMTreeMetaHeader *get_metapage();

    PagedFile *pfile;
    BloomFilter *tombstone_bloom_filter;
    PageNum root_page;
    PageNum first_data_page;
    PageNum last_data_page;
    size_t rec_cnt;
    char *internal_buffer;
    
    size_t tombstone_cnt; // number of tombstones within the ISAM Tree

    ISAMTree(PagedFile *pfile, BloomFilter *tombstone_filter);

    PageNum search_internal_node_lower(PageNum pnum, const char *key, char *buffer);
    PageNum search_internal_node_upper(PageNum pnum, const char *key, char *buffer);
    char *search_leaf_page(PageNum pnum, const char *key, char *buffer, size_t *idx=nullptr);

    static int initial_page_allocation(PagedFile *pfile, PageNum page_cnt, size_t tombstone_count, PageNum *first_leaf, PageNum *first_internal, PageNum *meta);
    static PageNum generate_internal_levels(PagedFile *pfile, size_t final_leaf_rec_cnt, char *buffer, size_t buffer_sz);
    static PageNum generate_next_internal_level(PagedFile *pfile, size_t *pl_final_pg_rec_cnt, PageNum *pl_first_pg, bool first_level, char *out_buffer, size_t out_buffer_sz, char *in_buffer, size_t in_buffer_sz);

    static void generate_leaf_pages(PagedFile *pfile, PagedFileIterator *iter1, PagedFileIterator *iter2);

    static BloomFilter *initialize(PagedFile *pfile, PagedFileIterator *iter1, size_t iter1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, char *sorted_run2, size_t run2_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng);
    static BloomFilter *initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, size_t tombstone_count, gsl_rng *rng);

    static PageNum pre_init(size_t record_count, size_t tombstone_count, gsl_rng *rng, PagedFile *pfile, BloomFilter **filter, char **buffer);
    static PageNum write_final_buffer(size_t output_idx, PageNum cur_pnum, size_t *last_leaf_rec_cnt, PagedFile *pfile, char *buffer);
    static bool post_init(size_t record_count, size_t tombstone_count, PageNum last_leaf, PageNum root_pnum, char* buffer, PagedFile *pfile);
    static constexpr size_t internal_record_size = key_size + MAXALIGN(sizeof(PageNum));

    static inline void build_internal_record(char *buffer, const char *key, PageNum target_page) {
        memcpy(buffer, key, key_size);
        memcpy(buffer + key_size, &target_page, sizeof(PageNum));
    }

    static inline char *get_internal_record(char *internal_page_buffer, size_t idx) {
        return internal_page_buffer + ISAMTreeInternalNodeHeaderSize + internal_record_size * idx;
    }

    static inline const char *get_internal_key(const char *buffer) {
        return buffer;
    }

    static inline PageNum get_internal_value(const char *buffer) {
        return *((PageNum *) buffer + key_size);
    }

    static inline char *get_page(char *buffer, size_t idx) {
        return buffer + (idx * PAGE_SIZE);
    }


    static inline ISAMTreeInternalNodeHeader *get_header(char *buffer, size_t idx=0) {
        return (ISAMTreeInternalNodeHeader *) get_page(buffer, idx);
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
