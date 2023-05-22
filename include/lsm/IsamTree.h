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
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "lsm/CHTRun.h"
#include "util/internal_record.h"

namespace lsm { 

struct ISAMTreeMetaHeader {
    PageNum root_node;
    PageNum first_data_page;
    PageNum last_data_page;
    size_t tombstone_count;
    size_t record_count;
};

const PageNum BTREE_META_PNUM = 1;
const PageNum BTREE_FIRST_LEAF_PNUM = 2;

const size_t ISAM_INIT_BUFFER_SIZE = 64; // measured in pages
const size_t ISAM_RECORDS_PER_LEAF = PAGE_SIZE / sizeof(record_t);

thread_local size_t cancelations = 0;

// Convert an index into the runs array to the
// corresponding index into the cursor array
#define RCUR(i) (tree_cnt + (i))

// Convert an index into the trees array to
// the corresponding index into the cursor array
#define TCUR(i) ((i))

class ISAMTree {
public:
    /*
     * Create an ISAM Tree object from an already formatted PagedFile
     */
    ISAMTree(PagedFile *pfile, size_t record_cnt, size_t ts_cnt, PageNum last_leaf, PageNum root_leaf, BloomFilter *tomb_filter, const gsl_rng *rng) 
    : pfile(pfile)
    , root_page(root_leaf)
    , first_data_page(BTREE_FIRST_LEAF_PNUM)
    , last_data_page(last_leaf)
    , rec_cnt(record_cnt)
    , tombstone_cnt(ts_cnt)
    , retain_file(false) {

        // rebuild the bloom filters
        auto iter = this->start_scan();
        size_t records_processed = 0;
        while (iter->next()) {
            auto pg = (record_t*)iter->get_item();
            for (size_t i=0; i < PAGE_SIZE/sizeof(record_t); i++) {
                if (++records_processed > this->rec_cnt) {
                    break;
                }

                auto key = pg[i].key;
                tomb_filter->insert(key);
            }
        }

        delete iter;
    }

    ISAMTree(PagedFile *pfile, const gsl_rng *rng, BloomFilter *tomb_filter, CHTRun * const* runs, size_t run_cnt, ISAMTree * const*trees, size_t tree_cnt) {
        TIMER_INIT();
        std::vector<Cursor> cursors(run_cnt + tree_cnt);
        std::vector<PagedFileIterator *> isam_iters(tree_cnt);

        PriorityQueue pq(run_cnt + tree_cnt);

        size_t incoming_record_cnt = 0;
        size_t incoming_tombstone_cnt = 0;

        TIMER_START();
        // Initialize the priority queue
        // load up the disk levels
        for (size_t i=0; i<tree_cnt; i++) {
            assert(trees[i]);
            isam_iters[i] = trees[i]->start_scan();
            assert(isam_iters[i]->next());
            const record_t *start = (record_t*)isam_iters[i]->get_item();
            const record_t *end = start + ISAM_RECORDS_PER_LEAF;
            cursors[TCUR(i)] = Cursor{start, end, 0, trees[i]->get_record_count()};
            pq.push(cursors[TCUR(i)].ptr, TCUR(i));

            incoming_record_cnt += trees[i]->get_record_count();
            incoming_tombstone_cnt += trees[i]->get_tombstone_count();
        }

        // load up the memory levels;
        for (size_t i=0; i<run_cnt; i++) {
            assert(runs[i]);
            const record_t *start = runs[i]->sorted_output();
            const record_t *end = start + runs[i]->get_record_count();
            cursors[RCUR(i)] = Cursor{start, end, 0, runs[i]->get_record_count()};
            pq.push(cursors[RCUR(i)].ptr, RCUR(i));

            incoming_record_cnt += runs[i]->get_record_count();
            incoming_tombstone_cnt += runs[i]->get_tombstone_count();
        }

        char *buffer = nullptr;
        size_t last_leaf_rec_cnt = 0;

        PageNum leaf_page_cnt = ISAMTree::pre_init(incoming_record_cnt, incoming_tombstone_cnt, rng, pfile, &buffer);
        assert(leaf_page_cnt);
        assert(buffer);

        PageNum cur_leaf_pnum = BTREE_FIRST_LEAF_PNUM;
        size_t output_idx = 0;

        this->rec_cnt = 0;
        this->tombstone_cnt = 0;

        while (pq.size()) {
            auto cur = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record{nullptr, 0};

            // If this record is not a tombstone, and there is another
            // record next in the stream with the same key and value, then
            // the record and tombstone should cancel each other out.
            if (!cur.data->is_tombstone() && next.data != nullptr &&
                cur.data->match(next.data) && next.data->is_tombstone()) {
                
                // pop the next two records from the queue and discard them
                pq.pop(); pq.pop();
                cancelations++;

                auto iter = (cur.version >= tree_cnt) ? nullptr : isam_iters[cur.version];
                if (advance_cursor(cursors[cur.version], iter)) {
                    pq.push(cursors[cur.version].ptr, cur.version);

                }

                iter = (next.version >= tree_cnt ? nullptr : isam_iters[next.version]);
                if (advance_cursor(cursors[next.version], iter)) {
                    pq.push(cursors[next.version].ptr, next.version);
                }

                continue;
            }

            memcpy(buffer +  sizeof(record_t) * output_idx, cur.data, sizeof(record_t));
            output_idx++;
            this->rec_cnt += 1;
            if (cur.data->is_tombstone() && tomb_filter) {
                tomb_filter->insert(cur.data->key);
                this->tombstone_cnt += 1;
            }
            
            pq.pop();

            auto iter = (cur.version >= tree_cnt) ? nullptr : isam_iters[cur.version];

            if (advance_cursor(cursors[cur.version], iter)) {
                    pq.push(cursors[cur.version].ptr, cur.version);
            }

            if (output_idx >= ISAM_INIT_BUFFER_SIZE * ISAM_RECORDS_PER_LEAF) {
                assert(pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer));
                output_idx = 0;
                cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
            }
        }

        this->last_data_page = ISAMTree::write_final_buffer(output_idx, cur_leaf_pnum, &last_leaf_rec_cnt, pfile, buffer);
        assert(this->last_data_page != INVALID_PNUM);

        TIMER_STOP();
        auto copy_time = TIMER_RESULT();

        TIMER_START();
        this->root_page = ISAMTree::generate_internal_levels(pfile, last_leaf_rec_cnt, buffer, ISAM_INIT_BUFFER_SIZE);
        TIMER_STOP();

        auto internal_time = TIMER_RESULT();
        this->first_data_page = BTREE_FIRST_LEAF_PNUM;

        assert(ISAMTree::post_init(this->rec_cnt, this->tombstone_cnt, this->last_data_page, this->root_page, buffer, pfile));

        for (size_t i=0; i<isam_iters.size(); i++) {
            delete isam_iters[i];
        }

        this->pfile = pfile;
        this->retain_file = false;

        fprintf(stdout, "%ld %ld\n", copy_time, internal_time);

        free(buffer);
    }


    ~ISAMTree() {
        if (!this->retain_file) {
            this->pfile->remove_file();
        }
    }

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
    PageNum get_lower_bound(const key_t& key, char *buffer) {
        PageNum current_page = this->root_page;
        // The leaf pages are all allocated contiguously at the start of the file,
        // so we'll know when we have a pointer to one when the pnum is no larger
        // than the last data page.
        while (current_page > this->last_data_page) {
            current_page = search_internal_node_lower(current_page, key, buffer);
        }

        return current_page;
    }

    std::pair<PageNum, size_t> get_lower_bound_index(const key_t& key, char *buffer) {
        auto pnum = this->get_lower_bound(key, buffer);

        if (pnum == INVALID_PNUM) {
            return {pnum, 0};
        }

        size_t idx;
        this->search_leaf_page(pnum, key, buffer , &idx);

        return {pnum, idx};
    }


    std::pair<PageNum, size_t> get_upper_bound_index(const key_t& key, char *buffer) {
        auto pnum = this->get_upper_bound(key, buffer);

        if (pnum == INVALID_PNUM) {
            return {pnum, 0};
        }

        size_t idx;
        this->search_leaf_page(pnum, key, buffer, &idx);

        // FIXME: This could be replaced by a modified version of
        // search_leaf_page, but this avoids a lot of code duplication for what
        // will almost certainly be a very short loop over in-cache data.
        while (idx <= this->max_leaf_record_idx(pnum) && key >= ((record_t*)(buffer) + idx)->key) {
            idx++;
        }

        return {pnum, --idx};
    }


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
    PageNum get_upper_bound(const key_t& key, char *buffer) {
        PageNum current_page = this->root_page;
        // The leaf pages are all allocated contiguously at the start of the file,
        // so we'll know when we have a pointer to one when the pnum is no larger
        // than the last data page.
        while (current_page > this->last_data_page) {
            current_page = search_internal_node_upper(current_page, key, buffer);
        }

        // If the key being searched for is the boundary key, search_internal_node_upper
        // will return the page after the page for which it is the boundary key. If the
        // page in question does not actually contain the key being searched for, then
        // we want to return one less than the returned page.
        if (current_page > BTREE_FIRST_LEAF_PNUM) {
            if (this->search_leaf_page(current_page, key, buffer) == 0) {
                return current_page - 1;
            }
        }

        return current_page;
    }

    /*
     * Returns a pointer to the record_idx'th record starting to count from the
     * first record of start_page, or nullptr if this record doesn't exist
     * (passed the end of the tree). The pointer will point to a record contained
     * within buffer. If pg_in_buffer matches the page containing the desired
     * record, no IO will be performed as the existing buffer contents will be
     * reused. Otherwise, an IO will be performed, and the pg_in_buffer will be
     * updated to match the page currently in the buffer.
     */
    const record_t *sample_record(PageNum start_page, size_t record_idx, char *buffer, PageNum &pg_in_buffer) {
        // TODO: Verify that this is the appropriate interface to use 
        assert(start_page >= this->first_data_page && start_page <= this->last_data_page);

        constexpr size_t records_per_page = PAGE_SIZE / sizeof(record_t);

        PageNum page_offset = record_idx / records_per_page;
        assert(start_page + page_offset <= this->last_data_page);

        size_t idx = page_offset % records_per_page;

        if (start_page + page_offset != pg_in_buffer) {
            assert(this->pfile->read_page(start_page + page_offset, buffer));
            pg_in_buffer = start_page + page_offset;
        }

        return (record_t*)(buffer + idx * sizeof(record_t));
    }

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
    bool check_tombstone(const key_t& key, const value_t& val, char *buffer) {
        auto lb = this->get_lower_bound_index(key, buffer);
        PageNum pnum = lb.first;
        size_t idx = lb.second;

        if (pnum == INVALID_PNUM) {
            return false;
        }

        do {
            assert(this->pfile->read_page(pnum, buffer));
            for (size_t i=idx; i<this->max_leaf_record_idx(pnum); i++) {
                auto rec = (record_t*)(buffer + (idx * sizeof(record_t)));

                if (!rec->lt(key, val)) {
                    return rec->match(key, val, true);
                }
            }

            pnum++;
            idx = 0;
        } while (pnum <= this->last_data_page);

        return false;
    }

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
     * Returns the PageNum corresponding to the final leaf page within the
     * tree.
     */
    inline PageNum get_last_leaf_pnum() {
        return this->last_data_page;
    }

    /*
     * Returns the PageNum corresponding to the root node within the
     * tree.
     */
    inline PageNum get_root_pnum() {
        return this->root_page;
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
    inline size_t get_memory_utilization() {
        return 0;
    }

    /*
     * Returns the number of tombstone records within this level
     */
    inline size_t get_tombstone_count() {
        return this->tombstone_cnt;
    }

    /*
     *  Prevent the deletion of the backing file from the
     *  underlying filesystem when this object's destructor
     *  is called.
     */
    void retain() {
        this->retain_file = true;
    }

private:
    PagedFile *pfile;
    PageNum root_page;
    PageNum first_data_page;
    PageNum last_data_page;

    size_t rec_cnt;
    size_t tombstone_cnt; // number of tombstones within the ISAM Tree
    
    bool retain_file;


    PageNum search_internal_node_lower(PageNum pnum, const key_t& key, char *buffer) {
        assert(this->pfile->read_page(pnum, buffer));

        size_t min = 0;
        size_t max = ((ISAMTreeInternalNodeHeader *) buffer)->internal_rec_cnt - 1;

        // If the entire range of numbers falls below the target key, the algorithm
        // will return max as its bound, even though there actually isn't a valid
        // bound. So we need to check this case manually and return INVALID_PNUM.
        auto node_key = get_internal_record_key(buffer, max);
        if (key > node_key) {
            return INVALID_PNUM;
        }

        while (min < max) {
            size_t mid = (min + max) / 2;
            node_key = get_internal_record_key(buffer, mid);
            if (key > node_key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return get_internal_value(get_internal_record(buffer, min));
    }

    PageNum search_internal_node_upper(PageNum pnum, const key_t& key, char *buffer) {
        assert(this->pfile->read_page(pnum, buffer));

        size_t min = 0;
        size_t max = ((ISAMTreeInternalNodeHeader *) buffer)->internal_rec_cnt - 1;

        // If the entire range of numbers falls below the target key, the algorithm
        // will return max as its bound, even though there actually isn't a valid
        // bound. So we need to check this case manually and return INVALID_PNUM.
        auto node_key = get_internal_record_key(buffer, max);
        if (key > node_key) {
            return INVALID_PNUM;
        }

        while (min < max) {
            size_t mid = (min + max) / 2;
            node_key = get_internal_record_key(buffer, mid);
            if (key >= node_key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return get_internal_value(get_internal_record(buffer, min));
    }

    char *search_leaf_page(PageNum pnum, const key_t& key, char *buffer, size_t *idx=nullptr) {
        size_t min = 0;
        size_t max = this->max_leaf_record_idx(pnum);

        assert(this->pfile->read_page(pnum, buffer));
        //const char * record_key;

        while (min < max) {
            size_t mid = (min + max) / 2;
            //record_key = get_key(buffer + (mid * sizeof(record_t)));
            auto record_key = (((record_t*)buffer) + mid)->key;

            if (key > record_key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        char *record = buffer + (min * sizeof(record_t));

        // Update idx if required, regardless of if the found
        // record is an exact match (lower-bound behavior).
        if (idx) {
            *idx = min;
        }

        // Check if the thing that we found matches the target. If so, we've found
        // it. If not, the target doesn't exist on the page.
        if (key == ((record_t*)record)->key) {
            return record;
        }

        return nullptr;
    }

    static int initial_page_allocation(PagedFile *pfile, PageNum page_cnt, size_t tombstone_count, PageNum *first_leaf, PageNum *first_internal, PageNum *meta);

    static PageNum generate_internal_levels(PagedFile *pfile, size_t final_leaf_rec_cnt, char *out_buffer, size_t out_buffer_sz) {
        // FIXME: There're some funky edge cases here if the input_buffer_sz is larger
        // than the number of leaf pages
        size_t in_buffer_sz = 1;
        auto in_buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * in_buffer_sz);

        // First, generate the first internal level
        PageNum pl_first_pg = BTREE_FIRST_LEAF_PNUM;
        size_t pl_final_rec_cnt = final_leaf_rec_cnt;
        PageNum pl_pg_cnt = ISAMTree::generate_next_internal_level(pfile, &pl_final_rec_cnt, &pl_first_pg, true, out_buffer, out_buffer_sz, in_buffer, in_buffer_sz);

        assert(pl_pg_cnt != INVALID_PNUM);

        // If there was only 1 page in the first internal level, then that will be our root.
        if (pl_pg_cnt == 1) {
            free(in_buffer);
            return pl_first_pg;
        }

        // Otherwise, we need to repeatedly create new levels until the page count returned
        // is 1.
        while ((pl_pg_cnt = ISAMTree::generate_next_internal_level(pfile, &pl_final_rec_cnt, &pl_first_pg, false, out_buffer, out_buffer_sz, in_buffer, in_buffer_sz)) != 1) {
            assert(pl_pg_cnt != INVALID_PNUM);
        }

        // The last page allocated is the tree root.
        free(in_buffer);
        return pl_first_pg + pl_pg_cnt - 1;
    }

    static PageNum generate_next_internal_level(PagedFile *pfile, size_t *pl_final_pg_rec_cnt, PageNum *pl_first_pg, bool first_level, char *out_buffer, size_t out_buffer_sz, char *in_buffer, size_t in_buffer_sz) {
        
        // These variables names were getting very unwieldy. Here's a little glossary
        //      nl - new level (the level being created by this function)
        //      pl - previous level (the level from which the new level is being created)
        //      in - input -- refers to the buffers, etc. for the previous level that are read
        //      out - output -- refers to buffers, etc. for the new level that are written

        // We need to zero out the output buffer, as we may not exactly fill a page,
        // and there could be leftover data that would corrupt the last page in the level
        // otherwise.
        memset(out_buffer, 0, out_buffer_sz * PAGE_SIZE);

        size_t int_recs_per_pg = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

        size_t pl_recs_per_pg = (first_level) ? PAGE_SIZE / sizeof(record_t) : int_recs_per_pg;

        PageNum in_pnum = *pl_first_pg;
        PageNum pl_last_pg = pfile->get_page_count();
        PageNum nl_first_pg = pl_last_pg + 1;
        PageNum out_pnum = nl_first_pg;

        size_t pl_pgs_remaining = pl_last_pg - *pl_first_pg + 1;

        size_t out_pg_idx = 0;
        size_t out_rec_idx = 0;

        // The first internal node has no previous prev_sibling
        get_header(out_buffer)->prev_sibling = INVALID_PNUM;
        get_header(out_buffer)->leaf_rec_cnt = 0;
        get_header(out_buffer)->internal_rec_cnt = 0;

        size_t total_records = 0;

        do {
            // Read as many input pages from the previous level as the buffer
            // will allow, or as many pages as remain, whichever is smaller.
            size_t in_pgs = std::min(in_buffer_sz, pl_pgs_remaining);
            if (pfile->read_pages(in_pnum, in_pgs, in_buffer) == 0) {
                goto error;
            }

            // Track the number of pages that remain to be read, necessary
            // for the above check to ensure we don't read past the end of
            // the level.
            pl_pgs_remaining -= in_pgs;

            for (size_t in_pg_idx = 0; in_pg_idx < in_pgs; in_pg_idx++) {
                // Get the key of the last record in this leaf page
                size_t last_record = (in_pnum < pl_last_pg || *pl_final_pg_rec_cnt == 0) ? pl_recs_per_pg - 1 : (*pl_final_pg_rec_cnt) - 1;

                key_t key = (first_level) ? ((record_t*)(get_page(in_buffer, in_pg_idx) + last_record * sizeof(record_t)))->key
                                                : get_internal_record_key(get_page(in_buffer, in_pg_idx), last_record);

                // Increment the total number of children of this internal page
                get_header(out_buffer, out_pg_idx)->leaf_rec_cnt += (first_level) ? pl_recs_per_pg : get_header(in_buffer, in_pg_idx)->leaf_rec_cnt;
                total_records += (first_level) ? pl_recs_per_pg : get_header(in_buffer, in_pg_idx)->leaf_rec_cnt;

                // Get the address in the buffer for the new internal record
                char *internal_buff = get_internal_record(get_page(out_buffer, out_pg_idx), out_rec_idx++);

                // Create the new internal record at the address from above
                build_internal_record(internal_buff, key, in_pnum);

                // Increment the record counter for this page
                get_header(out_buffer, out_pg_idx)->internal_rec_cnt++;

                // Advance to the next input page
                in_pnum++;

                // We've filled up an output page with records, so we initialize
                // the next output page
                if (out_rec_idx >= int_recs_per_pg) {
                    // Advance the index variables to the first record of the next
                    // page
                    out_rec_idx = 0;
                    out_pg_idx++;

                    // Update the full page's header to point to the next one.
                    get_header(out_buffer, out_pg_idx - 1)->next_sibling = out_pnum + out_pg_idx;

                    // If we've filled up the whole buffer, we need to write it
                    if (out_pg_idx >= out_buffer_sz) {
                        if (!pfile->allocate_pages(out_buffer_sz)) {
                            goto error;
                        }

                        if (!pfile->write_pages(out_pnum, out_buffer_sz, out_buffer)) {
                            goto error;
                        }

                        out_pnum += out_buffer_sz;
                        out_pg_idx = 0;
                  }

                  // Set up the pg pointers in the header. Technically, we
                  // don't *really* need these as the pgs are laid out
                  // sequentially anyway. But they are a convenient way still to
                  // identify the first and last pg on a given level.
                  get_header(out_buffer, out_pg_idx)->prev_sibling = out_pnum + out_pg_idx - 1;
                  get_header(out_buffer, out_pg_idx)->next_sibling = INVALID_PNUM;
                  get_header(out_buffer, out_pg_idx)->leaf_rec_cnt = 0;
                  get_header(out_buffer, out_pg_idx)->internal_rec_cnt = 0;
                }
            }
        } while (in_pnum <= pl_last_pg);

        
        // If this is the case, it means we wrote the buffer on the last iteration
        // of the loop and didn't add any records to the current buffer, so we need
        // to roll-back the updates to the output pg and index.
        if (out_pg_idx == 0 && out_rec_idx == 0) {
            out_pg_idx = out_buffer_sz;
            out_pnum -= out_buffer_sz;
        }

        // If we are creating the first level, the last leaf page may not have been
        // full, in which case we need to subtract the difference from the last
        // internal page's record count in its header.
        get_header(out_buffer, out_pg_idx)->leaf_rec_cnt -= ((pl_recs_per_pg - (*pl_final_pg_rec_cnt)) * first_level);

        // Write any remaining data from the buffer.
        if (!pfile->allocate_pages(out_pg_idx+1)) {
            goto error;
        }

        if (!pfile->write_pages(out_pnum, out_pg_idx+1, out_buffer)) {
            goto error;
        }

        *pl_first_pg = nl_first_pg;
        *pl_final_pg_rec_cnt = (out_rec_idx) ? out_rec_idx : 0;

        // return the number of pages on the newly created level.
        return (out_pnum + out_pg_idx) - nl_first_pg + 1;

        error:
            return INVALID_PNUM;
    }

    static PageNum pre_init(size_t record_count, size_t tombstone_count, const gsl_rng *rng, PagedFile *pfile, char **buffer) {
        // Allocate initial pages for data and for metadata
        size_t leaf_page_cnt = (record_count / ISAM_RECORDS_PER_LEAF) + ((record_count % ISAM_RECORDS_PER_LEAF) != 0);

        PageNum meta = pfile->allocate_pages(1); // Should be page 1
        PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt); // should start at page 1

        assert(*buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * ISAM_INIT_BUFFER_SIZE));
        assert(meta == BTREE_META_PNUM && first_leaf == BTREE_FIRST_LEAF_PNUM);

        return leaf_page_cnt;
    }

    static PageNum write_final_buffer(size_t output_idx, PageNum cur_pnum, size_t *last_leaf_rec_cnt, PagedFile *pfile, char *buffer) {
        size_t full_leaf_pages = (output_idx) / (PAGE_SIZE / sizeof(record_t));
        *last_leaf_rec_cnt = (output_idx) % (PAGE_SIZE / sizeof(record_t));

        if (full_leaf_pages == 0 && *last_leaf_rec_cnt == 0) {
            return cur_pnum - 1;
        }

        if (pfile->write_pages(cur_pnum, full_leaf_pages + ((*last_leaf_rec_cnt == 0) ? 0 : 1), buffer)) {
            if (full_leaf_pages == 0) {
                return cur_pnum;
            }

            return cur_pnum + full_leaf_pages - (*last_leaf_rec_cnt == 0);
        }

        assert(false);
    }

    static bool post_init(size_t record_count, size_t tombstone_count, PageNum last_leaf, PageNum root_pnum, char* buffer, PagedFile *pfile) {
        memset(buffer, 0, PAGE_SIZE);

        auto metadata = (ISAMTreeMetaHeader *) buffer;
        metadata->root_node = root_pnum;
        metadata->first_data_page = BTREE_FIRST_LEAF_PNUM;
        metadata->last_data_page = last_leaf;
        metadata->tombstone_count = tombstone_count;
        metadata->record_count = record_count;

        return pfile->write_page(BTREE_META_PNUM, buffer);
    }


    inline size_t max_leaf_record_idx(PageNum pnum) {
        if (pnum == this->last_data_page) {
            size_t excess_records = rec_cnt % (PAGE_SIZE / sizeof(record_t));

            if (excess_records > 0) {
                return excess_records - 1;
            }
        }

        return (PAGE_SIZE/sizeof(record_t)) - 1;
    }
};

}
