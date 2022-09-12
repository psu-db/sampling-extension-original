/*
 *
 */

#include "ds/IsamTree.h"

namespace lsm { namespace ds {

ISAMTree *ISAMTree::create(PagedFile *pfile, ISAMTree *tree1, ISAMTree *tree2, gsl_rng *rng)
{
    auto iter1 = tree1->start_scan();
    auto iter2 = tree2->start_scan();

    auto filter = ISAMTree::initialize(pfile, iter1, tree1->rec_cnt, iter2, tree2->rec_cnt, tree1->tombstone_cnt + tree2->tombstone_cnt, rng);

    delete iter1;
    delete iter2;

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter); 
}


ISAMTree *ISAMTree::create(PagedFile *pfile, MemTable *mtable, ISAMTree *tree2, gsl_rng *rng)
{
    auto sortedrun1 = mtable->sorted_output();
    auto iter2 = tree2->start_scan();

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), iter2, tree2->rec_cnt, mtable->tombstone_count() + tree2->tombstone_cnt, rng);
    delete iter2;

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter);
}


ISAMTree *ISAMTree::create(PagedFile *pfile, MemTable *mtable, MemISAMTree *tree, gsl_rng *rng)
{
    auto sortedrun1 = mtable->sorted_output();
    auto sortedrun2 = tree->start_scan();

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), sortedrun2, tree->get_record_count(), mtable->tombstone_count() + tree->tombstone_count(), rng);

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter);
}


ISAMTree *ISAMTree::create(PagedFile *pfile, MemISAMTree *tree1, MemISAMTree *tree2, gsl_rng *rng)
{
    auto sortedrun1 = tree1->start_scan();
    auto sortedrun2 = tree2->start_scan();

    auto filter = ISAMTree::initialize(pfile, sortedrun1, tree1->get_record_count(), sortedrun2, tree2->get_record_count(), tree1->tombstone_count() + tree2->tombstone_count(), rng);

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter);
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, PagedFileIterator *iter1, size_t iter1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    size_t record_count = iter1_rec_cnt + iter2_rec_cnt;

    auto buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * ISAM_INIT_BUFFER_SIZE);

    // Allocate initial pages for data and for metadata
    size_t leaf_page_cnt = (record_count / ISAM_RECORDS_PER_LEAF) + (record_count % ISAM_RECORDS_PER_LEAF != 0);

    PageNum meta = pfile->allocate_pages(); // Should be page 0
    PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt); // should start at page 1

    if (meta == INVALID_PNUM || first_leaf == INVALID_PNUM) {
        free(buffer);
        return nullptr;
    }

    BloomFilter *tomb_filter = new BloomFilter(BF_FPR, tombstone_count, BF_HASH_FUNCS, rng);

    PageNum cur_leaf_pnum = first_leaf;

    char *iter1_page = nullptr;
    char *iter2_page = nullptr;

    size_t iter1_rec_idx = 0;
    size_t iter2_rec_idx = 0;
    size_t output_idx = 0;

    size_t iter1_records = 0;
    size_t iter2_records = 0;


    bool iter1_page_done = true;
    bool iter2_page_done = true;

    if (iter1->next()) {
        iter1_page = iter1->get_item();
        iter1_page_done = false;
    }

    if (iter2->next()) {
        iter2_page = iter2->get_item();
        iter2_page_done = false;
    }

    // if neither iterator contains any items, then there is no merging work to
    // be done.
    while (!iter1_page_done || !iter2_page_done) {
        char *rec1 = (!iter1_page_done) ? iter1_page + (record_size * iter1_rec_idx) : nullptr;
        char *rec2 = (!iter2_page_done) ? iter2_page + (record_size * iter2_rec_idx) : nullptr;

        char *to_copy;
        if (iter1_page_done || (!iter2_page_done && record_cmp(rec1, rec2) == 1)) {
            to_copy = rec2;
            iter2_rec_idx++;
            iter2_rec_cnt++;
            iter2_page_done = iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt;

            if (iter2_page_done) {
                iter2_page = (iter2->next()) ? iter2->get_item() : nullptr;
                iter2_rec_idx = 0;
                if (iter2_page) {
                    iter2_page_done = false;
                }
            }
        } else {
            to_copy = rec1;
            iter1_rec_idx++;
            iter1_rec_cnt++;
            iter1_page_done = iter1_rec_idx >= ISAM_RECORDS_PER_LEAF || iter1_records >= iter1_rec_cnt;

            if (iter1_page_done) {
                iter1_page = (iter1->next()) ? iter1->get_item() : nullptr;
                iter1_rec_idx = 0;
                if (iter1_page) {
                    iter1_page_done = false;
                }
            } 
        }

        memcpy(buffer + (output_idx++ * record_size), to_copy, record_size);

        if (is_tombstone(to_copy)) {
            tomb_filter->insert((char *) get_key(to_copy), key_size);            
        }

        if (output_idx >= ISAM_INIT_BUFFER_SIZE * ISAM_RECORDS_PER_LEAF) {
            pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer);
            output_idx = 0;
            cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
        }
    }

    // Write the excess leaf data to the file
    size_t full_leaf_pages = (output_idx - 1) / PAGE_SIZE;
    size_t excess_records = (output_idx - 1) % PAGE_SIZE;

    pfile->write_pages(cur_leaf_pnum, full_leaf_pages + ((excess_records == 0) ? 0 : 1), buffer);
    cur_leaf_pnum += (full_leaf_pages) + ((excess_records == 0) ? 0 : 1);
    
    auto root_pnum = ISAMTree::generate_internal_levels(pfile, first_leaf, excess_records, buffer, ISAM_INIT_BUFFER_SIZE);

    pfile->read_page(meta, buffer);
    auto metadata = (ISAMTreeMetaHeader *) buffer;
    metadata->root_node = root_pnum;
    metadata->first_data_page = first_leaf;
    metadata->last_data_page = cur_leaf_pnum;
    metadata->tombstone_count = tombstone_count;
    metadata->record_count = record_count;
    pfile->write_page(meta, buffer);

    free(buffer);
    return tomb_filter;
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, char *sorted_run2, size_t run2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    size_t record_count = run1_rec_cnt + run2_rec_cnt;

    auto buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * ISAM_INIT_BUFFER_SIZE);

    // Allocate initial pages for data and for metadata
    size_t leaf_page_cnt = (record_count / ISAM_RECORDS_PER_LEAF) + (record_count % ISAM_RECORDS_PER_LEAF != 0);

    PageNum meta = pfile->allocate_pages(); // Should be page 0
    PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt); // should start at page 1

    if (meta == INVALID_PNUM || first_leaf == INVALID_PNUM) {
        free(buffer);
        return nullptr;
    }

    BloomFilter *tomb_filter = new BloomFilter(BF_FPR, tombstone_count, BF_HASH_FUNCS, rng);

    PageNum cur_leaf_pnum = first_leaf;

    size_t run1_rec_idx = 0;
    size_t run2_rec_idx = 0;
    size_t output_idx = 0;

    while (run1_rec_idx < run1_rec_cnt || run2_rec_idx < run2_rec_cnt) {
        char *rec1 = (run1_rec_idx < run1_rec_cnt) ? sorted_run1 + (record_size * run1_rec_idx) : nullptr;
        char *rec2 = (run2_rec_idx < run2_rec_cnt) ? sorted_run2 + (record_size * run2_rec_idx) : nullptr;
        
        char *to_copy;
        if (!rec1 || (rec2 && record_cmp(rec1, rec2) == 1)) {
            to_copy = rec2;
            run2_rec_idx++;
        } else {
            to_copy = rec1;
            run1_rec_idx++;
        }

        memcpy(buffer + (output_idx++ * record_size), to_copy, record_size);

        if (is_tombstone(to_copy)) {
            tomb_filter->insert((char *) get_key(to_copy), key_size);            
        }

        if (output_idx >= ISAM_INIT_BUFFER_SIZE * ISAM_RECORDS_PER_LEAF) {
            pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer);
            output_idx = 0;
            cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
        }
    }

    // Write the excess leaf data to the file
    size_t full_leaf_pages = (output_idx - 1) / PAGE_SIZE;
    size_t excess_records = (output_idx - 1) % PAGE_SIZE;

    pfile->write_pages(cur_leaf_pnum, full_leaf_pages + ((excess_records == 0) ? 0 : 1), buffer);
    cur_leaf_pnum += (full_leaf_pages) + ((excess_records == 0) ? 0 : 1);
    
    auto root_pnum = ISAMTree::generate_internal_levels(pfile, first_leaf, excess_records, buffer, ISAM_INIT_BUFFER_SIZE);

    pfile->read_page(meta, buffer);
    auto metadata = (ISAMTreeMetaHeader *) buffer;
    metadata->root_node = root_pnum;
    metadata->first_data_page = first_leaf;
    metadata->last_data_page = cur_leaf_pnum;
    metadata->tombstone_count = tombstone_count;
    metadata->record_count = record_count;
    pfile->write_page(meta, buffer);

    free(buffer);
    return tomb_filter;
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    size_t record_count = run1_rec_cnt + iter2_rec_cnt;

    auto buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * ISAM_INIT_BUFFER_SIZE);

    // Allocate initial pages for data and for metadata
    size_t leaf_page_cnt = (record_count / ISAM_RECORDS_PER_LEAF) + (record_count % ISAM_RECORDS_PER_LEAF != 0);

    PageNum meta = pfile->allocate_pages(); // Should be page 0
    PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt); // should start at page 1

    if (meta == INVALID_PNUM || first_leaf == INVALID_PNUM) {
        free(buffer);
        return nullptr;
    }

    BloomFilter *tomb_filter = new BloomFilter(BF_FPR, tombstone_count, BF_HASH_FUNCS, rng);

    PageNum cur_leaf_pnum = first_leaf;

    size_t run1_rec_idx = 0;

    char *iter2_page = nullptr;
    size_t iter2_rec_idx = 0;
    size_t iter2_records = 0;
    bool iter2_page_done = true;

    size_t output_idx = 0;

    if (iter2->next()) {
        iter2_page = iter2->get_item();
        iter2_page_done = false;
    }

    while (run1_rec_idx < run1_rec_cnt || !iter2_page_done) {
        char *rec1 = (run1_rec_idx < run1_rec_cnt) ? sorted_run1 + (record_size * run1_rec_idx) : nullptr;
        char *rec2 = (!iter2_page_done) ? iter2_page + (record_size * iter2_rec_idx) : nullptr;
        
        char *to_copy;
        if (!rec1 || (rec2 && record_cmp(rec1, rec2) == 1)) {
            to_copy = rec2;
            iter2_rec_idx++;
            iter2_rec_cnt++;

            iter2_page_done = iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt;

            if (iter2_page_done) {
                iter2_page = (iter2->next()) ? iter2->get_item() : nullptr;
                iter2_rec_idx = 0;
                if (iter2_page) {
                    iter2_page_done = false;
                }
            }
        } else {
            to_copy = rec1;
            run1_rec_idx++;
        }

        memcpy(buffer + (output_idx++ * record_size), to_copy, record_size);

        if (is_tombstone(to_copy)) {
            tomb_filter->insert((char *) get_key(to_copy), key_size);            
        }

        if (output_idx >= ISAM_INIT_BUFFER_SIZE * ISAM_RECORDS_PER_LEAF) {
            pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer);
            output_idx = 0;
            cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
        }
    }

    // Write the excess leaf data to the file
    size_t full_leaf_pages = (output_idx - 1) / PAGE_SIZE;
    size_t excess_records = (output_idx - 1) % PAGE_SIZE;

    pfile->write_pages(cur_leaf_pnum, full_leaf_pages + ((excess_records == 0) ? 0 : 1), buffer);
    cur_leaf_pnum += (full_leaf_pages) + ((excess_records == 0) ? 0 : 1);
    
    auto root_pnum = ISAMTree::generate_internal_levels(pfile, first_leaf, excess_records, buffer, ISAM_INIT_BUFFER_SIZE);

    pfile->read_page(meta, buffer);
    auto metadata = (ISAMTreeMetaHeader *) buffer;
    metadata->root_node = root_pnum;
    metadata->first_data_page = first_leaf;
    metadata->last_data_page = cur_leaf_pnum;
    metadata->tombstone_count = tombstone_count;
    metadata->record_count = record_count;
    pfile->write_page(meta, buffer);

    free(buffer);
    return tomb_filter;
}


PageNum ISAMTree::generate_internal_levels(PagedFile *pfile, PageNum first_leaf_page, size_t final_leaf_rec_cnt, char *buffer, size_t buffer_sz)
{
    size_t leaf_recs_per_page = PAGE_SIZE / record_size;
    size_t int_recs_per_page = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

    // FIXME: There're some funky edge cases here if the input_buffer_sz is larger
    // than the number of leaf pages
    size_t input_buffer_sz = 1;
    auto input_buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * input_buffer_sz);

    // First, we generate the initial internal level. This processes a slightly
    // different data format than subsequent internal levels, so we'll do it
    // separately.
    PageNum cur_output_pnum = pfile->allocate_pages(buffer_sz);
    PageNum cur_input_pnum = first_leaf_page;
    PageNum last_leaf_page = cur_output_pnum - 1;

    size_t cur_int_page_idx = 0;
    size_t cur_int_rec_idx = 0;

    size_t last_leaf_rec_offset = leaf_recs_per_page * record_size;

    do {
        pfile->read_pages(cur_input_pnum, input_buffer_sz, input_buffer);
        for (size_t i=0; i<input_buffer_sz; i++) { 
            char *last_record = input_buffer + (PAGE_SIZE * i) + last_leaf_rec_offset;
            const char *key = get_key(last_record);

            char *internal_buff = buffer + (cur_int_page_idx * PAGE_SIZE) + (cur_int_rec_idx++ * internal_record_size) + ISAMTreeInternalNodeHeaderSize;
            build_internal_record(internal_buff, key, cur_input_pnum);

            if (cur_int_rec_idx >= int_recs_per_page) {
                cur_int_rec_idx = 0;
                cur_int_page_idx++;
                if (cur_int_page_idx >= buffer_sz) {
                    pfile->write_pages(cur_output_pnum, buffer_sz, buffer);
                    cur_output_pnum += buffer_sz;
                    cur_int_page_idx = 0;
                }
            }
            cur_input_pnum++;
        }
    } while (cur_input_pnum < last_leaf_page);
    // Note: We'll process the last leaf page separately to avoid
    // an if-statement within the loop.

    /*
    auto input_page = io::FixedlenDataPage(input_buf.get());
    auto input_header = (ISAMTreeInternalNodeHeader *) input_page.get_user_data();
    while (input_header->next_sibling != INVALID_PNUM) {
        auto new_output_pid = pfile->allocate_page();
        auto current_output_pid = new_output_pid;

        io::FixedlenDataPage::initialize(output_buf.get(), schema->record_size(), ISAMTreeInternalNodeHeaderSize);
        auto output_page = io::FixedlenDataPage(output_buf.get());
        auto output_header = (ISAMTreeInternalNodeHeader *) output_page.get_user_data();
        output_header->prev_sibling = INVALID_PNUM;
        output_header->next_sibling = INVALID_PNUM;
        output_header->leaf_rec_cnt = 0;

        while (true) {
            // move the last key value on this node up to the next level of the tree
            auto key_record = input_page.get_record(input_page.get_max_sid());
            auto key = schema->get_key(key_record.get_data()).chars();

            auto new_record_buf = schema->create_record(key, (char *) &current_pnum);
            auto new_record = io::Record(new_record_buf.get(), schema->record_size());

            // insert record into output page, writing the old and creating a new if
            // it is full
            if (output_page.insert_record(new_record)) {
                output_header->leaf_rec_cnt += ((ISAMTreeInternalNodeHeader *) input_page.get_user_data())->leaf_rec_cnt;
            } else {
                auto new_pid = pfile->allocate_page();
                output_header->next_sibling = new_pid.page_number;
                pfile->write_page(current_output_pid, output_buf.get());

                io::FixedlenDataPage::initialize(output_buf.get(), schema->record_size(), ISAMTreeInternalNodeHeaderSize);
                output_page = io::FixedlenDataPage(output_buf.get());
                output_header = (ISAMTreeInternalNodeHeader *) output_page.get_user_data();
                output_header->prev_sibling = current_output_pid.page_number;
                output_header->next_sibling = INVALID_PNUM;
                output_header->leaf_rec_cnt = ((ISAMTreeInternalNodeHeader *) input_page.get_user_data())->leaf_rec_cnt;

                output_page.insert_record(new_record);
                current_output_pid = new_pid;
            }
                
            // advance to next page at this level
            if (input_header->next_sibling == INVALID_PNUM) {
                break;
            }

            current_pnum = input_header->next_sibling;

            pfile->read_page(current_pnum, input_buf.get());
            input_page = io::FixedlenDataPage(input_buf.get());
            input_header = (ISAMTreeInternalNodeHeader *) input_page.get_user_data();
        } 

        // Make sure that the output buffer is flushed before the next iteration
        pfile->write_page(current_output_pid, output_buf.get());

        // Set up to begin processing the newly created level in the next iteration
        current_pnum = new_output_pid.page_number;
        pfile->read_page(current_pnum, input_buf.get());
        input_page = io::FixedlenDataPage(input_buf.get());
        input_header = (ISAMTreeInternalNodeHeader *) input_page.get_user_data();
    }
    */

    free(input_buffer);
    // The last pnum processed will belong to the page in the level with only 1 node,
    // i.e., the root node.
    return cur_output_pnum;
}


ISAMTree::ISAMTree(PagedFile *pfile, BloomFilter *tombstone_filter) :
            pfile(pfile), tombstone_bloom_filter(tombstone_filter), 
            internal_buffer((char*) aligned_alloc(SECTOR_SIZE, PAGE_SIZE))
{
    this->pfile->read_page(BTREE_META_PNUM, this->internal_buffer);
    auto meta = (ISAMTreeMetaHeader *) this->internal_buffer;
    this->root_page = meta->root_node;
    this->first_data_page = meta->first_data_page;
    this->last_data_page = meta->last_data_page;
    this->rec_cnt = meta->record_count;
    this->tombstone_cnt = meta->tombstone_count;
}


ISAMTree::~ISAMTree()
{
    this->pfile->remove_file();
    free(this->internal_buffer);
    delete this->tombstone_bloom_filter;
}


PageNum ISAMTree::get_lower_bound(const char *key, char *buffer)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_lower(current_page, key, buffer);
    }

    return current_page;
}


PageNum ISAMTree::get_upper_bound(const char *key, char *buffer)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_upper(current_page, key, buffer);
    }

    // It is possible, because the internal records contain max values for each
    // run, that an adjacent page to the one reported above may contain valid
    // keys within the range. This can only occur in this case of the internal
    // key being equal to the boundary key, and the next page containing
    // duplicate values of that same key.
    if (current_page < this->last_data_page && current_page != INVALID_PNUM) {
        if (this->search_leaf_page(current_page + 1, key, buffer) != 0) {
            current_page = current_page + 1;
        }
    }
        

    return current_page;
}


PageNum ISAMTree::search_internal_node_lower(PageNum pnum, const char *key, char *buffer)
{
    this->pfile->read_page(pnum, buffer);

    size_t min = 0;
    size_t max = ((ISAMTreeInternalNodeHeader *) buffer)->internal_rec_nt - 1;

    // If the entire range of numbers falls below the target key, the algorithm
    // will return max as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = get_internal_key(get_internal_record(buffer, max));
    if (key_cmp(key, node_key) > 0) {
        return INVALID_PNUM;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto node_key = get_internal_key(get_internal_record(buffer, mid));
        if (key_cmp(key, node_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return get_internal_value(get_internal_record(buffer, min));
}


PageNum ISAMTree::search_internal_node_upper(PageNum pnum, const char *key, char *buffer)
{
    this->pfile->read_page(pnum, buffer);

    size_t min = 0;
    size_t max = ((ISAMTreeInternalNodeHeader *) buffer)->internal_rec_nt - 1;

    // If the entire range of numbers falls below the target key, the algorithm
    // will return max as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = get_internal_key(get_internal_record(buffer, min));
    if (key_cmp(key, node_key) < 0) {
        return INVALID_PNUM;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto node_key = get_internal_key(get_internal_record(buffer, mid));
        if (key_cmp(key, node_key) >= 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return get_internal_value(get_internal_record(buffer, min));
}


char *ISAMTree::search_leaf_page(PageNum pnum, const char *key, char *buffer, size_t *idx)
{
    size_t min = 0;
    size_t max = this->max_leaf_record_idx(pnum);

    this->pfile->read_page(pnum, buffer);
    const char * record_key;

    while (min < max) {
        size_t mid = (min + max) / 2;
        record_key = get_key(buffer + (mid * record_size));

        if (key_cmp(key, record_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    char *record = buffer + (min * record_size);

    // Check if the thing that we found matches the target. If so, we've found
    // it. If not, the target doesn't exist on the page.
    if (key_cmp(key, get_key(record)) == 0) {
        if (idx) {
            *idx = min;
        }

        return record;
    }

    return nullptr;
}


char *ISAMTree::get(const char *key, char *buffer)
{
    // find the first page to search
    auto pnum = this->get_lower_bound(key, buffer);
    auto record = this->search_leaf_page(pnum, key, buffer);

    if (is_tombstone(record)) {
        return nullptr;
    }

    // Because we aren't using a cache, we need to manually manage these
    // objects. The internal buffer is subject to immediate reuse, so we
    // need to copy the record out.
    return copy_of(record);

    // All the extra code here from the earlier version is for scanning over
    // sequential matching records with identical timestamps. Without
    // timestamps, the record found will either match, or not, and that is
    // final.
}


char *ISAMTree::get_tombstone(const char *key, const char *val, char *buffer)
{
    if (!this->tombstone_bloom_filter->lookup((char *) key, record_size)) {
        return nullptr;
    }

    auto pnum = this->get_lower_bound(key, buffer);

    do {
        size_t idx;
        auto record = this->search_leaf_page(pnum, key, buffer, &idx);

        if (!record) {
            return nullptr;
        }

        for (size_t i=idx; i<this->max_leaf_record_idx(pnum); i++) {
            auto rec = buffer + (idx * record_size);
            if (key_cmp(get_key(rec), key) != 0) {
                return nullptr;
            }

            if (record_match(rec, key, val, true)) {
                return copy_of(rec);
            }
        }

        pnum++;
    } while (pnum <= this->last_data_page);

    return nullptr;
}
}}
