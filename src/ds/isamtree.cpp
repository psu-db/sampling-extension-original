/*
 *
 */

#include "ds/isamtree.hpp"

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

    auto filter = ISAMTree::initialize(pfile, (byte *) sortedrun1, mtable->get_record_count(), iter2, tree2->rec_cnt, mtable->tombstone_count() + tree2->tombstone_cnt, rng);
    delete iter2;

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter);
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, PagedFileIterator *iter1, size_t iter1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    size_t buffer_sz = 1; // measured in pages
    size_t records_per_page = PAGE_SIZE / RECORDLEN;
    size_t record_count = iter1_rec_cnt + iter2_rec_cnt;

    auto buffer = (byte *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * buffer_sz);

    // Allocate initial pages for data and for metadata
    size_t leaf_page_cnt = (record_count / records_per_page) + (record_count % records_per_page != 0);

    PageNum meta = pfile->allocate_pages();
    PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt);

    if (meta == INVALID_PNUM || first_leaf == INVALID_PNUM) {
        free(buffer);
        return nullptr;
    }

    BloomFilter *tomb_filter = new BloomFilter(BF_FPR, tombstone_count, BF_HASH_FUNCS, rng);

    PageNum cur_leaf_pnum = first_leaf;

    byte *iter1_page = nullptr;
    byte *iter2_page = nullptr;

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
        byte *rec1 = (!iter1_page_done) ? iter1_page + (RECORDLEN * iter1_rec_idx) : nullptr;
        byte *rec2 = (!iter2_page_done) ? iter2_page + (RECORDLEN * iter2_rec_idx) : nullptr;

        byte *to_copy;
        // FIXME: This is really gross. I was considering placing null check code within
        // RECCMP with null comparing greater, but that isn't a perfect solution either as
        // it still requires a null check (as both of them being null will be equal)
        if (iter1_page_done) {
            to_copy = rec2;
            iter2_rec_idx++;
            iter2_rec_cnt++;
            iter2_page_done = iter2_rec_idx >= records_per_page || iter2_records >= iter2_rec_cnt;
        } else if (iter2_page_done) {
            to_copy = rec1;
            iter1_rec_idx++;
            iter1_rec_cnt++;
            iter1_page_done = iter1_rec_idx >= records_per_page || iter1_records >= iter1_rec_cnt;
        } else if (RECCMP(rec1, rec2) == 1) {
            to_copy = rec2;
            iter2_rec_idx++;
            iter2_rec_cnt++;
            iter2_page_done = iter2_rec_idx >= records_per_page || iter2_records >= iter2_rec_cnt;
        } else {
            to_copy = rec1;
            iter1_rec_idx++;
            iter1_rec_cnt++;
            iter1_page_done = iter1_rec_idx >= records_per_page || iter1_records >= iter1_rec_cnt;
        }

        memcpy(buffer + (output_idx++ * RECORDLEN), to_copy, RECORDLEN);

        if (ISTOMBSTONE(to_copy)) {
            tomb_filter->insert((char *) GETKEY(to_copy), KEYLEN);            
        }

        if (output_idx >= buffer_sz * records_per_page) {
            pfile->write_pages(cur_leaf_pnum, buffer_sz, buffer);
            output_idx = 0;
            cur_leaf_pnum += buffer_sz;
        }

        if (iter1_page_done) {
            iter1_page = (iter1->next()) ? iter1->get_item() : nullptr;
            iter1_rec_idx = 0;
            if (iter1_page) {
                iter1_page_done = false;
            }
        } 

        if (iter2_page_done) {
            iter2_page = (iter2->next()) ? iter2->get_item() : nullptr;
            iter2_rec_idx = 0;
            if (iter2_page) {
                iter2_page_done = false;
            }
        }
    }

    // Write the excess leaf data to the file
    size_t full_leaf_pages = (output_idx - 1) / PAGE_SIZE;
    size_t excess_records = (output_idx - 1) % PAGE_SIZE;

    pfile->write_pages(cur_leaf_pnum, full_leaf_pages + ((excess_records == 0) ? 0 : 1), buffer);
    cur_leaf_pnum += (full_leaf_pages) + ((excess_records == 0) ? 0 : 1);
    
    auto root_pnum = ISAMTree::generate_internal_levels(pfile, first_leaf, excess_records, buffer, buffer_sz);

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


PageNum ISAMTree::generate_internal_levels(PagedFile *pfile, PageNum first_leaf_page, size_t final_leaf_rec_cnt, byte *buffer, size_t buffer_sz)
{
    size_t leaf_recs_per_page = PAGE_SIZE / RECORDLEN;
    size_t int_recs_per_page = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

    // FIXME: There're some funky edge cases here if the input_buffer_sz is larger
    // than the number of leaf pages
    size_t input_buffer_sz = 1;
    auto input_buffer = (byte *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * input_buffer_sz);

    // First, we generate the initial internal level. This processes a slightly
    // different data format than subsequent internal levels, so we'll do it
    // separately.
    PageNum cur_output_pnum = pfile->allocate_pages(buffer_sz);
    PageNum cur_input_pnum = first_leaf_page;
    PageNum last_leaf_page = cur_output_pnum - 1;

    size_t cur_int_page_idx = 0;
    size_t cur_int_rec_idx = 0;

    size_t last_leaf_rec_offset = leaf_recs_per_page * RECORDLEN;

    do {
        pfile->read_pages(cur_input_pnum, input_buffer_sz, input_buffer);
        for (size_t i=0; i<input_buffer_sz; i++) { 
            byte *last_record = input_buffer + (PAGE_SIZE * i) + last_leaf_rec_offset;
            byte *key = GETKEY(last_record);

            byte *internal_buff = buffer + (cur_int_page_idx * PAGE_SIZE) + (cur_int_rec_idx++ * internal_record_size) + ISAMTreeInternalNodeHeaderSize;
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

        io::FixedlenDataPage::initialize(output_buf.get(), schema->record_length(), ISAMTreeInternalNodeHeaderSize);
        auto output_page = io::FixedlenDataPage(output_buf.get());
        auto output_header = (ISAMTreeInternalNodeHeader *) output_page.get_user_data();
        output_header->prev_sibling = INVALID_PNUM;
        output_header->next_sibling = INVALID_PNUM;
        output_header->leaf_rec_cnt = 0;

        while (true) {
            // move the last key value on this node up to the next level of the tree
            auto key_record = input_page.get_record(input_page.get_max_sid());
            auto key = schema->get_key(key_record.get_data()).Bytes();

            auto new_record_buf = schema->create_record(key, (byte *) &current_pnum);
            auto new_record = io::Record(new_record_buf.get(), schema->record_length());

            // insert record into output page, writing the old and creating a new if
            // it is full
            if (output_page.insert_record(new_record)) {
                output_header->leaf_rec_cnt += ((ISAMTreeInternalNodeHeader *) input_page.get_user_data())->leaf_rec_cnt;
            } else {
                auto new_pid = pfile->allocate_page();
                output_header->next_sibling = new_pid.page_number;
                pfile->write_page(current_output_pid, output_buf.get());

                io::FixedlenDataPage::initialize(output_buf.get(), schema->record_length(), ISAMTreeInternalNodeHeaderSize);
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
            internal_buffer((byte*) aligned_alloc(SECTOR_SIZE, PAGE_SIZE))
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


PageNum ISAMTree::get_lower_bound(byte *key)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_lower(current_page, key);
    }

    return current_page;
}


PageNum ISAMTree::get_upper_bound(byte *key)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_upper(current_page, key);
    }

    // It is possible, because the internal records contain max values for each
    // run, that an adjacent page to the one reported above may contain valid
    // keys within the range. This can only occur in this case of the internal
    // key being equal to the boundary key, and the next page containing
    // duplicate values of that same key.
    if (current_page < this->last_data_page && current_page != INVALID_PNUM) {
        if (this->search_leaf_page(current_page + 1, key) != 0) {
            current_page = current_page + 1;
        }
    }
        

    return current_page;
}


PageNum ISAMTree::search_internal_node_lower(PageNum pnum, byte *key)
{
    this->pfile->read_page(pnum, this->internal_buffer);

    size_t min = 0;
    size_t max = ((ISAMTreeInternalNodeHeader *) this->internal_buffer)->internal_rec_nt - 1;

    // If the entire range of numbers falls below the target key, the algorithm
    // will return max as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = get_internal_key(get_internal_record(this->internal_buffer, max));
    if (KEYCMP(key, node_key) > 0) {
        return INVALID_PNUM;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto node_key = get_internal_key(get_internal_record(this->internal_buffer, mid));
        if (KEYCMP(key, node_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return get_internal_value(get_internal_record(this->internal_buffer, min));
}


PageNum ISAMTree::search_internal_node_upper(PageNum pnum, byte *key)
{
    this->pfile->read_page(pnum, this->internal_buffer);

    size_t min = 0;
    size_t max = ((ISAMTreeInternalNodeHeader *) this->internal_buffer)->internal_rec_nt - 1;

    // If the entire range of numbers falls below the target key, the algorithm
    // will return max as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = get_internal_key(get_internal_record(this->internal_buffer, min));
    if (KEYCMP(key, node_key) < 0) {
        return INVALID_PNUM;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto node_key = get_internal_key(get_internal_record(this->internal_buffer, mid));
        if (KEYCMP(key, node_key) >= 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return get_internal_value(get_internal_record(this->internal_buffer, min));
}


byte *ISAMTree::search_leaf_page(PageNum pnum, byte *key, size_t *idx)
{
    size_t min = 0;
    size_t max = this->max_leaf_record_idx(pnum);

    this->pfile->read_page(pnum, this->internal_buffer);
    byte * record_key;

    while (min < max) {
        size_t mid = (min + max) / 2;
        record_key = GETKEY(this->internal_buffer + (mid * RECORDLEN));

        if (KEYCMP(key, record_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    byte *record = this->internal_buffer + (min * RECORDLEN);

    // Check if the thing that we found matches the target. If so, we've found
    // it. If not, the target doesn't exist on the page.
    if (KEYCMP(key, GETKEY(record)) == 0) {
        if (idx) {
            *idx = min;
        }

        return record;
    }

    return nullptr;
}


byte *ISAMTree::get(byte *key)
{
    // find the first page to search
    auto pnum = this->get_lower_bound(key);
    auto record = this->search_leaf_page(pnum, key);

    if (ISTOMBSTONE(record)) {
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


byte *ISAMTree::get_tombstone(byte *key, byte *val)
{
    if (!this->tombstone_bloom_filter->lookup((char *) key, RECORDLEN)) {
        return nullptr;
    }

    auto pnum = this->get_lower_bound(key);

    do {
        size_t idx;
        auto record = this->search_leaf_page(pnum, key, &idx);

        if (!record) {
            return nullptr;
        }

        for (size_t i=idx; i<this->max_leaf_record_idx(pnum); i++) {
            auto rec = this->internal_buffer + (idx * RECORDLEN);
            if (KEYCMP(GETKEY(rec), key) != 0) {
                return nullptr;
            }

            if (ISTOMBSTONE(rec) && VALCMP(GETVAL(rec), val) == 0) {
                return copy_of(rec);
            }
        }

        pnum++;
    } while (pnum <= this->last_data_page);

    return nullptr;
}
}}
