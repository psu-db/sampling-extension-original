/*
 *
 */

#include "ds/IsamTree.h"

namespace lsm {

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


ISAMTree *ISAMTree::create(PagedFile *pfile, MemTable *mtable, gsl_rng *rng)
{
    auto sortedrun1 = mtable->sorted_output();

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), mtable->tombstone_count(), rng);

    if (!filter) {
        return nullptr;
    }

    return new ISAMTree(pfile, filter);
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, PagedFileIterator *iter1, size_t iter1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    BloomFilter *tomb_filter = nullptr;
    char *buffer = nullptr;
    PageNum last_leaf = INVALID_PNUM;
    PageNum root_pnum = INVALID_PNUM;
    size_t last_leaf_record_cnt = 0;
    size_t record_count = iter1_rec_cnt + iter2_rec_cnt;

    PageNum leaf_page_cnt = ISAMTree::pre_init(record_count, tombstone_count, rng, pfile, &tomb_filter, &buffer);
    if (leaf_page_cnt == 0) {
        goto error;
    }

    {
        PageNum cur_leaf_pnum = BTREE_FIRST_LEAF_PNUM;

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
                if (!pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer)) {
                    goto error_filter;
                }
                output_idx = 0;
                cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
            }
        }

        // Write the excess leaf data to the file
        last_leaf = ISAMTree::write_final_buffer(output_idx, cur_leaf_pnum, &last_leaf_record_cnt, pfile, buffer);
        if (last_leaf == INVALID_PNUM) {
            goto error_filter;
        }
    }
    
    root_pnum = ISAMTree::generate_internal_levels(pfile, last_leaf_record_cnt, buffer, ISAM_INIT_BUFFER_SIZE);

    if (!ISAMTree::post_init(record_count, tombstone_count, last_leaf, root_pnum, buffer, pfile)) {
        goto error_filter;
    }

    free(buffer);
    return tomb_filter;

error_filter:
    delete tomb_filter;
error_buffer:
    free(buffer);
error:
    return nullptr;
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, char *sorted_run2, size_t run2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    BloomFilter *tomb_filter = nullptr;
    char *buffer = nullptr;
    PageNum last_leaf = INVALID_PNUM;
    PageNum root_pnum = INVALID_PNUM;
    size_t last_leaf_record_cnt = 0;
    size_t record_count = run1_rec_cnt + run2_rec_cnt;

    PageNum leaf_page_cnt = ISAMTree::pre_init(record_count, tombstone_count, rng, pfile, &tomb_filter, &buffer);
    if (leaf_page_cnt == 0) {
        goto error;
    }

    {
        PageNum cur_leaf_pnum = BTREE_FIRST_LEAF_PNUM;

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
                if (!pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer)) {
                    goto error_filter;
                }
                output_idx = 0;
                cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
            }
        }

        // Write the excess leaf data to the file
        last_leaf = ISAMTree::write_final_buffer(output_idx, cur_leaf_pnum, &last_leaf_record_cnt, pfile, buffer);
        if (last_leaf == INVALID_PNUM) {
            goto error_filter;
        }
    }
    
    root_pnum = ISAMTree::generate_internal_levels(pfile, last_leaf_record_cnt, buffer, ISAM_INIT_BUFFER_SIZE);

    if (!ISAMTree::post_init(record_count, tombstone_count, last_leaf, root_pnum, buffer, pfile)) {
        goto error_filter;
    }

    free(buffer);
    return tomb_filter;

error_filter:
    delete tomb_filter;
error_buffer:
    free(buffer);
error:
    return nullptr;
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, PagedFileIterator *iter2, size_t iter2_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    BloomFilter *tomb_filter = nullptr;
    char *buffer = nullptr;
    PageNum last_leaf = INVALID_PNUM;
    PageNum root_pnum = INVALID_PNUM;
    size_t last_leaf_record_cnt = 0;
    size_t record_count = run1_rec_cnt + iter2_rec_cnt;

    PageNum leaf_page_cnt = ISAMTree::pre_init(record_count, tombstone_count, rng, pfile, &tomb_filter, &buffer);
    if (leaf_page_cnt == 0) {
        goto error;
    }

    {
        PageNum cur_leaf_pnum = BTREE_FIRST_LEAF_PNUM;
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
                iter2_records++;

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
                if (!pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer)) {
                    goto error_filter;
                }
                output_idx = 0;
                cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
            }
        }

        // Write the excess leaf data to the file
        last_leaf = ISAMTree::write_final_buffer(output_idx, cur_leaf_pnum, &last_leaf_record_cnt, pfile, buffer);
        if (last_leaf == INVALID_PNUM) {
            goto error_filter;
        }
    }
    
    root_pnum = ISAMTree::generate_internal_levels(pfile, last_leaf_record_cnt, buffer, ISAM_INIT_BUFFER_SIZE);

    if (!ISAMTree::post_init(record_count, tombstone_count, last_leaf, root_pnum, buffer, pfile)) {
        goto error_filter;
    }

    free(buffer);
    return tomb_filter;

error_filter:
    delete tomb_filter;
error_buffer:
    free(buffer);
error:
    return nullptr;
}


BloomFilter *ISAMTree::initialize(PagedFile *pfile, char *sorted_run1, size_t run1_rec_cnt, size_t tombstone_count, gsl_rng *rng)
{
    BloomFilter *tomb_filter = nullptr;
    char *buffer = nullptr;
    PageNum last_leaf = INVALID_PNUM;
    PageNum root_pnum = INVALID_PNUM;
    size_t last_leaf_record_cnt = 0;
    size_t record_count = run1_rec_cnt;

    PageNum leaf_page_cnt = ISAMTree::pre_init(record_count, tombstone_count, rng, pfile, &tomb_filter, &buffer);
    if (leaf_page_cnt == 0) {
        goto error;
    }

    {
        PageNum cur_leaf_pnum = BTREE_FIRST_LEAF_PNUM;

        size_t run1_rec_idx = 0;
        size_t output_idx = 0;

        while (run1_rec_idx < run1_rec_cnt) {
            char *rec1 = (run1_rec_idx < run1_rec_cnt) ? sorted_run1 + (record_size * run1_rec_idx) : nullptr;
            run1_rec_idx++;
            
            memcpy(buffer + (output_idx++ * record_size), rec1, record_size);

            if (is_tombstone(rec1)) {
                tomb_filter->insert((char *) get_key(rec1), key_size);            
            }

            if (output_idx >= ISAM_INIT_BUFFER_SIZE * ISAM_RECORDS_PER_LEAF) {
                if (!pfile->write_pages(cur_leaf_pnum, ISAM_INIT_BUFFER_SIZE, buffer)) {
                    goto error_filter;
                }
                output_idx = 0;
                cur_leaf_pnum += ISAM_INIT_BUFFER_SIZE;
            }
        }

        // Write the excess leaf data to the file
        last_leaf = ISAMTree::write_final_buffer(output_idx, cur_leaf_pnum, &last_leaf_record_cnt, pfile, buffer);
        if (last_leaf == INVALID_PNUM) {
            goto error_filter;
        }
    }
    
    root_pnum = ISAMTree::generate_internal_levels(pfile, last_leaf_record_cnt, buffer, ISAM_INIT_BUFFER_SIZE);

    if (!ISAMTree::post_init(record_count, tombstone_count, last_leaf, root_pnum, buffer, pfile)) {
        goto error_filter;
    }

    free(buffer);
    return tomb_filter;

error_filter:
    delete tomb_filter;
error_buffer:
    free(buffer);
error:
    return nullptr;
}


PageNum ISAMTree::pre_init(size_t record_count, size_t tombstone_count, gsl_rng *rng, PagedFile *pfile, BloomFilter **filter, char **buffer)
{
    // Allocate initial pages for data and for metadata
    size_t leaf_page_cnt = (record_count / ISAM_RECORDS_PER_LEAF) + ((record_count % ISAM_RECORDS_PER_LEAF) != 0);

    PageNum meta = pfile->allocate_pages(1); // Should be page 0
    PageNum first_leaf = pfile->allocate_pages(leaf_page_cnt); // should start at page 1

    if(!(*buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * ISAM_INIT_BUFFER_SIZE))) {
        goto error;
    }

    if (meta != BTREE_META_PNUM || first_leaf != BTREE_FIRST_LEAF_PNUM) {
        goto error_buffer;
    }

    *filter = new BloomFilter(BF_FPR, tombstone_count, BF_HASH_FUNCS, rng);

    return leaf_page_cnt;

error_buffer:
    free(buffer);
error:
    return 0;
}


PageNum ISAMTree::write_final_buffer(size_t output_idx, PageNum cur_pnum, size_t *last_leaf_rec_cnt, PagedFile *pfile, char *buffer)
{
    size_t full_leaf_pages = (output_idx) / PAGE_SIZE;
    *last_leaf_rec_cnt = (output_idx) % PAGE_SIZE;

    if (full_leaf_pages == 0 && *last_leaf_rec_cnt == 0) {
        return cur_pnum - 1;
    }

    if (pfile->write_pages(cur_pnum, full_leaf_pages + ((*last_leaf_rec_cnt == 0) ? 0 : 1), buffer)) {
        if (full_leaf_pages == 0) {
            return cur_pnum;
        }

        return cur_pnum + full_leaf_pages - (last_leaf_rec_cnt == 0);
    }

    return INVALID_PNUM;
}


bool ISAMTree::post_init(size_t record_count, size_t tombstone_count, PageNum last_leaf, PageNum root_pnum, char* buffer, PagedFile *pfile)
{
    memset(buffer, 0, PAGE_SIZE);

    auto metadata = (ISAMTreeMetaHeader *) buffer;
    metadata->root_node = root_pnum;
    metadata->first_data_page = BTREE_FIRST_LEAF_PNUM;
    metadata->last_data_page = last_leaf;
    metadata->tombstone_count = tombstone_count;
    metadata->record_count = record_count;

    return pfile->write_page(BTREE_META_PNUM, buffer);
}


PageNum ISAMTree::generate_first_internal_level(PagedFile *pfile, size_t final_leaf_rec_cnt, char *out_buffer, size_t out_buffer_sz, char *in_buffer, size_t in_buffer_sz)
{    
    size_t leaf_recs_per_page = PAGE_SIZE / record_size;
    size_t int_recs_per_page = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

    // First, we generate the initial internal level. This processes a slightly
    // different data format than subsequent internal levels, so we'll do it
    // separately.

    PageNum cur_input_pnum = BTREE_FIRST_LEAF_PNUM;
    PageNum last_leaf_page = pfile->get_page_count();
    PageNum first_int_page = last_leaf_page + 1;
    PageNum cur_output_pnum = first_int_page;

    size_t leaf_pages_remaining = last_leaf_page - 1;

    size_t cur_int_page_idx = 0;
    size_t cur_int_rec_idx = 0;

    size_t internal_node_child_rec_cnt = 0;

    // The first internal node has no previous prev_sibling
    ((ISAMTreeInternalNodeHeader *) (out_buffer))->prev_sibling = INVALID_PNUM;
            ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_page_idx * PAGE_SIZE)))->leaf_rec_cnt = 0;

    do {
        size_t read_pages = std::min(in_buffer_sz, leaf_pages_remaining);
        if (pfile->read_pages(cur_input_pnum, read_pages, in_buffer) == 0) {
            goto error;
        }

        leaf_pages_remaining -= read_pages;

        for (size_t i=0; i<read_pages; i++) { 
            // Get the key of the last record in this leaf page
            size_t last_record_offset = (cur_input_pnum < last_leaf_page) ?  record_size*(leaf_recs_per_page-1) : record_size * (final_leaf_rec_cnt-1);
            const char *key = get_key(in_buffer + (PAGE_SIZE * (i)) + last_record_offset);

            char *internal_buff = out_buffer + (cur_int_page_idx * PAGE_SIZE) + (cur_int_rec_idx++ * internal_record_size) + ISAMTreeInternalNodeHeaderSize;
            ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_page_idx * PAGE_SIZE)))->leaf_rec_cnt += leaf_recs_per_page;

            build_internal_record(internal_buff, key, cur_input_pnum);
            cur_input_pnum++;

            if (cur_int_rec_idx >= int_recs_per_page) {
                cur_int_rec_idx = 0;
                cur_int_page_idx++;

                ((ISAMTreeInternalNodeHeader *) (out_buffer + ((cur_int_page_idx - 1) * PAGE_SIZE)))->next_sibling = cur_output_pnum + cur_int_page_idx;

                if (cur_int_page_idx >= out_buffer_sz) {
                    if (!pfile->allocate_pages(out_buffer_sz)) {
                        goto error;
                    }

                    if (!pfile->write_pages(cur_output_pnum, out_buffer_sz, out_buffer)) {
                        goto error;
                    }

                    cur_output_pnum += out_buffer_sz;
                    cur_int_page_idx = 0;
                }

                // Set up the page pointers in the header. Technically, we
                // don't *really* need these as the pages are laid out
                // sequentially anyway. But they are a convenient way still to
                // identify the first and last page on a given level.
                ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_rec_idx * PAGE_SIZE)))->prev_sibling = cur_output_pnum + cur_int_page_idx - 1;
                ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_rec_idx * PAGE_SIZE)))->next_sibling = INVALID_PNUM;
                ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_page_idx * PAGE_SIZE)))->leaf_rec_cnt = 0;
            }
        }
    } while (cur_input_pnum <= last_leaf_page);

    // If this is the case, it means we wrote the buffer on the last iteration
    // of the loop and didn't add any records to the current buffer, so we need
    // to roll-back the updates to the output page and index.
    if (cur_int_page_idx == 0 && cur_int_rec_idx == 0) {
        cur_int_page_idx = out_buffer_sz;
        cur_output_pnum -= out_buffer_sz;
    } 
    
    // Rather than account for it within the loop, it's probably better just to 
    // account for the last leaf's record count after the loop, avoiding a branch
    ((ISAMTreeInternalNodeHeader *) (out_buffer + (cur_int_page_idx * PAGE_SIZE)))->leaf_rec_cnt -= (leaf_recs_per_page - final_leaf_rec_cnt);

    // Write any remaining data from the buffer.
    if (!pfile->allocate_pages(cur_int_page_idx+1)) {
        goto error;
    }

    if (!pfile->write_pages(cur_output_pnum, cur_int_page_idx+1, out_buffer)) {
        goto error;
    }

    // return the number of pages on Level1
    return (cur_output_pnum + cur_int_page_idx) - first_int_page;

error:
    return INVALID_PNUM;
}


PageNum ISAMTree::generate_internal_levels(PagedFile *pfile, size_t final_leaf_rec_cnt, char *buffer, size_t buffer_sz)
{
    // FIXME: There're some funky edge cases here if the input_buffer_sz is larger
    // than the number of leaf pages
    size_t input_buffer_sz = 1;
    auto input_buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * input_buffer_sz);

    PageNum prev_level_first_pnum = pfile->get_page_count() + 1;
    PageNum prev_level_pg_cnt = ISAMTree::generate_first_internal_level(pfile, final_leaf_rec_cnt, buffer, buffer_sz, input_buffer, input_buffer_sz);
    PageNum cur_out_pnum = pfile->get_page_count() + 1;

    if (prev_level_pg_cnt == INVALID_PNUM) {
        goto error_buffer;
    }


    free(input_buffer);
    // The last pnum processed will belong to the page in the level with only 1 node,
    // i.e., the root node.
    return cur_out_pnum;

error_buffer:
    free(input_buffer);

error:
    return INVALID_PNUM;
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
}
