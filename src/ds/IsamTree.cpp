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

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), iter2, tree2->rec_cnt, mtable->get_tombstone_count() + tree2->tombstone_cnt, rng);
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

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), sortedrun2, tree->get_record_count(), mtable->get_tombstone_count() + tree->tombstone_count(), rng);

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

    auto filter = ISAMTree::initialize(pfile, sortedrun1, mtable->get_record_count(), mtable->get_tombstone_count(), rng);

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


        if (iter1->next()) {
            iter1_page = iter1->get_item();
        }

        if (iter2->next()) {
            iter2_page = iter2->get_item();
        }

        // if neither iterator contains any items, then there is no merging work to
        // be done.
        while (!(iter1_rec_idx >= ISAM_RECORDS_PER_LEAF || iter1_records >= iter1_rec_cnt) || !(iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) {
            char *rec1 = (!(iter1_rec_idx >= ISAM_RECORDS_PER_LEAF || iter1_records >= iter1_rec_cnt)) ? iter1_page + (record_size * iter1_rec_idx) : nullptr;
            char *rec2 = (!(iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) ? iter2_page + (record_size * iter2_rec_idx) : nullptr;

            char *to_copy;
            if ((iter1_rec_idx >= ISAM_RECORDS_PER_LEAF || iter1_records >= iter1_rec_cnt) || (!(iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt) && record_cmp(rec1, rec2) == 1)) {
                to_copy = rec2;
                iter2_rec_idx++;
                iter2_rec_cnt++;

                if ((iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) {
                    iter2_page = (iter2->next()) ? iter2->get_item() : nullptr;
                    iter2_rec_idx = 0;
                }
            } else {
                to_copy = rec1;
                iter1_rec_idx++;
                iter1_rec_cnt++;

                if ((iter1_rec_idx >= ISAM_RECORDS_PER_LEAF || iter1_records >= iter1_rec_cnt)) {
                    iter1_page = (iter1->next()) ? iter1->get_item() : nullptr;
                    iter1_rec_idx = 0;
                } 
            }

            memcpy((void *) get_record(buffer, output_idx++), rec1, record_size);

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

    if (root_pnum == INVALID_PNUM) {
        goto error_filter;
    }

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


            memcpy((void *) get_record(buffer, output_idx++), rec1, record_size);

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

    if (root_pnum == INVALID_PNUM) {
        goto error_filter;
    }

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

        size_t output_idx = 0;

        if (iter2->next()) {
            iter2_page = iter2->get_item();
        }

        while (run1_rec_idx < run1_rec_cnt || !(iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) {
            char *rec1 = (run1_rec_idx < run1_rec_cnt) ? sorted_run1 + (record_size * run1_rec_idx) : nullptr;
            char *rec2 = (!(iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) ? iter2_page + (record_size * iter2_rec_idx) : nullptr;
            
            char *to_copy;
            if (!rec1 || (rec2 && record_cmp(rec1, rec2) == 1)) {
                to_copy = rec2;
                iter2_rec_idx++;
                iter2_records++;

                if ((iter2_rec_idx >= ISAM_RECORDS_PER_LEAF || iter2_records >= iter2_rec_cnt)) {
                    iter2_page = (iter2->next()) ? iter2->get_item() : nullptr;
                    iter2_rec_idx = 0;
                }
            } else {
                to_copy = rec1;
                run1_rec_idx++;
            }

            memcpy((void *) get_record(buffer, output_idx++), rec1, record_size);

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

    if (root_pnum == INVALID_PNUM) {
        goto error_filter;
    }

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
            
            memcpy((void *) get_record(buffer, output_idx++), rec1, record_size);

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

    if (root_pnum == INVALID_PNUM) {
        goto error_filter;
    }

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


PageNum ISAMTree::generate_next_internal_level(PagedFile *pfile, size_t *pl_final_pg_rec_cnt, PageNum *pl_first_pg, bool first_level, char *out_buffer, size_t out_buffer_sz, char *in_buffer, size_t in_buffer_sz)
{    
    // These variables names were getting very unwieldy. Here's a little glossary
    //      nl - new level (the level being created by this function)
    //      pl - previous level (the level from which the new level is being created)
    //      in - input -- refers to the buffers, etc. for the previous level that are read
    //      out - output -- refers to buffers, etc. for the new level that are written

    size_t int_recs_per_pg = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

    size_t pl_recs_per_pg = (first_level) ? PAGE_SIZE / record_size : int_recs_per_pg;

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
            size_t last_record = (in_pnum < pl_last_pg) ? pl_recs_per_pg - 1 : (*pl_final_pg_rec_cnt) - 1;

            const char *key;
            if (first_level) {
                key = get_key(get_record(get_page(in_buffer, in_pg_idx), last_record));
            } else {
                key = get_internal_key(get_internal_record(get_page(in_buffer, in_pg_idx), last_record));
            }

            // Increment the total number of children of this internal page
            get_header(out_buffer, out_pg_idx)->leaf_rec_cnt += (first_level) ? pl_recs_per_pg : get_header(in_buffer, in_pg_idx)->leaf_rec_cnt;

            // Get the address in the buffer for the new internal record
            char *internal_buff = get_internal_record(get_page(out_buffer, out_pg_idx), out_rec_idx++);

            // Create the new internal record at the address from above
            build_internal_record(internal_buff, key, in_pnum);

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
    *pl_final_pg_rec_cnt = (out_rec_idx) ? out_rec_idx - 1 : 0;

    // return the number of pages on the newly created leve.
    return (out_pnum + out_pg_idx) - nl_first_pg + 1;

error:
    return INVALID_PNUM;
}


PageNum ISAMTree::generate_internal_levels(PagedFile *pfile, size_t final_leaf_rec_cnt, char *out_buffer, size_t out_buffer_sz)
{
    // FIXME: There're some funky edge cases here if the input_buffer_sz is larger
    // than the number of leaf pages
    size_t in_buffer_sz = 1;
    auto in_buffer = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE * in_buffer_sz);

    // First, generate the first internal level
    PageNum pl_first_pg = BTREE_FIRST_LEAF_PNUM;
    size_t pl_final_rec_cnt = final_leaf_rec_cnt;
    PageNum pl_pg_cnt = ISAMTree::generate_next_internal_level(pfile, &pl_final_rec_cnt, &pl_first_pg, true, out_buffer, out_buffer_sz, in_buffer, in_buffer_sz);

    if (pl_pg_cnt == INVALID_PNUM) {
        goto error_buffer;
    }

    // If there was only 1 page in the first internal level, then that will be our root.
    if (pl_pg_cnt == 1) {
        return pl_first_pg;
    }

    // Otherwise, we need to repeatedly create new levels until the page count returned
    // is 1.
    while ((pl_pg_cnt = ISAMTree::generate_next_internal_level(pfile, &pl_final_rec_cnt, &pl_first_pg, false, out_buffer, out_buffer_sz, in_buffer, in_buffer_sz)) != 1) {
        if (pl_pg_cnt == INVALID_PNUM) {
            goto error_buffer;
        }
    }

    // The last page allocated is the tree root.
    free(in_buffer);
    return pl_first_pg + pl_pg_cnt;

error_buffer:
    free(in_buffer);

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
