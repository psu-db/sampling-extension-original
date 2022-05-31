/*
 *
 */

#include "ds/staticbtree.hpp"

namespace lsm { namespace ds {

std::unique_ptr<StaticBTree> StaticBTree::initialize(io::PagedFile *pfile,
                                                     std::unique_ptr<iter::MergeIterator> record_iter, 
                                                     PageNum data_page_cnt,
                                                     catalog::FixedKVSchema *record_schema,
                                                     iter::CompareFunc key_cmp)
{
    auto internal_schema = StaticBTree::generate_internal_schema(record_schema);

    auto leaf_output_buffer = mem::page_alloc_unique();
    auto internal_output_buffer = mem::page_alloc_unique();
    auto input_buffer = mem::page_alloc_unique();

    io::FixedlenDataPage::initialize(leaf_output_buffer.get(), record_schema->record_length());
    io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), StaticBTreeInternalNodeHeaderSize);

    // Allocate initial pages for data and for metadata
    PageId first_leaf, first_internal, meta;
    if (!StaticBTree::initial_page_allocation(pfile, data_page_cnt, &first_leaf, &first_internal, &meta)) {
        return nullptr;
    }

    io::FixedlenDataPage leaf_page = io::FixedlenDataPage(leaf_output_buffer.get());
    io::FixedlenDataPage internal_page = io::FixedlenDataPage(internal_output_buffer.get());

    ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->first_child = first_leaf.page_number;
    ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = INVALID_PNUM;


    // we want to keep track of the first key for each node (the one associated with first_child),
    // as these keys are the ones that get pushed up to the parent levels. For the moment, this is
    // hardcoded as int64_t. If I'm to keep it fully generic, I'll need to do an extra memcpy on each
    // key and manage that memory--we'll leave that aside for the moment.
    std::vector<std::pair<PageNum, int64_t>> leading_keys;

    PageNum leaf_page_cnt = 0;
    PageNum internal_page_cnt = 0;
    bool first_iteration = true;
    while (record_iter->next() && leaf_page_cnt < data_page_cnt) {
        auto rec = record_iter->get_item();

        if (first_iteration) {
            first_iteration = false;
            leading_keys.push_back({first_internal.page_number, record_schema->get_key(rec.get_data()).Int64()});
        }

        if (!leaf_page.insert_record(rec)) {
            // write the full page to the file
            PageNum new_page = first_leaf.page_number + leaf_page_cnt++;
            pfile->write_page(new_page, leaf_output_buffer.get());

            // create a new page and load the record into it.
            io::FixedlenDataPage::initialize(leaf_output_buffer.get(), record_schema->record_length());
            leaf_page = io::FixedlenDataPage(leaf_output_buffer.get()); // probably not necessary
            leaf_page.insert_record(rec);

            // insert the separator key into the parent leaf node.
            auto internal_recbuf = internal_schema->create_record_unique(record_schema->get_key(rec.get_data()).Bytes(), (byte *) &new_page);
            if (!internal_page.insert_record(io::Record(internal_recbuf.get(), internal_schema->record_length()))) {
                PageNum new_internal_page = pfile->allocate_page().page_number;
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = new_internal_page;
                pfile->write_page(new_internal_page, internal_output_buffer.get());

                leading_keys.push_back({new_internal_page, record_schema->get_key(rec.get_data()).Int64()});

                internal_page_cnt++;

                io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length());
                internal_page = io::FixedlenDataPage(internal_output_buffer.get()); // probably not necessary
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->first_child = new_page;
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = new_internal_page - 1;
            }
        }
    }

    auto root_pnum = StaticBTree::generate_next_internal_level(pfile, first_internal.page_number, first_internal.page_number + internal_page_cnt, internal_schema.get());

    return std::make_unique<StaticBTree>(pfile, record_schema, key_cmp);
}


int initial_page_allocation(io::PagedFile *pfile, PageNum page_cnt, PageId *first_leaf, PageId *first_internal, PageId *meta)
{
    *meta = pfile->allocate_page();
    if (pfile->supports_allocation() == io::PageAllocSupport::BULK) {
        *first_leaf = pfile->allocate_page_bulk(page_cnt);
    } else {
        *first_leaf = pfile->allocate_page();
        for (size_t i=1; i<page_cnt; i++) {
            if (pfile->allocate_page() == INVALID_PID) {
                return 0;
            }
        }
    }

    *first_internal = pfile->allocate_page();

    if (*meta == INVALID_PID || *first_leaf == INVALID_PID || *first_internal == INVALID_PID) {
        return 0;
    }

    return 1;
}


std::unique_ptr<catalog::FixedKVSchema> generate_internal_schema(catalog::FixedKVSchema *record_schema)
{
    PageOffset key_length = record_schema->key_len();
    PageOffset value_length = sizeof(PageNum);

    return std::make_unique<catalog::FixedKVSchema>(key_length, value_length, 0);
}


PageNum generate_next_internal_level(io::PagedFile *pfile, catalog::FixedKVSchema *schema, std::vector<std::pair<PageNum, int64_t>> leading_keys)
{
   if (leading_keys.size() == 0) {
        return 0;
    } 

    auto internal_page_buf = mem::page_alloc_unique();
    io::FixedlenDataPage::initialize(internal_page_buf.get(), schema->record_length(), StaticBTreeInternalNodeHeaderSize);
    PageNum new_level_first_page = pfile->allocate_page().page_number;
    PageNum new_level_page_cnt = 0;

    return INVALID_PNUM;
}


StaticBTree::StaticBTree(io::PagedFile *pfile, catalog::FixedKVSchema *record_schema,
                iter::CompareFunc key_cmp, io::ReadCache *cache)
{
    this->cache = cache;
    this->key_cmp = key_cmp;
    this->record_schema = record_schema;
    this->internal_index_schema = StaticBTree::generate_internal_schema(record_schema);

    this->pfile = pfile;

    byte *frame_ptr;
    auto frame_id = this->cache->pin(BTREE_META_PNUM, this->pfile, &frame_ptr);
    this->root_page = ((StaticBTreeMetaHeader *) frame_ptr)->root_node;
    this->first_data_page = ((StaticBTreeMetaHeader *) frame_ptr)->first_data_page;
    this->last_data_page = ((StaticBTreeMetaHeader *) frame_ptr)->last_data_page;
    this->cache->unpin(frame_id);
}


PageId StaticBTree::get_lower_bound(const byte *key)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page >= this->last_data_page) {
        current_page = search_internal_node_lower(current_page, key);
    }

    return this->pfile->pnum_to_pid(current_page);
}


PageId StaticBTree::get_upper_bound(const byte *key)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_upper(current_page, key);
    }

    return this->pfile->pnum_to_pid(current_page);
}


bool StaticBTree::tombstone_exists(const byte *key, Timestamp time) 
{
    PageId first_page = this->get_lower_bound(key);
    PageNum current_pnum = first_page.page_number;
    byte *page_buf;
    auto frid = this->cache->pin(current_pnum, this->pfile, &page_buf);

    auto current_sid = this->search_leaf_page(page_buf, key);

    // Because the lower_bound check is conservative and may include the
    // first_child page even if the desired value isn't there, we may need to
    // check the page beside if too.
    if (current_sid == INVALID_SID) {
        this->cache->unpin(frid);
        current_pnum++;
        frid = this->cache->pin(current_pnum, this->pfile, &page_buf);
        current_sid = this->search_leaf_page(page_buf, key);
    }

    if (current_sid == INVALID_SID) {
        this->cache->unpin(frid);
        return false; // record doesn't exist to begin with
    }

    auto leaf_page = io::FixedlenDataPage(page_buf);
    auto leaf_record = leaf_page.get_record(current_sid);
    Timestamp newest_tombstone = 0;
    Timestamp newest_record = 0;
    bool tombstone_found = false;
    while (this->key_cmp(key, this->record_schema->get_key(leaf_record.get_data()).Bytes()) == 0) {
        // The records may not be sorted in timestamp order. It would be a lot easier here if
        // that ordering were enforced. This could be done by modifying the comparator passed into
        // MergeIterator to account for this--which I will probably do later. But for now, we'll 
        // do it this way.
        if (leaf_record.get_timestamp() <= time) {
           if (leaf_record.get_timestamp() > newest_record) {
                newest_record = leaf_record.get_timestamp();
            } 

            if (leaf_record.is_tombstone()) {
                tombstone_found = true;
                if (leaf_record.get_timestamp() > newest_tombstone) {
                    newest_tombstone = leaf_record.get_timestamp();
                }
            }
        } 

        current_sid++;
        leaf_record = leaf_page.get_record(current_sid);

        // if we've run to the end of the page, we should check the next one,
        // as the key range may span multiple pages. The break will hit when we
        // finish checking the last leaf page, and the while conditional will
        // stop the loop when the keys cease to match.
        if (!leaf_record.is_valid()) {
            this->cache->unpin(frid);
            if (++current_pnum > this->last_data_page) {
                break;
            }
            
            frid = this->cache->pin(current_pnum, this->pfile, &page_buf);
            leaf_page = io::FixedlenDataPage(page_buf);
            current_sid = leaf_page.get_min_sid();
            leaf_record = leaf_page.get_record(current_sid);
        }
    }

    this->cache->unpin(frid);
    return tombstone_found && (newest_tombstone == newest_record);
}


PageNum StaticBTree::search_internal_node_lower(PageNum pnum, const byte *key)
{
    byte *page_buf;
    auto frid = this->cache->pin(pnum, this->pfile, &page_buf);
    auto page = io::FixedlenDataPage(page_buf);
    auto first_child = ((StaticBTreeInternalNodeHeader *) page.get_user_data())->first_child;

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();

    // the page has no records, or if the input key is smaller than the first
    // key record, then we return the first_child reference.
    if (page.get_record_count() == 0) {
        this->cache->unpin(frid);
        return first_child;
    }

    // NOTE: We can't know, based on the way that the first pointer on a node
    // is set up, whether our lower bound should be in the first_child or in
    // the first internal record (second child), as we don't track the key
    // value for the first child. Consider finding the lower bound of 30 in the
    // following case,
    // |34|50|...
    // If anything between 30 and 34 exists within the first child, that page
    // should be our lower bound, but if not, then the page associated with the
    // key 34 is. Here I am being conservative and assuming that the first
    // child contains elements within the range. This will result, potentially,
    // in increasing the rejection rate.
    auto first_key = this->internal_index_schema->get_key(page.get_record(min).get_data()).Bytes();
    if (this->key_cmp(first_key, key) > 0) {
        this->cache->unpin(frid);
        return first_child;
    }

    // otherwise, we do a full binary search
    while (min < max) {
        SlotId mid = (min + max) / 2;
        auto node_key = this->internal_index_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(node_key, key) >= 0) {
            max = mid;
        } else {
            min = mid + 1;
        }
    }

    auto index_record = page.get_record(min);
    auto target_pnum = this->internal_index_schema->get_val(index_record.get_data()).Int32();

    this->cache->unpin(frid);
    return target_pnum;
}


PageNum StaticBTree::search_internal_node_upper(PageNum pnum, const byte *key)
{
    byte *page_buf;
    auto frid = this->cache->pin(pnum, this->pfile, &page_buf);
    auto page = io::FixedlenDataPage(page_buf);
    auto first_child = ((StaticBTreeInternalNodeHeader *) page.get_user_data())->first_child;

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();

    // the page has no records, or if the input key is smaller than the first
    // key record, then we return the first_child reference.
    if (page.get_record_count() == 0) {
        this->cache->unpin(frid);
        return first_child;
    }

    auto first_key = this->internal_index_schema->get_key(page.get_record(min).get_data()).Bytes();
    if (this->key_cmp(first_key, key) > 0) {
        this->cache->unpin(frid);
        return first_child;
    }

    // otherwise, we do a full binary search
    while (min < max) {
        SlotId mid = (min + max) / 2;
        auto node_key = this->internal_index_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(node_key, key) > 0) {
            max = mid;
        } else {
            min = mid + 1;
        }
    }

    auto index_record = page.get_record(max - 1);
    auto target_pnum = this->internal_index_schema->get_val(index_record.get_data()).Int32();

    this->cache->unpin(frid);
    return target_pnum;
}


SlotId StaticBTree::search_leaf_page(byte *page_buf, const byte *key)
{
    auto page = io::FixedlenDataPage(page_buf);

    if (page.get_record_count() == 0) {
        return INVALID_SID;
    }

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();

    while (min < max) {
        SlotId mid = (max + min) / 2;
        auto node_key = this->internal_index_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(node_key, key) > 0) {
            max = mid;
        } else {
            min = mid + 1;
        }
    }

    return max - 1;
}

}}
