/*
 *
 */

#include "ds/staticbtree.hpp"

namespace lsm { namespace ds {

void StaticBTree::initialize(io::IndexPagedFile *pfile, std::unique_ptr<iter::MergeIterator> record_iter,
                             PageNum data_page_cnt, catalog::FixedKVSchema *record_schema)
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
        return;
    }

    io::FixedlenDataPage leaf_page = io::FixedlenDataPage(leaf_output_buffer.get());
    io::FixedlenDataPage internal_page = io::FixedlenDataPage(internal_output_buffer.get());

    ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = INVALID_PNUM;
    ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;

    PageNum leaf_page_cnt = 0;
    PageNum internal_page_cnt = 0;
    PageNum new_page;
    PageNum cur_internal_page = first_internal.page_number;

    size_t i=0;
    while (record_iter->next() && leaf_page_cnt < data_page_cnt) {
        i++;
        auto rec = record_iter->get_item();

        if (!leaf_page.insert_record(rec)) {
            // write the full page to the file
            new_page = first_leaf.page_number + leaf_page_cnt++;
            pfile->write_page(new_page, leaf_output_buffer.get());

            // the last key of the just-written page should be inserted into the internal node
            // above.
            auto new_key_record = leaf_page.get_record(leaf_page.get_max_sid());
            auto internal_recbuf = internal_schema->create_record_unique(record_schema->get_key(new_key_record.get_data()).Bytes(), (byte *) &new_page);
            auto key_rec = io::Record(internal_recbuf.get(), internal_schema->record_length()); 

            if (!internal_page.insert_record(key_rec)) {
                auto new_internal_page = pfile->allocate_page().page_number;
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = new_internal_page;
                pfile->write_page(cur_internal_page, internal_output_buffer.get());
                cur_internal_page = new_internal_page;

                internal_page_cnt++;

                io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), StaticBTreeInternalNodeHeaderSize);
                internal_page = io::FixedlenDataPage(internal_output_buffer.get()); // probably not necessary
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = new_internal_page - 1;
                ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;

                internal_page.insert_record(key_rec);
            }

            // create a new leaf page and load the record into it
            io::FixedlenDataPage::initialize(leaf_output_buffer.get(), record_schema->record_length());
            leaf_page = io::FixedlenDataPage(leaf_output_buffer.get()); // probably not necessary
            leaf_page.insert_record(rec);
        }
    }

    // We need to ensure that the last leaf page has an internal record pointing to it. As we're tracking
    // the end of the range of values for a page, we will always need to do this. The above loop inserts
    // new internal records when it writes a full leaf page, but it always writes a new record to a new
    // leaf page immediately afterward--hence there will always be a leaf page with no internal record
    // at the end of that loop.
    new_page = first_leaf.page_number + leaf_page_cnt;
    auto new_key_record = leaf_page.get_record(leaf_page.get_max_sid());
    auto internal_recbuf = internal_schema->create_record_unique(record_schema->get_key(new_key_record.get_data()).Bytes(), (byte *) &new_page);
    auto key_rec = io::Record(internal_recbuf.get(), internal_schema->record_length()); 

    if (!internal_page.insert_record(key_rec)) {
        auto new_internal_page = pfile->allocate_page().page_number;
        ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = new_internal_page;
        pfile->write_page(cur_internal_page, internal_output_buffer.get());
        cur_internal_page = new_internal_page;

        internal_page_cnt++;

        io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), StaticBTreeInternalNodeHeaderSize);
        internal_page = io::FixedlenDataPage(internal_output_buffer.get()); // probably not necessary
        ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = new_internal_page - 1;
        ((StaticBTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;

        internal_page.insert_record(key_rec);
    }

    pfile->write_page(new_page, leaf_output_buffer.get());
    pfile->write_page(cur_internal_page, internal_output_buffer.get());

    auto root_pnum = StaticBTree::generate_internal_levels(pfile, first_internal.page_number, internal_schema.get());

    pfile->read_page(meta, internal_output_buffer.get());
    auto metadata = (StaticBTreeMetaHeader *) internal_output_buffer.get();
    metadata->root_node = root_pnum;
    metadata->first_data_page = first_leaf.page_number;
    metadata->last_data_page = new_page;
    pfile->write_page(meta, internal_output_buffer.get());
}


int StaticBTree::initial_page_allocation(io::PagedFile *pfile, PageNum page_cnt, PageId *first_leaf, PageId *first_internal, PageId *meta)
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


std::unique_ptr<catalog::FixedKVSchema> StaticBTree::generate_internal_schema(catalog::FixedKVSchema *record_schema)
{
    PageOffset key_length = record_schema->key_len();
    PageOffset value_length = sizeof(PageNum);

    return std::make_unique<catalog::FixedKVSchema>(key_length, value_length, 0);
}


PageNum StaticBTree::generate_internal_levels(io::PagedFile *pfile, PageNum first_internal, catalog::FixedKVSchema *schema)
{
    // NOTE: We could just take advantage of the ReadCache, rather than
    // defining a new input buffer here
    auto input_buf = mem::page_alloc_unique();
    auto output_buf = mem::page_alloc_unique();

    PageNum current_pnum = first_internal;
    pfile->read_page(current_pnum, input_buf.get());
    auto input_page = io::FixedlenDataPage(input_buf.get());
    auto input_header = (StaticBTreeInternalNodeHeader *) input_page.get_user_data();
    size_t i = 0;
    while (input_header->next_sibling != INVALID_PNUM) {
        i++;
        auto new_output_pid = pfile->allocate_page();
        auto current_output_pid = new_output_pid;

        io::FixedlenDataPage::initialize(output_buf.get(), schema->record_length(), StaticBTreeInternalNodeHeaderSize);
        auto output_page = io::FixedlenDataPage(output_buf.get());
        auto output_header = (StaticBTreeInternalNodeHeader *) output_page.get_user_data();
        output_header->prev_sibling = INVALID_PNUM;

        while (true) {
            // move the last key value on this node up to the next level of the tree
            auto key_record = input_page.get_record(input_page.get_max_sid());
            auto key = schema->get_key(key_record.get_data()).Bytes();
            auto value = current_pnum;

            auto new_record_buf = schema->create_record_unique(key, (byte *) &value);
            auto new_record = io::Record(new_record_buf.get(), schema->record_length());

            // insert record into output page
            if (!output_page.insert_record(new_record)) {
                auto new_pid = pfile->allocate_page();
                output_header->next_sibling = new_pid.page_number;
                pfile->write_page(current_output_pid, output_buf.get());

                io::FixedlenDataPage::initialize(output_buf.get(), schema->record_length(), StaticBTreeInternalNodeHeaderSize);
                output_page = io::FixedlenDataPage(output_buf.get());
                output_header = (StaticBTreeInternalNodeHeader *) output_page.get_user_data();
                output_header->prev_sibling = current_output_pid.page_number;

                output_page.insert_record(key_record);
                current_output_pid = new_pid;
            }

            // advance to next page at this level
            if (input_header->next_sibling == INVALID_PNUM) {
                break;
            }

            current_pnum = input_header->next_sibling;
            pfile->read_page(current_pnum, input_buf.get());
            input_page = io::FixedlenDataPage(input_buf.get());
            input_header = (StaticBTreeInternalNodeHeader *) input_page.get_user_data();
        } 

        // Make sure that the output buffer is flushed before the next iteration
        pfile->write_page(current_output_pid, output_buf.get());

        // Set up to begin processing the newly created level in the next iteration
        current_pnum = new_output_pid.page_number;
        pfile->read_page(current_pnum, input_buf.get());
        input_page = io::FixedlenDataPage(input_buf.get());
        input_header = (StaticBTreeInternalNodeHeader *) input_page.get_user_data();
    }

    // The last pnum processed will belong to the page in the level with only 1 node,
    // i.e., the root node.
    return current_pnum;
}


StaticBTree::StaticBTree(io::IndexPagedFile *pfile, catalog::FixedKVSchema *record_schema,
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

    // It is possible, because the internal records contain max values for each
    // run, that an adjacent page to the one reported above may contain valid
    // keys within the range. This can only occur in this case of the internal
    // key being equal to the boundary key, and the next page containing
    // duplicate values of that same key.
    if (current_page < this->last_data_page && current_page != INVALID_PNUM) {
        byte *buf;
        auto frid = this->cache->pin(current_page + 1, this->pfile, &buf);
        if (this->search_leaf_page(buf, key) != INVALID_SID) {
            current_page = current_page + 1;
        }

        this->cache->unpin(frid);
    }
        

    return this->pfile->pnum_to_pid(current_page);
}


bool StaticBTree::tombstone_exists(const byte *key, Timestamp time) 
{
    // Because we can have duplicate keys with differing timestamps, we need to
    // establish a range of pages over which the tombstone could be located.
    PageId first_page = this->get_lower_bound(key);
    PageId last_page = this->get_upper_bound(key);

    // if the first and last page are not both defined, then the record doesn't exist within
    // the tree, and so no tombstone is present.
    if (first_page.page_number == INVALID_PNUM || last_page.page_number == INVALID_PNUM) {
        return false;
    }

    // For now, I'm not going to assume that the provided comparison function necessarily sorts
    // by timestamp, so we'll do a scan of the valid record targets.
    Timestamp newest_tombstone = 0;
    Timestamp newest_record = 0;
    bool tombstone_found = false;

    for (PageNum i=first_page.page_number; i<=last_page.page_number; i++) {
        byte *page_buf;
        auto frid = this->cache->pin(i, this->pfile, &page_buf);
        auto sid = this->search_leaf_page(page_buf, key);
        auto leaf_page = io::FixedlenDataPage(page_buf);
        while (sid != INVALID_SID && sid <= leaf_page.get_max_sid() 
          && this->key_cmp(key, this->record_schema->get_key(leaf_page.get_record(sid).get_data()).Bytes()) == 0) {
            auto leaf_record = leaf_page.get_record(sid++);
            auto record_time = leaf_record.get_timestamp();

            if (record_time <= time) {
                newest_record = std::max(record_time, newest_record);
                if (leaf_record.is_tombstone()) {
                    tombstone_found = true;
                    newest_tombstone = std::max(record_time, newest_tombstone);
                }
            }
        }

        this->cache->unpin(frid);
    }

    return tombstone_found && (newest_tombstone == newest_record);
}


PageNum StaticBTree::search_internal_node_lower(PageNum pnum, const byte *key)
{
    byte *page_buf;
    auto frid = this->cache->pin(pnum, this->pfile, &page_buf);
    auto page = io::FixedlenDataPage(page_buf);

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();

    // If the entire range of numbers falls below the target key, the algorithm
    // will return max as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = this->internal_index_schema->get_key(page.get_record(max).get_data()).Bytes();
    if (this->key_cmp(key, node_key) > 0) {
        this->cache->unpin(frid);
        return INVALID_PNUM;
    }

    while (min < max) {
        SlotId mid = (min + max) / 2;
        auto node_key = this->internal_index_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(key, node_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
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

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();
    
    // If the entire range of numbers falls above the target key, the algorithm
    // will return min as its bound, even though there actually isn't a valid
    // bound. So we need to check this case manually and return INVALID_PNUM.
    auto node_key = this->internal_index_schema->get_key(page.get_record(min).get_data()).Bytes();
    if (this->key_cmp(key, node_key) < 0) {
        this->cache->unpin(frid);
        return INVALID_PNUM;
    }

    while (min < max) {
        SlotId mid = (min + max) / 2;
        auto node_key = this->internal_index_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(key, node_key) < 0) {
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


SlotId StaticBTree::search_leaf_page(byte *page_buf, const byte *key)
{
    auto page = io::FixedlenDataPage(page_buf);

    if (page.get_record_count() == 0) {
        return INVALID_SID;
    }

    SlotId min = page.get_min_sid();
    SlotId max = page.get_max_sid();
    const byte * record_key;

    while (min < max) {
        SlotId mid = (min + max) / 2;
        record_key = this->record_schema->get_key(page.get_record(mid).get_data()).Bytes();
        if (this->key_cmp(key, record_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    // Check if the thing that we found matches the target. If so, we've
    // found it. If not, the target doesn't exist on the page.
    if (this->key_cmp(key, record_key) == 0) {
        return min;
    }

    return INVALID_SID;
}


std::unique_ptr<iter::GenericIterator<Record>> StaticBTree::start_scan()
{
    return std::make_unique<io::IndexPagedFileRecordIterator>(this->pfile, this->cache, this->first_data_page, this->last_data_page);
}

}}
