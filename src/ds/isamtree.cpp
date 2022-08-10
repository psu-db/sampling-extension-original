/*
 *
 */

#include "ds/isamtree.hpp"

namespace lsm { namespace ds {

std::unique_ptr<ISAMTree> ISAMTree::create(std::unique_ptr<iter::MergeIterator> record_iter, PageNum leaf_page_cnt, bool bloom_filters, global::g_state *state, size_t tombstone_count)
{
    auto pfile = state->file_manager->create_indexed_pfile();
    ISAMTree::initialize(pfile, std::move(record_iter), leaf_page_cnt, state, bloom_filters, tombstone_count);

    return std::make_unique<ISAMTree>(pfile, state);
}

void ISAMTree::initialize(io::IndexPagedFile *pfile, std::unique_ptr<iter::MergeIterator> record_iter,
                             PageNum data_page_cnt, global::g_state *state, bool bloom_filters, size_t tombstone_count)
{
    auto record_schema = state->record_schema.get();
    auto internal_schema = ISAMTree::generate_internal_schema(record_schema);

    auto leaf_output_buffer = mem::page_alloc();
    auto internal_output_buffer = mem::page_alloc();
    auto input_buffer = mem::page_alloc();

    io::FixedlenDataPage::initialize(leaf_output_buffer.get(), record_schema->record_length());
    io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), ISAMTreeInternalNodeHeaderSize);

    // Allocate initial pages for data and for metadata
    PageId first_leaf, first_internal, meta, filter_meta, tombstone_filter_meta;
    PageOffset key_size = record_schema->key_len();
    if (!ISAMTree::initial_page_allocation(pfile, data_page_cnt, tombstone_count, key_size, bloom_filters, &first_leaf, &first_internal, &meta, &filter_meta, &tombstone_filter_meta, state)) {
        return;
    }

    std::unique_ptr<PersistentBloomFilter> tomb_filter = nullptr;

    if (bloom_filters) {
        tomb_filter = PersistentBloomFilter::open(tombstone_filter_meta, state);
    }

    io::FixedlenDataPage leaf_page = io::FixedlenDataPage(leaf_output_buffer.get());
    io::FixedlenDataPage internal_page = io::FixedlenDataPage(internal_output_buffer.get());

    ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = INVALID_PNUM;
    ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;
    ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->leaf_rec_cnt = 0;

    PageNum leaf_page_cnt = 0;
    PageNum internal_page_cnt = 0;
    PageNum new_page;
    PageNum cur_internal_page = first_internal.page_number;

    while (record_iter->next() && leaf_page_cnt < data_page_cnt) {
        auto rec = record_iter->get_item();

        if (bloom_filters && rec.is_tombstone()) {
            tomb_filter->insert(record_schema->get_key(rec.get_data()).Bytes());
        }

        if (leaf_page.insert_record(rec)) {
            ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->leaf_rec_cnt++;
        } else {
            // write the full page to the file
            new_page = first_leaf.page_number + leaf_page_cnt++;
            pfile->write_page(new_page, leaf_output_buffer.get());

            // the last key of the just-written page should be inserted into the internal node
            // above.
            auto new_key_record = leaf_page.get_record(leaf_page.get_max_sid());
            auto internal_recbuf = internal_schema->create_record(record_schema->get_key(new_key_record.get_data()).Bytes(), (byte *) &new_page);
            auto key_rec = io::Record(internal_recbuf.get(), internal_schema->record_length()); 


            if (!internal_page.insert_record(key_rec)) {
                auto new_internal_page = pfile->allocate_page().page_number;
                ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = new_internal_page;
                pfile->write_page(cur_internal_page, internal_output_buffer.get());
                cur_internal_page = new_internal_page;

                internal_page_cnt++;

                io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), ISAMTreeInternalNodeHeaderSize);
                internal_page = io::FixedlenDataPage(internal_output_buffer.get()); // probably not necessary
                ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = new_internal_page - 1;
                ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;
                ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->leaf_rec_cnt = 0;

                internal_page.insert_record(key_rec);
            }

            // create a new leaf page and load the record into it
            io::FixedlenDataPage::initialize(leaf_output_buffer.get(), record_schema->record_length());
            leaf_page = io::FixedlenDataPage(leaf_output_buffer.get()); // probably not necessary
            leaf_page.insert_record(rec);
            ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->leaf_rec_cnt++;
        }
    }

    if (bloom_filters) {
        tomb_filter->flush();
    }

    // We need to ensure that the last leaf page has an internal record pointing to it. As we're tracking
    // the end of the range of values for a page, we will always need to do this. The above loop inserts
    // new internal records when it writes a full leaf page, but it always writes a new record to a new
    // leaf page immediately afterward--hence there will always be a leaf page with no internal record
    // at the end of that loop.
    new_page = first_leaf.page_number + leaf_page_cnt;
    auto new_key_record = leaf_page.get_record(leaf_page.get_max_sid());
    auto internal_recbuf = internal_schema->create_record(record_schema->get_key(new_key_record.get_data()).Bytes(), (byte *) &new_page);
    auto key_rec = io::Record(internal_recbuf.get(), internal_schema->record_length()); 

    if (!internal_page.insert_record(key_rec)) {
        auto new_internal_page = pfile->allocate_page().page_number;
        ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = new_internal_page;
        pfile->write_page(cur_internal_page, internal_output_buffer.get());
        cur_internal_page = new_internal_page;

        internal_page_cnt++;

        io::FixedlenDataPage::initialize(internal_output_buffer.get(), internal_schema->record_length(), ISAMTreeInternalNodeHeaderSize);
        internal_page = io::FixedlenDataPage(internal_output_buffer.get()); // probably not necessary
        ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->prev_sibling = new_internal_page - 1;
        ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->next_sibling = INVALID_PNUM;
        ((ISAMTreeInternalNodeHeader *) internal_page.get_user_data())->leaf_rec_cnt = 1;

        internal_page.insert_record(key_rec);
    }

    pfile->write_page(new_page, leaf_output_buffer.get());
    pfile->write_page(cur_internal_page, internal_output_buffer.get());

    auto root_pnum = ISAMTree::generate_internal_levels(pfile, first_internal.page_number, internal_schema.get());

    pfile->read_page(meta, internal_output_buffer.get());
    auto metadata = (ISAMTreeMetaHeader *) internal_output_buffer.get();
    metadata->root_node = root_pnum;
    metadata->first_data_page = first_leaf.page_number;
    metadata->last_data_page = new_page;
    metadata->first_tombstone_bloom_page = tombstone_filter_meta.page_number;
    pfile->write_page(meta, internal_output_buffer.get());
}


int ISAMTree::initial_page_allocation(io::PagedFile *pfile, PageNum page_cnt, size_t tombstone_count, size_t key_len, bool filters, PageId *first_leaf, PageId *first_internal, PageId *meta, PageId *filter_meta, PageId *tombstone_filter_meta, global::g_state *state)
{
    *meta = pfile->allocate_page();

    if (filters) {
        *filter_meta = INVALID_PID;
        *tombstone_filter_meta = pfile->allocate_page();
        PersistentBloomFilter::create(.5, tombstone_count, key_len, 7, tombstone_filter_meta->page_number, pfile, state);
    } else {
        *filter_meta = INVALID_PID;
        *tombstone_filter_meta = INVALID_PID;
    }

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


std::unique_ptr<catalog::FixedKVSchema> ISAMTree::generate_internal_schema(catalog::FixedKVSchema *record_schema)
{
    PageOffset key_length = record_schema->key_len();
    PageOffset value_length = sizeof(PageNum);

    return std::make_unique<catalog::FixedKVSchema>(key_length, value_length, 0);
}


PageNum ISAMTree::generate_internal_levels(io::PagedFile *pfile, PageNum first_internal, catalog::FixedKVSchema *schema)
{
    // NOTE: We could just take advantage of the ReadCache, rather than
    // defining a new input buffer here
    auto input_buf = mem::page_alloc();
    auto output_buf = mem::page_alloc();

    PageNum current_pnum = first_internal;
    pfile->read_page(current_pnum, input_buf.get());
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

    // The last pnum processed will belong to the page in the level with only 1 node,
    // i.e., the root node.
    return current_pnum;
}


ISAMTree::ISAMTree(io::IndexPagedFile *pfile, catalog::FixedKVSchema *record_schema,
                catalog::KeyCmpFunc key_cmp, io::ReadCache *cache)
{
    this->cache = cache;
    this->key_cmp = key_cmp;
    this->record_schema = record_schema;
    this->internal_index_schema = ISAMTree::generate_internal_schema(record_schema);
    this->fixed_length = true;

    this->pfile = pfile;
    this->state = nullptr;

    byte *frame_ptr;
    auto frame_id = this->cache->pin(BTREE_META_PNUM, this->pfile, &frame_ptr);
    auto meta = (ISAMTreeMetaHeader *) frame_ptr;
    this->root_page = meta->root_node;
    this->first_data_page = meta->first_data_page;
    this->last_data_page = meta->last_data_page;
    this->tombstone_cnt = meta->tombstone_count;

    if (meta->first_tombstone_bloom_page) {
        this->tombstone_bloom_filter = PersistentBloomFilter::open(meta->first_tombstone_bloom_page, pfile);
    }

    this->cache->unpin(frame_id);

    frame_id = this->cache->pin(this->root_page, this->pfile, &frame_ptr);
    this->rec_cnt = ((ISAMTreeInternalNodeHeader *) io::FixedlenDataPage(frame_ptr).get_user_data())->leaf_rec_cnt;
    this->cache->unpin(frame_id);
}


ISAMTree::ISAMTree(io::IndexPagedFile *pfile, global::g_state *state)
{
    this->cache = state->cache.get();
    this->key_cmp = state->record_schema->get_key_cmp();
    this->record_schema = state->record_schema.get();
    this->internal_index_schema = ISAMTree::generate_internal_schema(record_schema);
    this->fixed_length = true;

    this->pfile = pfile;

    this->state = state;

    byte *frame_ptr;
    auto frame_id = this->cache->pin(BTREE_META_PNUM, this->pfile, &frame_ptr);
    auto meta = (ISAMTreeMetaHeader *) frame_ptr;
    this->root_page = meta->root_node;
    this->first_data_page = meta->first_data_page;
    this->last_data_page = meta->last_data_page;
    this->tombstone_cnt = meta->tombstone_count;

    if (meta->first_tombstone_bloom_page) {
        this->tombstone_bloom_filter = PersistentBloomFilter::open(meta->first_tombstone_bloom_page, pfile);
    }

    this->cache->unpin(frame_id);

    frame_id = this->cache->pin(this->root_page, this->pfile, &frame_ptr);
    this->rec_cnt = ((ISAMTreeInternalNodeHeader *) io::FixedlenDataPage(frame_ptr).get_user_data())->leaf_rec_cnt;
    this->cache->unpin(frame_id);
}


ISAMTree::~ISAMTree()
{
    // FIXME: I'm leaking btrees without closing their underlying files
    // somewhere while growing the tree. This fixes the problem for now.
    if (this->state) {
        auto flid = this->pfile->get_flid();
        this->state->file_manager->close_file(flid);
    }
}


PageId ISAMTree::get_lower_bound(const byte *key)
{
    PageNum current_page = this->root_page;
    // The leaf pages are all allocated contiguously at the start of the file,
    // so we'll know when we have a pointer to one when the pnum is no larger
    // than the last data page.
    while (current_page > this->last_data_page) {
        current_page = search_internal_node_lower(current_page, key);
    }

    return this->pfile->pnum_to_pid(current_page);
}


PageId ISAMTree::get_upper_bound(const byte *key)
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


bool ISAMTree::tombstone_exists(const byte *key, Timestamp time) 
{
    if (this->tombstone_bloom_filter) {
        if (!this->tombstone_bloom_filter->lookup(key)) {
            return false;
        }
    }

    FrameId frid;
    auto rec = this->get(key, &frid, time);

    bool has_tombstone = rec.is_valid() && rec.is_tombstone();
    if (frid != INVALID_FRID) {
        this->cache->unpin(frid);
    }

    return has_tombstone;
}


PageNum ISAMTree::search_internal_node_lower(PageNum pnum, const byte *key)
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
    PageNum target_pnum = this->internal_index_schema->get_val(index_record.get_data()).Int32();

    this->cache->unpin(frid);

    return target_pnum;
}


PageNum ISAMTree::search_internal_node_upper(PageNum pnum, const byte *key)
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
        if (this->key_cmp(key, node_key) >= 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    auto index_record = page.get_record(min);
    PageNum target_pnum = this->internal_index_schema->get_val(index_record.get_data()).Int32();

    this->cache->unpin(frid);

    return target_pnum;
}


SlotId ISAMTree::search_leaf_page(byte *page_buf, const byte *key)
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

    auto record = page.get_record(min);
    record_key = this->record_schema->get_key(page.get_record(min).get_data()).Bytes();

    // Check if the thing that we found matches the target. If so, we've found
    // it. If not, the target doesn't exist on the page.
    if (this->key_cmp(key, record_key) == 0) {
        return min;
    }

    return INVALID_SID;
}


std::unique_ptr<iter::GenericIterator<Record>> ISAMTree::start_scan()
{
    return std::make_unique<io::IndexPagedFileRecordIterator>(this->pfile, this->cache, this->first_data_page, this->last_data_page, this->get_record_count());
}


Record ISAMTree::get(const byte *key, FrameId *frid, Timestamp time)
{
    // find the first page to search
    auto pid = this->get_lower_bound(key);

    byte *frame;
    FrameId int_frid = this->cache->pin(pid, this->pfile, &frame);
    auto sid = this->search_leaf_page(frame, key);

    if (sid == INVALID_SID) {
        this->cache->unpin(int_frid);
        *frid = INVALID_FRID;
        return io::Record();
    }

    auto page = io::wrap_page(frame);
    Record rec;

    do {
        rec = page->get_record(sid);

        if (rec.get_timestamp() <= time) {
            *frid = int_frid;
            return rec;
        } 

        if (++sid > page->get_max_sid()) {
            this->cache->unpin(int_frid);

            pid.page_number++;
            if (pid.page_number > this->last_data_page) {
                *frid = INVALID_FRID;
                return io::Record();    
            }

            int_frid = this->cache->pin(pid, this->pfile, &frame);
        }
    } while (this->key_cmp(key, this->record_schema->get_key(rec.get_data()).Bytes()) == 0);

    this->cache->unpin(int_frid);
    *frid = INVALID_FRID;
    return io::Record();
}


io::Record ISAMTree::get_tombstone(const byte *key, const byte *val, FrameId *frid, Timestamp time)
{
    if (this->tombstone_bloom_filter) {
        if (!this->tombstone_bloom_filter->lookup(key)) {
            *frid = INVALID_FRID;
            return io::Record();
        }
    }

    auto first_pid = this->get_lower_bound(key);
    byte *frame;

    FrameId int_frid = this->cache->pin(first_pid, this->pfile, &frame);
    auto first_sid = this->search_leaf_page(frame, key);

    // check if there are any matches for the specified key
    if (first_sid == INVALID_SID) {
        this->cache->unpin(int_frid);
        *frid = INVALID_FRID;
        return io::Record();
    }

    auto page = io::wrap_page(frame);
    Record rec;

    SlotId sid = first_sid;
    PageId pid = first_pid;

    auto val_cmp = this->state->record_schema->get_val_cmp();

    do {
        rec = page->get_record(sid);
        auto rec_val = this->state->record_schema->get_val(rec.get_data()).Bytes();

        if (rec.get_timestamp() <= time && val_cmp(val, rec_val) == 0) {
            if (rec.is_tombstone()) {
                *frid = int_frid;
                return rec;
            } else {
                *frid = INVALID_FRID;
                this->cache->unpin(int_frid);
                return io::Record();
            }
        } 

        if (++sid > page->get_max_sid()) {
            this->cache->unpin(int_frid);

            pid.page_number++;
            if (pid.page_number > this->last_data_page) {
                *frid = INVALID_FRID;
                return io::Record();    
            }

            int_frid = this->cache->pin(pid, this->pfile, &frame);
            page = io::wrap_page(frame);
            sid = page->get_min_sid();
        }
    } while (this->key_cmp(key, this->record_schema->get_key(rec.get_data()).Bytes()) == 0);

    this->cache->unpin(int_frid);
    *frid = INVALID_FRID;
    return io::Record();
}


size_t ISAMTree::get_record_count()
{
    return this->rec_cnt;
}


PageNum ISAMTree::get_leaf_page_count()
{
    return this->last_data_page - this->first_data_page + 1;
}


io::PagedFile *ISAMTree::get_pfile() 
{
    return this->pfile;
}


bool ISAMTree::is_fixed_length()
{
    return this->fixed_length;
}


catalog::KeyCmpFunc ISAMTree::get_key_cmp()
{
    return this->key_cmp;
}


size_t ISAMTree::memory_utilization()
{
    return (this->tombstone_bloom_filter) ? this->tombstone_bloom_filter->memory_utilization() : 0;
}


size_t ISAMTree::tombstone_count()
{
    return this->tombstone_cnt;
}


}}
