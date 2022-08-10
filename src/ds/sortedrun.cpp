/*
 *
 */

#include "ds/sortedrun.hpp"

namespace lsm { namespace ds {

std::unique_ptr<SortedRun> SortedRun::create(std::unique_ptr<iter::MergeIterator> iter, size_t record_cnt, bool bloom_filters, global::g_state *state, size_t tombstone_count)
{
    size_t buffer_size = record_cnt * state->record_schema->record_length();
    auto buffer = mem::create_aligned_buffer(buffer_size);

    SortedRun::initialize(buffer.get(), std::move(iter), record_cnt, state, bloom_filters, tombstone_count);

    return std::make_unique<SortedRun>(std::move(buffer), record_cnt, state, tombstone_count);
}


SortedRun::SortedRun(io::PagedFile *pfile, global::g_state *state)
    : data_array(mem::wrap_aligned_buffer(nullptr))
{
    this->state = state;
}


void SortedRun::initialize(byte *buffer, std::unique_ptr<iter::MergeIterator> record_iter, size_t record_count, global::g_state *state, bool bloom_filters, size_t tombstone_count)
{
    size_t offset = 0;
    size_t idx = 0;

    while (idx < record_count && record_iter->next()) {
        auto rec = record_iter->get_item();
        memcpy(buffer + offset, rec.get_data(), rec.get_length());
        offset += rec.get_length();
        idx++;

        if (rec.is_tombstone()) {
            // do tombstoney stuff
        }
    }
}


SortedRun::SortedRun(mem::aligned_buffer data_array, size_t record_count, global::g_state *state, size_t tombstone_count) 
                : data_array(std::move(data_array))
{
    this->record_cnt = record_count;
    this->state = state;
    size_t tombstones = tombstone_count;
}


ssize_t SortedRun::get_lower_bound(const byte *key)
{
    size_t min = 0;
    size_t max = this->record_cnt - 1;

    auto max_key = this->state->record_schema->get_key(this->get_record(max).get_data()).Bytes();

    if (this->get_key_cmp()(key, max_key) > 0) {
        return -1;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto run_key =this->state->record_schema->get_key(this->get_record(mid).get_data()).Bytes(); 

        if (this->get_key_cmp()(key, run_key) > 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return min;
}


ssize_t SortedRun::get_upper_bound(const byte *key)
{
    size_t min = 0;
    size_t max = this->record_cnt - 1;

    auto min_key = this->state->record_schema->get_key(this->get_record(0).get_data()).Bytes();

    if (this->get_key_cmp()(key, min_key) < 0) {
        return -1;
    }

    while (min < max) {
        size_t mid = (min + max) / 2;
        auto run_key =this->state->record_schema->get_key(this->get_record(mid).get_data()).Bytes(); 

        if (this->get_key_cmp()(key, run_key) >= 0) {
            min = mid + 1;
        } else {
            max = mid;
        }
    }

    return min;
}


catalog::KeyCmpFunc SortedRun::get_key_cmp()
{
    return this->state->record_schema->get_key_cmp();
}


io::Record SortedRun::get_record(size_t idx) const
{
    if (idx < this->record_cnt) {
        size_t len = this->state->record_schema->record_length();
        byte *data = this->data_array.get() + idx * len;
        return io::Record(data, len);
    }

    return io::Record();
}


io::Record SortedRun::get(const byte *key, Timestamp time) 
{
    auto key_cmp = this->state->record_schema->get_key_cmp();

    auto idx = this->get_lower_bound(key);

    if (idx == -1) {
        return io::Record();
    }

    io::Record rec;
    const byte *key_ptr;
    do {
        rec = this->get_record(idx);
        key_ptr = this->state->record_schema->get_key(rec.get_data()).Bytes();

        if (rec.get_timestamp() <= time) {
            return rec;
        }

        if (++idx >= this->record_cnt) {
            return io::Record();
        }
    } while (key_cmp(key, key_ptr) == 0);

    return io::Record();
}


io::Record SortedRun::get_tombstone(const byte *key, const byte *val, Timestamp time)
{
    auto key_cmp = this->state->record_schema->get_key_cmp();
    auto val_cmp = this->state->record_schema->get_val_cmp();

    auto idx = this->get_lower_bound(key);

    if (idx == -1) {
        return io::Record();
    }

    io::Record rec;
    const byte *key_ptr;
    const byte *val_ptr;
    do {
        rec = this->get_record(idx);
        key_ptr = this->state->record_schema->get_key(rec.get_data()).Bytes();
        val_ptr = this->state->record_schema->get_val(rec.get_data()).Bytes();

        if (rec.get_timestamp() <= time && rec.is_tombstone()) {
            return rec;
        }

        if (++idx >= this->record_cnt) {
            return io::Record();
        }
    } while (key_cmp(key_ptr, key) == 0 && val_cmp(val_ptr, val) == 0);

    return io::Record();
}


size_t SortedRun::memory_utilization()
{
    return 0;
}


std::unique_ptr<iter::GenericIterator<io::Record>> SortedRun::start_scan()
{
    return std::make_unique<SortedRunRecordIterator>(this, this->record_cnt, 0, this->record_cnt-1);
}


size_t SortedRun::record_count()
{
    return this->record_cnt;
}


size_t SortedRun::tombstone_count()
{
    return this->tombstones;
}


SortedRunRecordIterator::SortedRunRecordIterator(const SortedRun *run, size_t record_count, size_t start_idx, size_t stop_idx)
{
    this->start_idx = start_idx;
    this->stop_idx = stop_idx;
    this->cur_idx = -1;
    this->run = run;
    this->record_count = record_count;
}


bool SortedRunRecordIterator::next()
{
    if (++this->cur_idx <= this->stop_idx) {
        this->current_record = this->run->get_record(this->cur_idx);
        return true;
    }

    return false;
}


io::Record SortedRunRecordIterator::get_item()
{
    return this->current_record;
}


bool SortedRunRecordIterator::supports_rewind()
{
    return true;
}


iter::IteratorPosition SortedRunRecordIterator::save_position()
{
    return this->cur_idx;
}


void SortedRunRecordIterator::rewind(iter::IteratorPosition position) 
{
    this->cur_idx = position;
}


size_t SortedRunRecordIterator::element_count()
{
    return this->record_count;
}


bool SortedRunRecordIterator::supports_element_count()
{
    return true;
}


void SortedRunRecordIterator::end_scan()
{
    this->current_record = io::Record();
    this->cur_idx = -1;
}

}}
