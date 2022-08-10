/*
 *
 */

#include "sampling/sortedlevel.hpp"

namespace lsm { namespace sampling {

SortedLevel::SortedLevel(size_t run_capacity, size_t record_capacity, 
                       std::vector<io::IndexPagedFile *> files, 
                       global::g_state *state,
                       double max_deletion_proportion,
                       bool bloom_filters)
{
    this->run_capacity = run_capacity;
    this->record_capacity = record_capacity;
    this->max_deleted_prop = max_deletion_proportion;

    this->run_count = 0;
    this->record_count = 0;

    this->runs = std::vector<std::unique_ptr<ds::SortedRun>>(run_capacity);
    this->state = state;

    this->record_cmp = state->record_schema->get_record_cmp();
    this->key_cmp = state->record_schema->get_key_cmp();

    this->bloom_filters = bloom_filters;

    for (size_t i=0; i<files.size(); i++) {
        if (i < this->run_capacity) {
            this->runs[i] = std::make_unique<ds::SortedRun>(files[i], state);
            this->run_count++;
            this->record_count += this->runs[i]->record_count();
        }
    }
}


int SortedLevel::emplace_run(std::unique_ptr<ds::SortedRun> run)
{
    if (this->run_count == this->run_capacity) {
        return 0; // no room
    }

    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i].get() == nullptr) {
            this->runs[i] = std::move(run);
            this->run_count++;
            this->record_count += this->runs[i]->record_count();
            return 1;
        }
    }

    return 0;
}


bool SortedLevel::can_emplace_run() 
{
    return this->run_count < this->run_capacity;
}


bool SortedLevel::can_merge_with(size_t incoming_record_count)
{
    if (this->run_count < this->run_capacity) {
        return true;
    } else if (this->run_capacity == 1 && this->record_count + incoming_record_count <= this->record_capacity) {
        return true;
    }

    return false;
}


bool SortedLevel::can_merge_with(LSMTreeLevel *level) 
{
    return this->can_merge_with(level->get_record_count());
}


io::Record SortedLevel::get(const byte *key, FrameId *frid, Timestamp time)
{
    for (int32_t i=this->run_capacity - 1; i >= 0; i--) {
        auto rec = this->runs[i]->get(key, time);
        if (rec.is_valid()) {
            return rec;
        }
    }

    return io::Record();
}


io::Record SortedLevel::get_tombstone(const byte *key, const byte *val, FrameId *frid, Timestamp time)
{
    for (int32_t i=this->run_capacity - 1; i >= 0; i--) {
        auto rec = this->runs[i]->get_tombstone(key, val, time);
        if (rec.is_valid()) {
            return rec;
        }
    }

    return io::Record();
}


int SortedLevel::remove(byte* /*key*/, byte* /*value*/, Timestamp /*time*/)
{
    return 0;
}


void SortedLevel::truncate()
{
    // Because files are std::moved from level to level, this should be safe to
    // do. Any file that has been relocated will no longer be here, so there
    // isn't any risk of closing a file that some other level is currently
    // using. This will, of course, need to be revisited for concurrency.
    this->run_count = 0;
    this->record_count = 0;
}


int SortedLevel::merge_with(LSMTreeLevel *level) 
{
    size_t tombstones = level->get_tombstone_count();
    auto iter = level->start_scan();
    return this->merge_with(std::move(iter), tombstones);
}


int SortedLevel::merge_with(std::unique_ptr<iter::GenericIterator<io::Record>> sorted_itr, size_t tombstone_count)
{
    // iterator must support element count to merge it.
    if (!sorted_itr->supports_element_count()) {
        return 0;
    }

    if (this->can_merge_with(sorted_itr->element_count())) {
        std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iters;
        PageNum existing_record_cnt = 0;
        size_t tombstones = tombstone_count;
        if (this->runs[0]) {
            iters.push_back(this->runs[0]->start_scan());
            tombstones += this->runs[0]->tombstone_count();
            existing_record_cnt += this->runs[0]->record_count();
        }

        size_t new_record_cnt = sorted_itr->element_count();
        iters.push_back(std::move(sorted_itr));
        
        auto merge_itr = std::make_unique<iter::MergeIterator>(iters, this->record_cmp);
        size_t total_record_count = existing_record_cnt + new_record_cnt;

        auto new_run = ds::SortedRun::create(std::move(merge_itr), total_record_count, this->bloom_filters, this->state, tombstones);

        // abort if the creation of the new, merged, level failed for some reason.
        if (!new_run) {
            return 0;
        }

        // FIXME: will need a different approach when concurrency comes into place
        this->truncate();
        this->emplace_run(std::move(new_run));
        return 1;
    }

    return 0;
}


std::vector<std::unique_ptr<SampleRange>> SortedLevel::get_sample_ranges(byte *lower_key, byte *upper_key)
{
    std::vector<std::unique_ptr<SampleRange>> ranges;
    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i]) {
            auto range = SortedSampleRange::create(this->runs[i].get(), lower_key, upper_key, this->state);

            if (range) {
                ranges.emplace_back(std::move(range));
            }
        }
    }

    return ranges;
}


size_t SortedLevel::get_record_capacity()
{
    return this->record_capacity;
}


size_t SortedLevel::get_record_count()
{
    return this->record_count;
}


size_t SortedLevel::get_tombstone_count()
{
    size_t total = 0;
    for (auto &run : this->runs) {
        total += run->tombstone_count();
    }

    return total;
}


size_t SortedLevel::get_run_capacity()
{
    return this->run_capacity;
}


size_t SortedLevel::get_run_count()
{
    return this->run_count;
}


size_t SortedLevel::memory_utilization()
{
    size_t util = 0;
    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i]) {
            util += this->runs[i]->memory_utilization();
        }
    }

    return util;
}


bool SortedLevel::is_memory_resident() 
{
    return true;
}


void SortedLevel::print_level()
{
    return;
}


std::unique_ptr<iter::GenericIterator<Record>> SortedLevel::start_scan()
{
    if (this->runs.size() == 0) {
        return nullptr;
    }

    if (this->runs[0]) {
        return this->runs[0]->start_scan();
    }

    return nullptr;
}
}}
