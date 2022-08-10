/*
 *
 */

#include "sampling/lsmtree.hpp"

namespace lsm {
namespace sampling {

std::unique_ptr<LSMTree> LSMTree::create(size_t memtable_capacity, 
                                         size_t scale_factor,
                                         std::unique_ptr<global::g_state> state,
                                         merge_policy policy,
                                         bool bloom_filters, bool range_filters, 
                                         double max_deleted_proportion,
                                         bool unsorted_memtable,
                                         size_t in_mem_levels) 
{
    auto lsm = new LSMTree(memtable_capacity, scale_factor, std::move(state),
                           policy, bloom_filters, range_filters, max_deleted_proportion, 
                           unsorted_memtable, in_mem_levels);

    return std::unique_ptr<LSMTree>(lsm);
}


LSMTree::LSMTree(size_t memtable_capacity, size_t scale_factor,
                 std::unique_ptr<global::g_state> state, merge_policy policy,
                 bool bloom_filters, bool range_filters, 
                 double max_deleted_proportion,
                 bool unsorted_memtable,
                 size_t in_mem_levels)
{
    this->rec_count = 0;
    this->memtable_capacity = memtable_capacity;
    this->scale_factor = scale_factor;
    this->policy = policy;
    this->max_deleted_proportion = max_deleted_proportion;
    this->state = std::move(state);
    this->memory_levels = in_mem_levels;

    this->bloom_filters = bloom_filters;
    this->range_filters = range_filters;

    if (unsorted_memtable) {
        this->memtable = std::make_unique<ds::UnsortedMemTable>(memtable_capacity, this->state.get());
    } else {
        this->memtable = std::make_unique<ds::MapMemTable>(memtable_capacity, this->state.get());
    }

    if (range_filters) {
        // TODO: Implement range filtering
    }
}

size_t LSMTree::grow()
{
    ssize_t lowest_level = this->levels.size() - 1;
    size_t new_record_capacity;
    size_t new_run_capacity;

    if (lowest_level == -1) {
        new_record_capacity = this->memtable->get_capacity() * scale_factor;
        new_run_capacity = 1;

        if (this->policy == TIERING)  {
            new_record_capacity = this->memtable->get_capacity();
            new_run_capacity = this->scale_factor;
        }
    } else {
        new_record_capacity =
            this->scale_factor * this->levels[lowest_level]->get_record_capacity();

        new_run_capacity = 1;
        if (this->policy == TIERING) {
            new_record_capacity = this->levels[lowest_level]->get_run_capacity() *
                this->levels[lowest_level]->get_record_capacity();
            new_run_capacity =
                this->scale_factor + this->levels[lowest_level]->get_run_capacity();
        }
    }

    auto new_level = std::make_unique<ISAMLevel>(new_run_capacity, new_record_capacity, std::vector<io::IndexPagedFile *>(), this->state.get(), this->max_deleted_proportion, this->bloom_filters);

    this->levels.emplace_back(std::move(new_level));

    return lowest_level + 1;
}


void LSMTree::merge_memtable()
{
    auto iter = this->memtable->start_sorted_scan();

    // run down the tree to find the first level capable of accommodating a
    // merge with the level above it.
    ssize_t merge_level_idx = -1;
    size_t incoming_record_count = this->memtable->get_capacity();
    for (size_t i=0; i<this->levels.size(); i++) {
        if (this->levels[i]->can_merge_with(incoming_record_count)) {
            merge_level_idx = i;
            break;
        }

        incoming_record_count = this->levels[i]->get_record_count();
    }

    // if we did not find a level to merge into, we need to create a new one,
    // and make that the merge level.
    if (merge_level_idx == -1) {
        merge_level_idx = this->grow();
    }

    // climb back up the tree, merging levels down until we hit the top
    for (size_t i=merge_level_idx; i>0; i--) {
        this->levels[i]->merge_with(this->levels[i-1].get());
        this->levels[i-1]->truncate();
    }

    // merge the sorted buffer into the first level
    this->levels[0]->merge_with(std::move(iter), this->memtable->tombstone_count());
    
    // truncate the memtable
    this->memtable->truncate();
}


int LSMTree::insert(byte *key, byte *value, Timestamp time)
{
    if (this->memtable->is_full()) {
        this->merge_memtable();
    }

    auto res = this->memtable->insert(key, value, time);

    this->rec_count += res;
    return res;
}


int LSMTree::remove(byte *key, byte *value, Timestamp time)
{
    // NOTE: tombstone based deletion. This will not first check if a record
    // with a matching key and value exists prior to inserting, or if other
    // tombstones already exist. If an identical tombstone already exists
    // within the memtable, it will fail to insert.
    if (this->memtable->is_full()) {
        this->merge_memtable();
    }

    auto res = this->memtable->insert(key, value, time, true);

    this->rec_count += res;
    return res;
}


int LSMTree::update(byte *key, byte *old_value, byte *new_value, Timestamp time)
{
    return this->remove(key, old_value, time) ? this->insert(key, new_value, time) : 0;
}


io::Record LSMTree::get(const byte *key, FrameId *frid, Timestamp time)
{
    io::Record record = this->memtable->get(key, time);
    if (record.is_valid()) {
        if (record.is_tombstone()) {
            *frid = INVALID_FRID;
            return io::Record();
        }

        *frid = INVALID_FRID;
        return record;
    }

    // then, we search the tree from newest levels to oldest
    for (auto &level : this->levels) {
        if (level) {
            record = level->get(key, frid, time);
            if (record.is_valid()) {
                if (record.is_tombstone()) {
                    this->state->cache->unpin(*frid);
                    *frid = INVALID_FRID;
                    return io::Record();
                }
                return record;
            }
        }
    }

    // if we get here, the desired record doesn't exist, so we
    // return an invalid record.
    if (*frid != INVALID_FRID) {
        this->state->cache->unpin(*frid);
    }

    *frid = INVALID_FRID;
    return io::Record();
}


bool LSMTree::has_tombstone(const byte *key, const byte *val, Timestamp time)
{
    if (this->memtable->has_tombstone(key, val, time)) {
        return true;
    }

    FrameId frid = INVALID_FRID;
    for (auto &level : this->levels) {
        if (level) {
            auto record = level->get_tombstone(key, val, &frid, time);
            if (record.is_valid()) {
                this->state->cache->unpin(frid);
                return true;
            }
        }
    }

    // if we get here, the desired record doesn't exist, so we
    // return an invalid record.
    if (frid != INVALID_FRID) {
        this->state->cache->unpin(frid);
    }

    return false;
}


/*
std::unique_ptr<Sample> LSMTree::range_sample(byte *start_key, byte *stop_key, size_t sample_size, size_t *rejections, size_t *attempts)
{
    auto sample = std::make_unique<Sample>(sample_size);
    std::vector<std::unique_ptr<SampleRange>> ranges;
    size_t total_elements = 0;

    size_t rej = 0;
    size_t atmpts = 0;

    auto memtable_range = this->memtable->get_sample_range(start_key, stop_key);
    if (memtable_range) {
        total_elements += memtable_range->length();
        ranges.emplace_back(std::move(memtable_range));
    }

    for (auto &level : this->levels) {
        auto level_ranges = level->get_sample_ranges(start_key, stop_key);
        for (auto &range : level_ranges) {
            total_elements += range->length();
            ranges.emplace_back(std::move(range));
        }
    }

    if (total_elements == 0) {
        return nullptr;
    }

    std::vector<double> weights(ranges.size());
    for (size_t i=0; i<ranges.size(); i++) {
        weights[i] = (double) ranges[i]->length() / (double) total_elements;
    }

    walker::AliasStructure alias(&weights, this->state->rng);

    size_t i=0;
    while (i < sample_size) {
        size_t range = alias.get();

        // If the sample range resides on disk, a page will
        // need to be pinned in the cache. For memory safety,
        // we pass the pinned FrameId back to here, and unpin
        // it later, once the record has been copied into the
        // sample.
        FrameId frid = INVALID_FRID;
        auto record = ranges[range]->get(&frid);

        if (record.is_valid() && !this->is_deleted(record)) {
            sample->add_record(record);
            i++;
        } else {
            rej++;
        }

        atmpts++;

        // Adding a record to the sample makes a deep copy of
        // it, so it is now safe to unpin the page it was drawn
        // from.
        if (frid != INVALID_FRID) {
            this->state->cache->unpin(frid);
        }

        if (atmpts > 5 * sample_size && rej == atmpts) {
            // assume nothing in the sample range
            return nullptr;
        }
    }

    if (rejections) {
        *rejections = rej;
    }

    if (attempts) {
        *attempts = atmpts;
    }

    return sample;
}
*/

std::unique_ptr<Sample> LSMTree::range_sample(byte *start_key, byte *stop_key, size_t sample_size, size_t *rejections, size_t *attempts)
{
    auto sample = std::make_unique<Sample>(sample_size);
    std::vector<std::unique_ptr<SampleRange>> ranges;
    size_t total_elements = 0;

    size_t rej = 0;
    size_t atmpts = 0;

    auto memtable_range = this->memtable->get_sample_range(start_key, stop_key);
    if (memtable_range) {
        total_elements += memtable_range->length();
        ranges.emplace_back(std::move(memtable_range));
    }

    for (auto &level : this->levels) {
        auto level_ranges = level->get_sample_ranges(start_key, stop_key);
        for (auto &range : level_ranges) {
            total_elements += range->length();
            ranges.emplace_back(std::move(range));
        }
    }

    if (total_elements == 0) {
        return nullptr;
    }

    std::vector<double> weights(ranges.size());
    for (size_t i=0; i<ranges.size(); i++) {
        weights[i] = (double) ranges[i]->length() / (double) total_elements;
    }

    walker::AliasStructure alias(&weights, this->state->rng);

    size_t memtable_samples = 0;
    std::vector<std::pair<PageId, io::PagedFile *>> pages;
    for (size_t i=0; i<sample_size; i++) {
        this->add_page_to_sample(pages, &alias, &memtable_samples, ranges);
    }

    this->state->cache->reset_miss_counter();
    this->state->cache->reset_io_time();

    // NOTE: The memtable_samples check here is not technically redundant, as
    // it covers the case of all records to be sampled falling into the
    // memtable initially, in which case pages.size() == 0, but
    // memtable_samples > 0 would be true, and without the check we'd bypass
    // the memtable sampling procedure altogether.
    while (pages.size() > 0 || memtable_samples > 0) { 
        auto pinned = this->cache()->pin_multiple(pages);
        for (auto pin : pinned) {
            atmpts++;

            auto record = this->sample_from(pin.second);

            // check if we should reject the sample, and if so add another page
            // to the pages vector
            bool rejected = false;
            if (this->reject_sample(record, start_key, stop_key)) {
                this->add_page_to_sample(pages, &alias, &memtable_samples, ranges);
                rejected = true;
                rej++;
            } 

            if (!rejected) {
                sample->add_record(record);
            }
        }

        this->cache()->unpin(pinned);

        // perform sampling from the memtable
        while (memtable_samples > 0) {
            // If we can sample from the memtable, it will be at index 0 of the array.
            // If index 0 is *not* the memtable, it should be impossible to get here as
            // memtable_samples == 0 must be true.
            auto record = ranges[0]->get(nullptr); 
            memtable_samples--;

            bool rejected = false;
            if (this->reject_sample(record, start_key, stop_key)) {
                this->add_page_to_sample(pages, &alias, &memtable_samples, ranges);
                rejected = true;
                rej++;
            } 

            if (!rejected) {
                sample->add_record(record);
            }
        }

        // quick + dirty check to avoid infinite loops when sampling an empty
        // range; breaks correctness, but this situation should be avoidable 
        // during testing/benchmarking. A more formally correct solution is an
        // implementation detail that shouldn't be relevant here, I don't think.
        if (atmpts > 5 * sample_size && rej == atmpts) {
            // assume nothing in the sample range
            return nullptr;
        }
    }

    *rejections = rej;
    *attempts = atmpts;

    return sample;
}

std::unique_ptr<Sample> LSMTree::range_sample_bench(byte *start_key, byte *stop_key, size_t sample_size, size_t *rejections, 
                                           size_t *attempts, long *buffer_time, long *bounds_time, long *walker_time,
                                           long *sample_time, long *rejection_time)
{
    auto sample = std::make_unique<Sample>(sample_size);
    std::vector<std::unique_ptr<SampleRange>> ranges;
    size_t total_elements = 0;

    size_t rej = 0;
    size_t atmpts = 0;

    auto buffer_proc_start = std::chrono::high_resolution_clock::now();
    auto memtable_range = this->memtable->get_sample_range(start_key, stop_key);
    if (memtable_range) {
        total_elements += memtable_range->length();
        ranges.emplace_back(std::move(memtable_range));
    }
    auto buffer_proc_stop = std::chrono::high_resolution_clock::now();

    auto bounds_start = std::chrono::high_resolution_clock::now();
    for (auto &level : this->levels) {
        auto level_ranges = level->get_sample_ranges(start_key, stop_key);
        for (auto &range : level_ranges) {
            total_elements += range->length();
            ranges.emplace_back(std::move(range));
        }
    }
    auto bounds_stop = std::chrono::high_resolution_clock::now();

    if (total_elements == 0) {
        return nullptr;
    }

    auto walker_start = std::chrono::high_resolution_clock::now();
    std::vector<double> weights(ranges.size());
    for (size_t i=0; i<ranges.size(); i++) {
        weights[i] = (double) ranges[i]->length() / (double) total_elements;
    }

    walker::AliasStructure alias(&weights, this->state->rng);
    auto walker_stop = std::chrono::high_resolution_clock::now();

    size_t memtable_samples = 0;
    std::vector<std::pair<PageId, io::PagedFile *>> pages;
    for (size_t i=0; i<sample_size; i++) {
        this->add_page_to_sample(pages, &alias, &memtable_samples, ranges, sample_time);
    }

    long total_rejection = 0;
    long total_sample = 0;

    this->state->cache->reset_miss_counter();
    this->state->cache->reset_io_time();

    // NOTE: The memtable_samples check here is not technically redundant, as
    // it covers the case of all records to be sampled falling into the
    // memtable initially, in which case pages.size() == 0, but
    // memtable_samples > 0 would be true, and without the check we'd bypass
    // the memtable sampling procedure altogether.
    while (pages.size() > 0 || memtable_samples > 0) { 
        auto pinned = this->cache()->pin_multiple(pages);
        for (auto pin : pinned) {
            atmpts++;

            auto sample_start = std::chrono::high_resolution_clock::now();
            auto record = this->sample_from(pin.second);
            auto sample_stop = std::chrono::high_resolution_clock::now();

            // check if we should reject the sample, and if so add another page
            // to the pages vector
            auto rejection_start = std::chrono::high_resolution_clock::now();
            bool rejected = false;
            if (this->reject_sample(record, start_key, stop_key)) {
                this->add_page_to_sample(pages, &alias, &memtable_samples, ranges, sample_time);
                rejected = true;
                rej++;
            } 
            auto rejection_stop = std::chrono::high_resolution_clock::now();

            auto add_start = std::chrono::high_resolution_clock::now();
            if (!rejected) {
                sample->add_record(record);
            }
            auto add_stop = std::chrono::high_resolution_clock::now();

            total_sample += std::chrono::duration_cast<std::chrono::nanoseconds>(sample_stop - sample_start).count();
            total_sample += std::chrono::duration_cast<std::chrono::nanoseconds>(add_stop - add_start).count();
            total_rejection += std::chrono::duration_cast<std::chrono::nanoseconds>(rejection_stop - rejection_start).count();
        }

        this->cache()->unpin(pinned);

        // perform sampling from the memtable
        while (memtable_samples > 0) {
            // If we can sample from the memtable, it will be at index 0 of the array.
            // If index 0 is *not* the memtable, it should be impossible to get here as
            // memtable_samples == 0 must be true.
            auto record = ranges[0]->get(nullptr); 
            memtable_samples--;

            bool rejected = false;
            if (this->reject_sample(record, start_key, stop_key)) {
                this->add_page_to_sample(pages, &alias, &memtable_samples, ranges, sample_time);
                rejected = true;
                rej++;
            } 

            if (!rejected) {
                sample->add_record(record);
            }
        }

        // quick + dirty check to avoid infinite loops when sampling an empty
        // range; breaks correctness, but this situation should be avoidable 
        // during testing/benchmarking. A more formally correct solution is an
        // implementation detail that shouldn't be relevant here, I don't think.
        if (atmpts > 5 * sample_size && rej == atmpts) {
            // assume nothing in the sample range
            return nullptr;
        }
    }

    *rejections = rej;
    *attempts = atmpts;

    auto buffer_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(buffer_proc_stop - buffer_proc_start).count();
    auto bounds_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(bounds_stop - bounds_start).count();
    auto walker_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(walker_stop - walker_start).count();

    (*walker_time) += walker_duration;
    (*bounds_time) += bounds_duration;
    (*buffer_time) += buffer_duration;
    (*sample_time) += total_sample;
    (*rejection_time) += total_rejection;

    return sample;
}


io::Record LSMTree::sample_from(FrameId frid)
{
    byte *frame = this->cache()->get_frame_ptr(frid);
    auto page = io::wrap_page(frame);

    // the page is empty--shouldn't happen, but just in case...
    if (page->get_max_sid() == INVALID_SID) {
        return io::Record();
    }

    SlotId sid = 1 + gsl_rng_uniform_int(this->state->rng, page->get_max_sid());

    return page->get_record(sid);
}


void LSMTree::add_page_to_sample(std::vector<std::pair<PageId, io::PagedFile *>> &pages, walker::AliasStructure *alias, size_t *memtable_samples, std::vector<std::unique_ptr<SampleRange>> &ranges, long *sample_time)
{
    if (sample_time) {
        auto start = std::chrono::high_resolution_clock::now();
        auto pid = ranges[alias->get()]->get_page();
                
        // note: any INVALID_PIDs in here indicate that the memtable should be sampled
        if (pid == INVALID_PID) {
            (*memtable_samples)++;
        } else {
            pages.push_back({pid, this->state->file_manager->get_pfile(pid.file_id)});
        }
        auto stop = std::chrono::high_resolution_clock::now();

        (*sample_time) += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    } else {
        auto pid = ranges[alias->get()]->get_page();
                
        // note: any INVALID_PIDs in here indicate that the memtable should be sampled
        if (pid == INVALID_PID) {
            (*memtable_samples)++;
        } else {
            pages.push_back({pid, this->state->file_manager->get_pfile(pid.file_id)});
        }
    }
}


bool LSMTree::reject_sample(io::Record record, byte *lower_key, byte *upper_key)
{
    if (!record.is_valid() || record.is_tombstone()) {
        return true;
    }

    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();

    if (this->state->record_schema->get_key_cmp()(key, lower_key) < 0 
        || this->state->record_schema->get_key_cmp()(key, upper_key) > 0) {
        return true;
    }

    if (this->is_deleted(record)) {
        return true;
    }

    return false;
}


bool LSMTree::is_deleted(io::Record record)
{
    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();
    auto val = this->state->record_schema->get_val(record.get_data()).Bytes();
    auto time = record.get_timestamp();

    return this->has_tombstone(key, val, time);
}


size_t LSMTree::record_count()
{
    return this->rec_count;
}


size_t LSMTree::depth()
{
    return this->levels.size();
}


size_t LSMTree::memory_utilization(bool detail_print)
{
    size_t total = 0;

    for (size_t i=0; i<this->levels.size(); i++) {
        if (this->levels[i]) {
            auto level_util = this->levels[i]->memory_utilization();
            total += level_util;

            if (detail_print) {
                fprintf(stderr, "Level [%ld] aux memory: %ld\n", i, level_util);
            }
        }
    }
    
    return total;
}


catalog::FixedKVSchema *LSMTree::schema() 
{
    return this->state->record_schema.get();
}


io::ReadCache *LSMTree::cache()
{
    return this->state->cache.get();
}


global::g_state *LSMTree::global_state()
{
    return this->state.get();
}

}
}
