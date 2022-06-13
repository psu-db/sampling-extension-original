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
                                         double max_deleted_proportion) 
{
    auto lsm = new LSMTree(memtable_capacity, scale_factor, std::move(state),
                                     policy, bloom_filters, range_filters, max_deleted_proportion);
    return std::unique_ptr<LSMTree>(lsm);
}


LSMTree::LSMTree(size_t memtable_capacity, size_t scale_factor,
                 std::unique_ptr<global::g_state> state, merge_policy policy,
                 bool bloom_filters, bool range_filters, 
                 double max_deleted_proportion)
{
    this->rec_count = 0;
    this->memtable_capacity = memtable_capacity;
    this->scale_factor = scale_factor;
    this->policy = policy;
    this->max_deleted_proportion = max_deleted_proportion;
    this->state = std::move(state);

    this->bloom_filters = bloom_filters;
    this->range_filters = range_filters;
    this->memtable = std::make_unique<ds::MapMemTable>(memtable_capacity, this->state.get());
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

    auto new_level = std::make_unique<BTreeLevel>(new_run_capacity, new_record_capacity, std::vector<io::IndexPagedFile *>(), this->state.get(), this->max_deleted_proportion);

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
    this->levels[0]->merge_with(std::move(iter));
    
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
    // tombstones already exist. If the record being deleted exists within the
    // memtable, it will be replaced by the tombstone.
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


io::Record LSMTree::get(const byte *key, FrameId *frid, Timestamp time, bool skip_delete_check)
{
    // first, we check the memory table
    auto record = this->memtable->get(key, time);
    if (record.is_valid()) {
        if (!skip_delete_check && record.is_tombstone()) {
            *frid = INVALID_FRID;
            return io::Record();
        }

        *frid = INVALID_FRID;
        return record;
    }

    // then, we search the tree from newest levels to oldest
    for (auto &level : this->levels) {
        if (level) {
            record = level->get_by_key(key, frid, time);
            if (record.is_valid()) {
                if (!skip_delete_check && record.is_tombstone()) {
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


std::unique_ptr<Sample> LSMTree::range_sample(byte *start_key, byte *stop_key, size_t sample_size, size_t *rejections, size_t *attempts)
{
    auto sample = std::make_unique<Sample>(sample_size);
    std::vector<std::unique_ptr<SampleRange>> ranges;
    size_t total_elements = 0;

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
        FrameId frid;
        auto record = ranges[range]->get(&frid);

        if (record.is_valid() && !this->is_deleted(record)) {
            sample->add_record(record);
            i++;
        } else {
           if (rejections) {
                (*rejections)++;
            } 
        }

        if (attempts) {
            (*attempts)++;
        }

        // Adding a record to the sample makes a deep copy of
        // it, so it is now safe to unpin the page it was drawn
        // from.
        if (frid != INVALID_FRID) {
            this->state->cache->unpin(frid);
        }
    }

    return sample;
}


bool LSMTree::is_deleted(io::Record record)
{
    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();
    auto time = record.get_timestamp();

    bool deleted = false;

    FrameId frid;
    auto res = this->get(key, &frid, time, true);

    if (res.is_tombstone()) {
        deleted = true;
    }

    this->state->cache->unpin(frid);
    return deleted;
}


size_t LSMTree::record_count()
{
    return this->rec_count;
}


size_t LSMTree::depth()
{
    return this->levels.size();
}


catalog::FixedKVSchema *LSMTree::schema() 
{
    return this->state->record_schema.get();
}


io::ReadCache *LSMTree::cache()
{
    return this->state->cache.get();
}

}
}
