#pragma once

#include <atomic>
#include <numeric>

#include "lsm/IsamTree.h"
#include "lsm/MemTable.h"
#include "lsm/MemoryLevel.h"
#include "lsm/DiskLevel.h"
#include "ds/Alias.h"

namespace lsm {

thread_local size_t sampling_attempts;
thread_local size_t sampling_rejections;
/*
 * thread_local size_t various_sampling_times go here.
 */

/*
 * LSM Tree configuration global variables
 */

// True for memtable rejection sampling
static constexpr bool LSM_REJ_SAMPLE = true;

// True for leveling, false for tiering
static constexpr bool LSM_LEVELING = true;

class LSMTree {
public:
    LSMTree(size_t memtable_cap, size_t memtable_bf_sz, size_t scale_factor, size_t memory_levels,
            gsl_rng *rng) 
        : active_memtable(0), memory_levels(std::vector<MemoryLevel*>(memory_levels)),
          scale_factor(scale_factor), 
          memtable_1(new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng)), 
          memtable_2(new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng)),
          memtable_1_merging(false), memtable_2_merging(false) {}

    int insert(const char *key, const char *val, bool tombstone=false) {
        // spin while both memtables are blocked and insertion fails
        // This can be switched out for a condition variable later
        MemTable *table;
        while (!(table = this->memtable()) || !(table->append(key, val, tombstone)))
            ;

        return 1;
    }

    char *range_sample(const char *lower_key, const char *upper_key, size_t sample_sz, char *buffer, char *utility_buffer, gsl_rng *rng) {
        // Allocate buffer into which to write the samples
        char *sample_set = new char[sample_sz * record_size];
        size_t sample_idx = 0;

        // Obtain the sampling ranges for each level
        std::vector<std::pair<const char *, const char *>> memory_ranges;
        std::vector<std::pair<PageNum, PageNum>> disk_ranges;
        std::vector<size_t> record_counts;

        MemTable *memtable = nullptr;

        while (!memtable) {
            memtable = this->memtable();
        }

        record_counts.push_back(memtable->get_record_count());

        for (auto &level : this->memory_levels) {
            if (level) {
                auto ranges = level->sample_ranges(lower_key, upper_key);
                for (auto range : ranges) {
                    memory_ranges.push_back(range);
                    record_counts.push_back((range.second - range.first) / record_size);
                }
            }
        }

        for (auto &level : this->disk_levels) {
            if (level) {
                auto ranges = level->sample_ranges(lower_key, upper_key, buffer);
                for (auto range : ranges) {
                    disk_ranges.push_back(range);
                    record_counts.push_back((range.second - range.first) * (PAGE_SIZE / record_size));
                }
            }
        }

        size_t total_records = std::accumulate(record_counts.begin(), record_counts.end(), 0);

        // NOTE: With the full pre-calculation with record offset only,
        // we don't actually need an alias structure anymore for the
        // non-weighted case, I don't think.

        /*
        std::vector<double> weights(record_counts.size());
        for (size_t i=0; i < record_counts.size(); i++) {
            weights[i] = (double) record_counts[i] / (double) total_records;
        }

        auto alias = Alias(weights);
        */

        // For implementation convenience, we'll treat the very
        // first sampling pass as though it were a sampling
        // pass following one in which every single sample
        // was rejected
        size_t rejections = sample_sz;
        sampling_attempts = 0;
        sampling_rejections = 0;
        
        do {
            // pre-calculate the random numbers. If a sample rejects,
            // we'll track that and redo the rejections in bulk, until
            // there are no further rejections.
            std::vector<size_t> to_sample(rejections);
            for (size_t i=0; i<rejections; i++) {
                to_sample[i] = gsl_rng_uniform_int(rng, total_records);
            }

            // reset rejection counter to begin tracking for next
            // sampling pass
            rejections = 0;

            // Rather than hopping all over the place, we'll draw the
            // sample records in order to maximize locality and simplify
            // sampling logic.
            std::sort(to_sample.begin(), to_sample.end());

            size_t level_idx = 0;
            size_t records_so_far = 0;
            for (size_t i=0; i<to_sample.size(); i++) {
                // Check to see if we need to advance the level index
                // for the current record to be sampled. If so, advance
                // until we hit the level containing the record
                while (to_sample[i] > record_counts[level_idx]) {
                    records_so_far += record_counts[level_idx++];
                }

                size_t record_idx = to_sample[i] - records_so_far;
                sampling_attempts++;

                // get the record
                const char *sample_record;
                if (level_idx == 0) {
                    sample_record = memtable->get_record(record_idx);
                } else if (level_idx < memory_levels.size()) {
                    // sample from the memory level
                } else {
                    // sample from the disk
                }
                
                // check for rejection, either the record is nullptr, meaning that the
                // run itself returned with a rejection (out of range on the last page of
                // an ondisk ISAMTree), or the record is a tombstone, outside of the key
                // range, or has been deleted.
                if (!sample_record || rejection(sample_record, level_idx, lower_key, upper_key, utility_buffer)) {
                    sampling_rejections++;
                    rejections++; 
                    continue; 
                }

                // if record isn't rejected, add it to the sample set
                memcpy(sample_set + (sample_idx++ * record_size), sample_record, record_size);
            }
        } while (sample_idx < sample_sz);

        return sample_set;
    }

    // Checks the tree and memtable for a tombstone corresponding to
    // the provided record in any level *above* the record_level, which
    // should correspond to the level containing the record in question
    bool is_deleted(const char *record, size_t record_level, char *buffer);

private:
    MemTable *memtable_1;
    MemTable *memtable_2;
    std::atomic<bool> active_memtable;
    std::atomic<bool> memtable_1_merging;
    std::atomic<bool> memtable_2_merging;

    size_t scale_factor;

    std::vector<MemoryLevel *> memory_levels;
    std::vector<DiskLevel *> disk_levels;

    MemTable *memtable() {
        if (memtable_1_merging && memtable_2_merging) {
            return nullptr;
        }

        return (active_memtable) ? memtable_2 : memtable_1;
    }

    inline bool rejection(const char *record, size_t level, const char *lower_bound, const char *upper_bound, char *buffer) {
        return is_tombstone(record) || key_cmp(get_key(record), lower_bound) < 0 || key_cmp(get_key(record), upper_bound) > 0 || this->is_deleted(record, level, buffer);
    }

};
}
