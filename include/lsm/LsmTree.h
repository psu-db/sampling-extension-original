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

    char *range_sample(const char *lower_key, const char *upper_key, size_t sample_sz, char *buffer) {
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

        std::vector<double> weights(record_counts.size());
        size_t total_records = std::accumulate(record_counts.begin(), record_counts.end(), 0);
        for (size_t i=0; i < record_counts.size(); i++) {
            weights[i] = (double) record_counts[i] / (double) total_records;
        }

        auto alias = Alias(weights);

        while (sample_idx < sample_sz) {
            // get sample

            // reject if needed

            // copy record into sample_set
        }

        return sample_set;
    }

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

};
}
