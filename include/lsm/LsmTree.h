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
    LSMTree(size_t memtable_cap, size_t memtable_bf_sz, size_t scale_factor, size_t n_memory_levels,
            gsl_rng *rng) 
        : active_memtable(0), memory_levels(n_memory_levels, nullptr),
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

    char *range_sample(char *sample_set, const char *lower_key, const char *upper_key, size_t sample_sz, char *buffer, char *utility_buffer, gsl_rng *rng) {
        // Allocate buffer into which to write the samples
        size_t sample_idx = 0;

        // Obtain the sampling ranges for each level
        std::vector<std::pair<RunId, std::pair<const char *, const char *>>> memory_ranges;
        std::vector<std::pair<RunId, std::pair<PageNum, PageNum>>> disk_ranges;
        std::vector<size_t> record_counts;

        MemTable *memtable = nullptr;

        while (!memtable) {
            memtable = this->memtable();
        }

        size_t memtable_cutoff = memtable->get_record_count() - 1;
        record_counts.push_back(memtable_cutoff + 1);

        for (auto &level : this->memory_levels) {
            if (level) {
                auto ranges = level->sample_ranges(lower_key, upper_key);
                for (auto range : ranges) {
                    memory_ranges.push_back(range);
                    record_counts.push_back((range.second.second - range.second.first) / record_size);
                }
            }
        }

        for (auto &level : this->disk_levels) {
            if (level) {
                auto ranges = level->sample_ranges(lower_key, upper_key, buffer);
                for (auto range : ranges) {
                    disk_ranges.push_back(range);
                    record_counts.push_back((range.second.second - range.second.first) * (PAGE_SIZE / record_size));
                }
            }
        }

        size_t total_records = std::accumulate(record_counts.begin(), record_counts.end(), 0);

        std::vector<double> weights(record_counts.size());
        for (size_t i=0; i < record_counts.size(); i++) {
            weights[i] = (double) record_counts[i] / (double) total_records;
        }

        auto alias = Alias(weights);

        // For implementation convenience, we'll treat the very
        // first sampling pass as though it were a sampling
        // pass following one in which every single sample
        // was rejected
        size_t rejections = sample_sz;
        sampling_attempts = 0;
        sampling_rejections = 0;

        std::vector<size_t> run_samples(record_counts.size(), 0);

        do {
            // This *should* be fully reset to 0 at the end of each loop
            // iteration.
            for (size_t i=0; i<run_samples.size(); i++) {
                assert(run_samples[i] == 0);
            }

            // pre-calculate the random numbers. If a sample rejects,
            // we'll track that and redo the rejections in bulk, until
            // there are no further rejections.
            for (size_t i=0; i<rejections; i++) {
                run_samples[alias.get(rng)] += 1;
            }

            // reset rejection counter to begin tracking for next
            // sampling pass
            rejections = 0;
            const char *sample_record;

            // We will draw the records from the runs in order

            // First the memtable,
            while (run_samples[0] > 0) {
                size_t idx = gsl_rng_uniform_int(rng, memtable_cutoff);
                sample_record = memtable->get_record(idx);

                run_samples[0]--;

                if (!add_to_sample(sample_record, INVALID_RID, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff)) {
                    rejections++;
                }
            }

            // Next the in-memory runs
            // QUESTION: can we assume that the memory page size is a multiple of the record length?
            // If so, we can do this. Otherwise, we'll need to do a double roll to get a leaf page
            // first, or use an interface like I have for the ISAM tree for getting a record based
            // on an index offset and a starting page.
            size_t run_offset = 1; // skip the memtable
            for (size_t i=0; i<memory_ranges.size(); i++) {
                size_t range_length = (memory_ranges[i].second.second - memory_ranges[i].second.first) / record_size;
                while (run_samples[i+run_offset] > 0) {
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = memory_ranges[i].second.first + (idx * record_size);
                    run_samples[i+run_offset]--;

                    if (!add_to_sample(sample_record, memory_ranges[i].first, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff)) {
                        rejections++;
                    }
                }

            }

            // Finally, the ISAM Trees
            // NOTE: this setup is not leveraging rolling all the pages first,
            // and then batching the IO operations so that each duplicate page
            // is sampled multiple times in a row. This could be done at the
            // cost of space, by tracking each page and the number of times it
            // is sampled, or at the cost of time, by sorting. In either case,
            // we'd need to double the number of random numbers rolled--as we'd
            // need to roll the pages, and then the record index within each
            // page
            run_offset = 1 + memory_ranges.size(); // Skip the memtable and the memory levels
            size_t records_per_page = PAGE_SIZE / record_size;
            PageNum buffered_page = INVALID_PNUM;
            for (size_t i=0; i<disk_ranges.size(); i++) {
                size_t range_length = (disk_ranges[i].second.second - disk_ranges[i].second.first) * records_per_page;
                size_t level_idx = disk_ranges[i].first.level_idx;
                size_t run_idx = disk_ranges[i].first.run_idx;

                while (run_samples[i+run_offset] > 0) {
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = this->disk_levels[level_idx]->get_run(run_idx)->sample_record(disk_ranges[i].second.first, idx, buffer, buffered_page);
                    run_samples[i+run_offset]--;

                    if (!add_to_sample(sample_record, disk_ranges[i].first, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff)) {
                        rejections++;
                    }
                }
            }

        } while (sample_idx < sample_sz);

        return sample_set;
    }

    // Checks the tree and memtable for a tombstone corresponding to
    // the provided record in any run *above* the rid, which
    // should correspond to the run containing the record in question
    // 
    // Passing INVALID_RID indicates that the record exists within the MemTable
    bool is_deleted(const char *record, const RunId &rid, char *buffer, MemTable *memtable, size_t memtable_cutoff) {
        // check for tombstone in the memtable. This will require accounting for the cutoff eventually.
        if (memtable->check_tombstone(get_key(record), get_val(record))) {
            return true;
        }

        // if the record is in the memtable, then we're done.
        if (rid == INVALID_RID) {
            return false;
        }

        // check all runs on all levels above the level containing the record
        for (size_t lvl=0; lvl<rid.level_idx; lvl++) {
            if (lvl<memory_levels.size()) {
                for (size_t run=0; run<memory_levels[lvl]->run_count(); run++) {
                    if (memory_levels[lvl]->get_run(run)->check_tombstone(get_key(record), get_val(record))) {
                        return true;
                    }
                }
            } else {
                size_t isam_lvl = lvl - memory_levels.size();
                for (size_t run=0; run<disk_levels[isam_lvl]->run_count(); run++) {
                    if (disk_levels[isam_lvl]->get_run(run)->check_tombstone(get_key(record), get_val(record), buffer)) {
                        return true;
                    }
                }
            }
        }

        // check any runs on the record's level that are before it
        for (size_t run=0; run<rid.run_idx; run++) {
            if (rid.level_idx < memory_levels.size()) {
                if (memory_levels[rid.level_idx]->get_run(run)->check_tombstone(get_key(record), get_val(record))) {
                    return true;
                }
            } else {
                if (disk_levels[rid.level_idx - memory_levels.size()]->get_run(run)->check_tombstone(get_key(record), get_val(record), buffer)) {
                    return true;
                }
            }
        }

        return false;
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

    inline bool rejection(const char *record, RunId rid, const char *lower_bound, const char *upper_bound, char *buffer, MemTable *memtable, size_t memtable_cutoff) {
        return is_tombstone(record) || key_cmp(get_key(record), lower_bound) < 0 || key_cmp(get_key(record), upper_bound) > 0 || this->is_deleted(record, rid, buffer, memtable, memtable_cutoff);
    }

    inline size_t rid_to_disk(RunId rid) {
        return rid.level_idx - this->memory_levels.size();
    }

    inline bool add_to_sample(const char *record, RunId rid, const char *upper_key, const char *lower_key, char *io_buffer,
                              char *sample_buffer, size_t &sample_idx, MemTable *memtable, size_t memtable_cutoff) {
        sampling_attempts++;
        if (!record || rejection(record, rid, lower_key, upper_key, io_buffer, memtable, memtable_cutoff)) {
            sampling_rejections++;
            return false;
        }

        memcpy(sample_buffer + (sample_idx++ * record_size), record, record_size);
        return true;
    }

};
}
