#pragma once

#include <atomic>
#include <numeric>

#include "lsm/IsamTree.h"
#include "lsm/MemTable.h"
#include "lsm/MemoryLevel.h"
#include "lsm/DiskLevel.h"
#include "ds/Alias.h"

namespace lsm {

thread_local size_t sampling_attempts = 0;
thread_local size_t sampling_rejections = 0;
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
    LSMTree(std::string root_dir, size_t memtable_cap, size_t memtable_bf_sz, size_t scale_factor, size_t memory_levels,
            gsl_rng *rng) 
        : active_memtable(0), memory_levels(memory_levels, 0),
          scale_factor(scale_factor), 
          root_directory(root_dir),
          memory_level_cnt(memory_levels),
          memtable_1(new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng)), 
          memtable_2(new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng)),
          memtable_1_merging(false), memtable_2_merging(false) {}

    ~LSMTree() {
        delete this->memtable_1;
        delete this->memtable_2;

        for (size_t i=0; i<this->memory_levels.size(); i++) {
            delete this->memory_levels[i];
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            delete this->disk_levels[i];
        }

    }

    int append(const char *key, const char *val, bool tombstone, gsl_rng *rng) {
        // NOTE: single-threaded implementation only
        MemTable *mtable;
        while (!(mtable = this->memtable()))
            ;
        
        if (mtable->is_full()) {
            this->merge_memtable(rng);
        }

        return mtable->append(key, val, tombstone);
    }

    void range_sample(char *sample_set, const char *lower_key, const char *upper_key, size_t sample_sz, char *buffer, char *utility_buffer, gsl_rng *rng) {
        // Allocate buffer into which to write the samples
        size_t sample_idx = 0;

        // Obtain the sampling ranges for each level
        std::vector<SampleRange> memory_ranges;
        std::vector<SampleRange> disk_ranges;
        std::vector<size_t> record_counts;

        MemTable *memtable = nullptr;

        while (!memtable) {
            memtable = this->memtable();
        }

        size_t memtable_cutoff = memtable->get_record_count() - 1;
        record_counts.push_back(memtable_cutoff + 1);

        for (auto &level : this->memory_levels) {
            if (level) {
                level->get_sample_ranges(memory_ranges, record_counts, lower_key, upper_key);
            }
        }

        for (auto &level : this->disk_levels) {
            if (level) {
                level->get_sample_ranges(disk_ranges, record_counts, lower_key, upper_key, buffer);
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
                sample_record = memtable->get_record_at(idx);

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
                size_t range_length = memory_ranges[i].high - memory_ranges[i].low;
                auto run_id = memory_ranges[i].run_id;
                while (run_samples[i+run_offset] > 0) {
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = memory_levels[run_id.level_idx]->get_record_at(run_id.run_idx, idx + memory_ranges[i].low);
                    run_samples[i+run_offset]--;

                    if (!add_to_sample(sample_record, memory_ranges[i].run_id, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff)) {
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
                size_t range_length = (disk_ranges[i].high - disk_ranges[i].low + 1) * records_per_page;
                size_t level_idx = disk_ranges[i].run_id.level_idx - this->memory_level_cnt;
                size_t run_idx = disk_ranges[i].run_id.run_idx;

                while (run_samples[i+run_offset] > 0) {
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = this->disk_levels[level_idx]->get_run(run_idx)->sample_record(disk_ranges[i].low, idx, buffer, buffered_page);
                    run_samples[i+run_offset]--;

                    if (!add_to_sample(sample_record, disk_ranges[i].run_id, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff)) {
                        rejections++;
                    }
                }
            }

        } while (sample_idx < sample_sz);
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

        for (size_t lvl=0; lvl<rid.level_idx; lvl++) {
            if (lvl < memory_levels.size()) {
                if (memory_levels[lvl]->tombstone_check(memory_levels[lvl]->get_run_count(), get_key(record), get_val(record))) {
                    return true;
                }
            } else {
                size_t isam_lvl = lvl - memory_levels.size();
                if (disk_levels[isam_lvl]->tombstone_check(disk_levels[isam_lvl]->get_run_count(), get_key(record), get_val(record), buffer)) {
                    return true;
                }

            }
        }

        // check the level containing the run
        if (rid.level_idx < memory_levels.size()) {
            size_t run_idx = std::min((size_t) rid.run_idx, memory_levels[rid.level_idx]->get_run_count() + 1);
            return memory_levels[rid.level_idx]->tombstone_check(run_idx, get_key(record), get_val(record));
        } else {
            size_t isam_lvl = rid.level_idx - memory_levels.size();
            size_t run_idx = std::min((size_t) rid.run_idx, disk_levels[isam_lvl]->get_run_count());
            return disk_levels[isam_lvl]->tombstone_check(run_idx, get_key(record), get_val(record), buffer);
        }
    }


    size_t get_record_cnt() {
        // FIXME: need to account for both memtables with concurrency
        size_t cnt = this->memtable()->get_record_count();

        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (this->memory_levels[i]) cnt += this->memory_levels[i]->get_record_cnt();
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            if (this->disk_levels[i]) cnt += this->disk_levels[i]->get_record_cnt();
        }

        return cnt;
    }

    // FIXME: Technically, this doesn't work properly because the size of the
    // memory_levels is always memory_level_cnt irrespective of which levels
    // are populated. But it should be good enough for now--I'll fix this
    // later.
    size_t get_height() {
        return this->memory_levels.size() + this->disk_levels.size();
    }

private:
    MemTable *memtable_1;
    MemTable *memtable_2;
    std::atomic<bool> active_memtable;
    std::atomic<bool> memtable_1_merging;
    std::atomic<bool> memtable_2_merging;

    size_t scale_factor;

    std::vector<MemoryLevel *> memory_levels;
    size_t memory_level_cnt;
    std::vector<DiskLevel *> disk_levels;

    // The directory containing all of the backing files
    // for this LSM Tree.
    std::string root_directory;

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

    // Add a new level to the tree as part of the merge process. Returns true
    // if the new level is a disk level, and false if it is a memory level.
    // Update the level_idx parameter to the index of the new level in the
    // appropriate backing array.
    inline bool grow(size_t &new_level_idx) {
        bool new_disk_level;
        size_t new_run_cnt = (LSM_LEVELING) ? 1 : this->scale_factor;

        // Determine where to insert the new level
        if (this->memory_levels.size() < this->memory_level_cnt - 1) {
            new_level_idx = this->memory_levels.size();
            this->memory_levels.emplace_back(new MemoryLevel(new_level_idx, new_run_cnt));
            new_disk_level = false;
        } else {
            new_level_idx = this->disk_levels.size();
            this->disk_levels.emplace_back(new DiskLevel(this->memory_levels.size() + new_level_idx, new_run_cnt, this->root_directory));
            new_disk_level = true;
        }

        return new_disk_level;
    }


    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    void merge_memtable(gsl_rng *rng) {
        auto mtable = this->memtable();

        // swapping over to the backup in the case of merging would go here.
        
        bool level_found = false;
        bool disk_level;
        size_t merge_level_idx;

        size_t incoming_rec_cnt = mtable->get_record_count();
        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (level_found) break;

            if (memlevel_can_merge_with(incoming_rec_cnt, i)) {
                merge_level_idx = i;
                level_found = true;
                disk_level = false;
            } else {
                incoming_rec_cnt = this->memory_levels[i]->get_record_cnt();
            }
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            if (level_found) break;

            if (disklevel_can_merge_with(incoming_rec_cnt, i)) {
                merge_level_idx = i;
                level_found = true;
                disk_level = true;
            } else {
                incoming_rec_cnt = this->disk_levels[i]->get_record_cnt();
            }
        }   

        if (!level_found) { 
            disk_level = this->grow(merge_level_idx);
        }

        if (disk_level) {
            for (size_t i=merge_level_idx; i>0; i--) {
                if (LSM_LEVELING) {
                    auto tmp = this->disk_levels[i];
                    this->disk_levels[i] = DiskLevel::merge_levels(this->disk_levels[i], this->disk_levels[i-1], rng);
                    delete tmp;
                } else {
                    this->disk_levels[i]->append_merged_runs(this->disk_levels[i-1], rng);
                }
                delete this->disk_levels[i-1];
                this->disk_levels[i-1] = new DiskLevel(i - 1 + this->memory_level_cnt, (LSM_LEVELING) ? 1 : this->scale_factor, this->root_directory);
            }

            if (LSM_LEVELING) {
                auto tmp = this->disk_levels[0];
                this->disk_levels[0] = DiskLevel::merge_levels(this->disk_levels[0], this->memory_levels[this->memory_level_cnt - 1], rng);
                delete tmp;
            } else {
                this->disk_levels[0]->append_merged_runs(this->memory_levels[this->memory_level_cnt - 1], rng);
            }

            delete this->memory_levels[this->memory_level_cnt - 1];
            this->memory_levels[this->memory_level_cnt - 1] = new MemoryLevel(this->memory_level_cnt - 1, (LSM_LEVELING) ? 1 : this->scale_factor);
            merge_level_idx = this->memory_level_cnt - 1;
        }

        // If we are merging directly into the first level, and it can support the
        // merge, then we can just add the mem table.   
        if (merge_level_idx == 0 && this->memory_levels[0]) {
            
            if (LSM_LEVELING) {
                // FIXME: Kludgey implementation due to interface constraints.
                auto old_level = this->memory_levels[0];
                auto temp_level = new MemoryLevel(0, 1);
                temp_level->append_mem_table(mtable, rng);
                auto new_level = MemoryLevel::merge_levels(old_level, temp_level, rng);

                this->memory_levels[0] = new_level;

                // FIXME: old_level shouldn't necessary be deleted here when
                // concurrency is in play.
                delete temp_level;
                delete old_level;
            } else {
                this->memory_levels[0]->append_mem_table(mtable, rng);
            }

            mtable->truncate();
            return;
        }

        for (size_t i=merge_level_idx; i>0; i++) {
            if (LSM_LEVELING) {
                auto tmp = this->memory_levels[i];
                this->memory_levels[i] = MemoryLevel::merge_levels(this->memory_levels[i], this->memory_levels[i-1], rng);
                delete tmp;
            } else {
                this->memory_levels[i]->append_merged_runs(this->memory_levels[i-1], rng);
            }
            delete this->memory_levels[i-1];
            this->memory_levels[i-1] = new MemoryLevel(i-1, (LSM_LEVELING) ? 1 : this->scale_factor);
        }

        // NOTE: This is assuming that we will always have memory levels. If
        // not, the DiskLevel interface will need to be expanded to allow
        // appending a memory table as well, with an appropriate branch added
        // here.
        auto new_level = new MemoryLevel(0, (LSM_LEVELING) ? 1 : this->scale_factor);
        new_level->append_mem_table(mtable, rng);
        this->memory_levels[0] = new_level;

        mtable->truncate();
    }

    /*
     * For a given level number, returns true if the level corresponds to
     * a disk level and false if it corresponds to a memory level. Additionally,
     * set level_idx equal to the index of this level in the corresponding
     * backing array (disk_levels or memory_levels), based on the returned value.
     */
    inline bool decode_level_number(size_t level_no, size_t &level_idx) {
        assert(level_no < this->memory_levels.size() + this->disk_levels.size());

        if (level_no > this->memory_levels.size()) {
            level_idx = level_no;
            return false;
        }

        level_idx = level_no - this->memory_levels.size();
        return true;
    }

    // Assume that level "0" should be larger than the memtable
    inline size_t calc_level_rec_cnt(size_t idx) {
        return this->memtable()->get_capacity() * pow(this->scale_factor, idx+1);
    }


    inline bool memlevel_can_merge_with(size_t incoming_rec_cnt, size_t idx) {
        if (! this->memory_levels[idx]) {
            return true;
        }

        if (LSM_LEVELING) {
            if (this->memory_levels[idx]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_rec_cnt(idx)) {
                return true;
            }

            return false;
        } else {
            return this->memory_levels[idx]->get_run_count() < this->scale_factor;
        }
    }


    /*
     * Accepts the index into the disk level vector directly, not the level number,
     * so that translation should be made in advance.
     */
    inline bool disklevel_can_merge_with(size_t incoming_rec_cnt, size_t idx) {
        if (LSM_LEVELING) {
            if (this->disk_levels[idx]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_rec_cnt(idx)) {
                return true;
            }

            return false;
        } else {
            return this->disk_levels[idx]->get_run_count() < this->scale_factor;
        }

    }

};
}
