#pragma once

#include <atomic>
#include <numeric>

#include "lsm/IsamTree.h"
#include "lsm/MemTable.h"
#include "lsm/MemoryLevel.h"
#include "lsm/DiskLevel.h"
#include "ds/Alias.h"

#include "util/timer.h"

namespace lsm {

thread_local size_t sampling_attempts = 0;
thread_local size_t sampling_rejections = 0;
thread_local size_t deletion_rejections = 0;
thread_local size_t bounds_rejections = 0;
thread_local size_t tombstone_rejections = 0;

/*
 * thread_local size_t various_sampling_times go here.
 */
thread_local size_t sample_range_time = 0;
thread_local size_t alias_time = 0;
thread_local size_t alias_query_time = 0;
thread_local size_t rejection_check_time = 0;
thread_local size_t memtable_sample_time = 0;
thread_local size_t memlevel_sample_time = 0;
thread_local size_t disklevel_sample_time = 0;


/*
 * LSM Tree configuration global variables
 */

// True for memtable rejection sampling
static constexpr bool LSM_REJ_SAMPLE = true;

// True for leveling, false for tiering
static constexpr bool LSM_LEVELING = false;

typedef ssize_t level_index;

class LSMTree {
public:
    LSMTree(std::string root_dir, size_t memtable_cap, size_t memtable_bf_sz, size_t scale_factor, size_t memory_levels,
            double max_tombstone_prop, gsl_rng *rng) 
        : active_memtable(0), //memory_levels(memory_levels, 0),
          scale_factor(scale_factor), 
          max_tombstone_prop(max_tombstone_prop),
          root_directory(root_dir),
          last_level_idx(-1),
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
        TIMER_INIT();

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

        TIMER_START();

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

        TIMER_STOP();
        sample_range_time += TIMER_RESULT();

        TIMER_START();
        size_t total_records = std::accumulate(record_counts.begin(), record_counts.end(), 0);

        std::vector<double> weights(record_counts.size());
        for (size_t i=0; i < record_counts.size(); i++) {
            weights[i] = (double) record_counts[i] / (double) total_records;
        }

        auto alias = Alias(weights);

        TIMER_STOP();
        alias_time += TIMER_RESULT();

        // For implementation convenience, we'll treat the very
        // first sampling pass as though it were a sampling
        // pass following one in which every single sample
        // was rejected
        size_t rejections = sample_sz;
        sampling_attempts = 0;
        sampling_rejections = 0;
        tombstone_rejections = 0;
        bounds_rejections = 0;
        deletion_rejections = 0;

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
            TIMER_START();
            for (size_t i=0; i<rejections; i++) {
                run_samples[alias.get(rng)] += 1;
            }
            TIMER_STOP();
            alias_query_time += TIMER_RESULT();

            // reset rejection counter to begin tracking for next
            // sampling pass
            rejections = 0;
            const char *sample_record;

            // We will draw the records from the runs in order

            // First the memtable,
            while (run_samples[0] > 0) {
                TIMER_START();
                size_t idx = gsl_rng_uniform_int(rng, memtable_cutoff);
                sample_record = memtable->get_record_at(idx);
                TIMER_STOP();
                memtable_sample_time += TIMER_RESULT();

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
                    TIMER_START();
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = memory_levels[run_id.level_idx]->get_record_at(run_id.run_idx, idx + memory_ranges[i].low);
                    run_samples[i+run_offset]--;
                    TIMER_STOP();
                    memlevel_sample_time += TIMER_RESULT();

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
                    TIMER_START();
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = this->disk_levels[level_idx]->get_run(run_idx)->sample_record(disk_ranges[i].low, idx, buffer, buffered_page);
                    run_samples[i+run_offset]--;
                    TIMER_STOP();
                    disklevel_sample_time += TIMER_RESULT();

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


    size_t get_tombstone_cnt() {
        // FIXME: need to account for both memtables with concurrency
        size_t cnt = this->memtable()->get_tombstone_count();

        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (this->memory_levels[i]) cnt += this->memory_levels[i]->get_tombstone_count();
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            if (this->disk_levels[i]) cnt += this->disk_levels[i]->get_tombstone_count();
        }

        return cnt;
    }

    size_t get_height() {
        return this->memory_levels.size() + this->disk_levels.size();
    }

    size_t get_memory_utilization() {
        size_t cnt = this->memtable_1->get_memory_utilization() + this->memtable_2->get_memory_utilization();

        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (this->memory_levels[i]) cnt += this->memory_levels[i]->get_memory_utilization();
        }

        return cnt;
    }

    size_t get_aux_memory_utilization() {
        size_t cnt = this->memtable_1->get_aux_memory_utilization() + this->memtable_2->get_aux_memory_utilization();

        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (this->memory_levels[i]) cnt += this->memory_levels[i]->get_aux_memory_utilization();
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            if (this->disk_levels[i]) cnt += this->disk_levels[i]->get_aux_memory_utilization();
        }

        return cnt;
    }

    /*
     * Flattens the entire LSM structure into a single in-memory sorted
     * array and return a pointer to it. Will be used as a simple baseline
     * for performance comparisons.
     *
     * len will be updated to hold the number of records within the tree.
     *
     * It is the caller's responsibility to manage the memory of the returned
     * object. It should be released with free().
     *
     * NOTE: If an interface to create InMemRun objects using ISAM Trees were
     * added, this could be implemented just like get_flat_isam_tree and return
     * one of those. But as it stands, this seems the most straightforward way
     * to get a static in-memory structure.
     */
    char *get_sorted_array(size_t *len, gsl_rng *rng)
    {
        // flatten into an ISAM Tree to get a contiguous run of all
        // the records, and cancel out all tombstones.
        auto tree = this->get_flat_isam_tree(rng);
        size_t alloc_sz = tree->get_record_count() * record_size + (CACHELINE_SIZE - tree->get_record_count() * record_size % CACHELINE_SIZE);
        char *array = (char *) std::aligned_alloc(CACHELINE_SIZE, alloc_sz);

        assert(tree->get_tombstone_count() == 0);

        *len = tree->get_record_count();

        auto iter = tree->start_scan();
        size_t offset = 0;
        while (iter->next() && offset < tree->get_record_count()) {
            auto pg = iter->get_item();
            for (size_t i=0; i<PAGE_SIZE/record_size; i++) {
                memcpy(array + offset*record_size, pg + i*record_size, record_size);
                offset++;

                if (offset >= tree->get_record_count()) break;
            }
        }

        auto pfile = tree->get_pfile();
        delete iter;
        delete tree;
        delete pfile;

        return array;
    }

    /*
     * Flattens the entire LSM structure into a single ISAM Tree object
     * and return a pointer to it. Will be used as a simple baseline for
     * performance comparisons.
     */
    ISAMTree *get_flat_isam_tree(gsl_rng *rng) {
        auto mem_level = new MemoryLevel(-1, 1, this->root_directory);
        mem_level->append_mem_table(this->memtable(), rng);

        std::vector<InMemRun *> runs;
        std::vector<ISAMTree *> trees;

        for (int i=memory_levels.size() - 1; i>= 0; i--) {
            if (memory_levels[i]) {
                for (int j=0; j<memory_levels[i]->get_run_count(); j++) {
                    runs.push_back(memory_levels[i]->get_run(j));
                }
            }
        }

        runs.push_back(mem_level->get_run(0));

        for (int i=disk_levels.size() - 1; i >= 0; i--) {
            if (disk_levels[i]) {
                for (int j=0; j<disk_levels[i]->get_run_count(); j++) {
                    trees.push_back(disk_levels[i]->get_run(j));
                }
            }
        }

        auto pfile = PagedFile::create(root_directory + "flattened_tree.dat");
        auto bf = new BloomFilter(0, BF_HASH_FUNCS, rng);

        auto flat = new ISAMTree(pfile, rng, bf, runs.data(), runs.size(), trees.data(), trees.size());

        delete mem_level;
        delete bf;

        return flat;
    }


    bool validate_tombstone_proportion() {
        long double ts_prop;
        for (size_t i=0; i<this->memory_levels.size(); i++) {
            if (this->memory_levels[i]) {
                ts_prop = (long double) this->memory_levels[i]->get_tombstone_count() / (long double) this->calc_level_record_capacity(i);
                if (ts_prop > (long double) this->max_tombstone_prop) {
                    return false;
                }
            }
        }

        for (size_t i=0; i<this->disk_levels.size(); i++) {
            if (this->disk_levels[i]) {
                ts_prop = (long double) this->disk_levels[i]->get_tombstone_count() / (long double) this->calc_level_record_capacity(this->memory_levels.size() + i);
                if (ts_prop > (long double) this->max_tombstone_prop) {
                    return false;
                }
            }
        }

        return true;
    }

    void persist_tree(gsl_rng *rng) {
        std::string meta_dir = this->root_directory + "/meta";
        mkdir(meta_dir.c_str(), 0755);

        std::string meta_fname = meta_dir + "/lsmtree.dat";
        FILE *meta_f = fopen(meta_fname.c_str(), "w");
        assert(meta_f);

        // merge the memtable down to ensure it is persisted
        this->merge_memtable(rng);
        
        // persist each level of the tree
        for (size_t i=0; i<this->get_height(); i++) {
            bool disk = false;

            auto level_idx = this->decode_level_index(i, &disk);
            std::string level_meta = meta_dir + "/level-" + std::to_string(i) +"-meta.dat";
            fprintf(meta_f, "%s\n", level_meta.c_str());

            if (disk) {
                disk_levels[level_idx]->persist_level(level_meta);
            } else {
                memory_levels[level_idx]->persist_level(level_meta);
            }
        }

        fclose(meta_f);
    }

private:
    MemTable *memtable_1;
    MemTable *memtable_2;
    std::atomic<bool> active_memtable;
    std::atomic<bool> memtable_1_merging;
    std::atomic<bool> memtable_2_merging;

    size_t scale_factor;
    double max_tombstone_prop;

    std::vector<MemoryLevel *> memory_levels;
    size_t memory_level_cnt;
    std::vector<DiskLevel *> disk_levels;

    level_index last_level_idx;

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
        if (is_tombstone(record)) {
            tombstone_rejections++;
            return true;
        } else if (key_cmp(get_key(record), lower_bound) < 0 || key_cmp(get_key(record), upper_bound) > 0) {
            bounds_rejections++;
            return true;
        } else if (this->is_deleted(record, rid, buffer, memtable, memtable_cutoff)) {
            deletion_rejections++;
            return true;
        }

        return false;
    }

    inline size_t rid_to_disk(RunId rid) {
        return rid.level_idx - this->memory_levels.size();
    }

    inline bool add_to_sample(const char *record, RunId rid, const char *upper_key, const char *lower_key, char *io_buffer,
                              char *sample_buffer, size_t &sample_idx, MemTable *memtable, size_t memtable_cutoff) {
        TIMER_INIT();
        TIMER_START();
        sampling_attempts++;
        if (!record || rejection(record, rid, lower_key, upper_key, io_buffer, memtable, memtable_cutoff)) {
            sampling_rejections++;
            return false;
        }
        TIMER_STOP();
        rejection_check_time += TIMER_RESULT();


        memcpy(sample_buffer + (sample_idx++ * record_size), record, record_size);
        return true;
    }

    /*
     * Add a new level to the LSM Tree and return that level's index. Will
     * automatically determine whether the level should be on memory or on disk,
     * and act appropriately.
     */
    inline level_index grow() {
        level_index new_idx;

        size_t new_run_cnt = (LSM_LEVELING) ? 1 : this->scale_factor;
        if (this->memory_levels.size() < this->memory_level_cnt) {
            new_idx = this->memory_levels.size();
            if (new_idx > 0) {
                assert(this->memory_levels[new_idx - 1]->get_run(0)->get_tombstone_count() == 0);
            }
            this->memory_levels.emplace_back(new MemoryLevel(new_idx, new_run_cnt, this->root_directory));
        } else {
            new_idx = this->disk_levels.size() + this->memory_levels.size();
            if (this->disk_levels.size() > 0) {
                assert(this->disk_levels[this->disk_levels.size() - 1]->get_run(0)->get_tombstone_count() == 0);
            }
            this->disk_levels.emplace_back(new DiskLevel(new_idx, new_run_cnt, this->root_directory));
        } 

        this->last_level_idx++;
        return new_idx;
    }


    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    inline void merge_memtable(gsl_rng *rng) {
        auto mtable = this->memtable();

        if (!this->can_merge_with(0, mtable->get_record_count())) {
            this->merge_down(0, rng);
        }

        this->merge_memtable_into_l0(mtable, rng);
        this->enforce_tombstone_maximum(0, rng);

        mtable->truncate();
        return;
    }

    /*
     * Merge the specified level down into the tree. The level index must be
     * non-negative (i.e., this function cannot be used to merge the memtable). This
     * routine will recursively perform any necessary merges to make room for the 
     * specified level.
     */
    inline void merge_down(level_index idx, gsl_rng *rng) {
        level_index merge_base_level = this->find_mergable_level(idx);
        if (merge_base_level == -1) {
            merge_base_level = this->grow();
        }

        for (level_index i=merge_base_level; i>idx; i--) {
            this->merge_levels(i, i-1, rng);
            this->enforce_tombstone_maximum(i, rng);
        }

        return;
    }

    /*
     * Find the first level below the level indicated by idx that
     * is capable of sustaining a merge operation and return its
     * level index. If no such level exists, returns -1. Also
     * returns -1 if idx==0, and no such level exists, to simplify
     * the logic of the first merge.
     */
    inline level_index find_mergable_level(level_index idx, MemTable *mtable=nullptr) {

        if (idx == 0 && this->memory_levels.size() == 0) return -1;

        bool level_found = false;
        bool disk_level;
        level_index merge_level_idx;

        size_t incoming_rec_cnt = this->get_level_record_count(idx, mtable);
        for (level_index i=idx+1; i<=this->last_level_idx; i++) {
            if (this->can_merge_with(i, incoming_rec_cnt)) {
                return i;
            }

            incoming_rec_cnt = this->get_level_record_count(i);
        }

        return -1;
    }

    /*
     * Merge the level specified by incoming level into the level specified
     * by base level. The two levels should be sequential--i.e. no levels
     * are skipped in the merge process--otherwise the tombstone ordering
     * invariant may be violated by the merge operation.
     */
    inline void merge_levels(level_index base_level, level_index incoming_level, gsl_rng *rng) {
        bool base_disk_level;
        bool incoming_disk_level;

        size_t base_idx = decode_level_index(base_level, &base_disk_level);
        size_t incoming_idx = decode_level_index(incoming_level, &incoming_disk_level);

        // If the base level is a memory level, then the incoming level
        // cannot be a disk level.
        assert(!(!base_disk_level && incoming_disk_level));

        if (base_disk_level && incoming_disk_level) {
            // Merging two disk levels
            if (LSM_LEVELING) {
                auto tmp = this->disk_levels[base_idx];
                this->disk_levels[base_idx] = DiskLevel::merge_levels(this->disk_levels[base_idx], this->disk_levels[incoming_idx], rng);
                this->mark_as_unused(tmp);
            } else {
                this->disk_levels[base_idx]->append_merged_runs(this->disk_levels[incoming_idx], rng);
            }
            this->mark_as_unused(this->disk_levels[incoming_idx]);
            this->disk_levels[incoming_idx] = new DiskLevel(incoming_level, (LSM_LEVELING) ? 1 : this->scale_factor, this->root_directory);
        } else if (base_disk_level) {
            // Merging the last memory level into the first disk level
            assert(base_idx == 0);
            assert(incoming_idx == this->memory_level_cnt - 1);
            if (LSM_LEVELING) {
                auto tmp = this->disk_levels[base_idx];
                this->disk_levels[base_idx] = DiskLevel::merge_levels(this->disk_levels[base_idx], this->memory_levels[incoming_idx], rng);
                this->mark_as_unused(tmp);
            } else {
                this->disk_levels[base_idx]->append_merged_runs(this->memory_levels[incoming_idx], rng);
            }

            this->mark_as_unused(this->memory_levels[incoming_idx]);
            this->memory_levels[incoming_idx] = new MemoryLevel(incoming_level, (LSM_LEVELING) ? 1 : this->scale_factor, this->root_directory);
        } else {
            // merging two memory levels
            if (LSM_LEVELING) {
                auto tmp = this->memory_levels[base_idx];
                this->memory_levels[base_idx] = MemoryLevel::merge_levels(this->memory_levels[base_idx], this->memory_levels[incoming_idx], rng);
                this->mark_as_unused(tmp);
            } else {
                this->memory_levels[base_idx]->append_merged_runs(this->memory_levels[incoming_idx], rng);
            }

            this->mark_as_unused(this->memory_levels[incoming_idx]);
            this->memory_levels[incoming_idx] = new MemoryLevel(incoming_level, (LSM_LEVELING) ? 1 : this->scale_factor, this->root_directory);
        }
    }

    inline void merge_memtable_into_l0(MemTable *mtable, gsl_rng *rng) {
        assert(this->memory_levels[0]);
        if (LSM_LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto old_level = this->memory_levels[0];
            auto temp_level = new MemoryLevel(0, 1, this->root_directory);
            temp_level->append_mem_table(mtable, rng);
            auto new_level = MemoryLevel::merge_levels(old_level, temp_level, rng);

            this->memory_levels[0] = new_level;
            delete temp_level;
            this->mark_as_unused(old_level);
        } else {
            this->memory_levels[0]->append_mem_table(mtable, rng);
        }
    }

    /*
     * Mark a given disk level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(DiskLevel *level) {
        delete level;
    }

    /*
     * Mark a given memory level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(MemoryLevel *level) {
        delete level;
    }

    /*
     * Check the tombstone proportion for the specified level and
     * if the limit is exceeded, forcibly merge levels until all
     * levels below idx are below the limit.
     */
    inline void enforce_tombstone_maximum(level_index idx, gsl_rng *rng) {
        bool disk_level;
        size_t level_idx = this->decode_level_index(idx, &disk_level);

        long double ts_prop = (disk_level) ? (long double) this->disk_levels[level_idx]->get_tombstone_count() / (long double) this->calc_level_record_capacity(idx)
                                           : (long double) this->memory_levels[level_idx]->get_tombstone_count() / (long double) this->calc_level_record_capacity(idx);

        if (ts_prop > (long double) this->max_tombstone_prop) {
            this->merge_down(idx, rng);
        }

        return;
    }

    /*
     * Assume that level "0" should be larger than the memtable. The memtable
     * itself is index -1, which should return simply the memtable capacity.
     */
    inline size_t calc_level_record_capacity(level_index idx) {
        return this->memtable()->get_capacity() * pow(this->scale_factor, idx+1);
    }

    /*
     * Returns the actual number of records present on a specified level. An
     * index value of -1 indicates the memory table. Can optionally pass in
     * a pointer to the memory table to use, if desired. Otherwise, there are
     * no guarantees about which memtable will be accessed if level_index is -1.
     */
    inline size_t get_level_record_count(level_index idx, MemTable *mtable=nullptr) {
        assert(idx >= -1);
        if (idx == -1) {
            return (mtable) ? mtable->get_record_count() : memtable()->get_record_count();
        }

        bool disk_level;
        size_t vector_index = decode_level_index(idx, &disk_level);
        if (disk_level) {
            return (disk_levels[vector_index]) ? disk_levels[vector_index]->get_record_cnt() : 0;
        } 

        return (memory_levels[vector_index]) ? memory_levels[vector_index]->get_record_cnt() : 0;
    }

    /*
     * Determines if the specific level can merge with another record containing
     * incoming_rec_cnt number of records. The provided level index should be 
     * non-negative (i.e., not refer to the memtable) and will be automatically
     * translated into the appropriate index into either the disk or memory level
     * vector.
     */
    inline bool can_merge_with(level_index idx, size_t incoming_rec_cnt) {
        bool disk_level;
        ssize_t vector_index = decode_level_index(idx, &disk_level);
        assert(vector_index >= 0);

        if (disk_level) {
            if (LSM_LEVELING) {
                return this->disk_levels[vector_index]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_record_capacity(idx);
            } else {
                return this->disk_levels[vector_index]->get_run_count() < this->scale_factor;
            }
        } 

        if (vector_index >= this->memory_levels.size() || !this->memory_levels[vector_index]) {
            return false;
        }

        if (LSM_LEVELING) {
            return this->memory_levels[vector_index]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_record_capacity(idx);
        } else {
            return this->memory_levels[vector_index]->get_run_count() < this->scale_factor;
        }

        // unreachable
        assert(true);
    }

    /*
     * Converts a level_index into the appropriate integer index into 
     * either the memory level or disk level vector. If the index is
     * for a memory level, set disk_level to false. If it is for a 
     * disk level, set disk_level to true. If the index is less than
     * zero, returns -1 (may indicate either an invalid index, or the
     * memtable).
     */
    inline ssize_t decode_level_index(level_index idx, bool *disk_level) {
        *disk_level = false;

        if (idx < 0) return -1;

        if (idx < this->memory_level_cnt) return idx;

        *disk_level = true;
        return idx - this->memory_level_cnt;
    }


};
}
