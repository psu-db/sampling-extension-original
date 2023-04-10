#pragma once

#include <atomic>
#include <numeric>
#include <thread>
#include <mutex>

#include "lsm/MemTable.h"
#include "lsm/MemoryLevel.h"
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

/*
 * LSM Tree configuration global variables
 */

// True for memtable rejection sampling
static constexpr bool LSM_REJ_SAMPLE = true;

// True for leveling, false for tiering
static constexpr bool LSM_LEVELING = true;

static constexpr size_t LSM_MEMTABLE_CNT = 2;


class LSMTree {
private:
    typedef ssize_t level_index;

    struct version_data {
        std::atomic<size_t> active_memtable;
        std::vector<MemTable *> memtables;
        std::vector<memory_level_ptr> mem_levels; 
        std::atomic<size_t> pins;
        level_index last_level_idx;

        version_data() : active_memtable(0), pins(0), last_level_idx(-1) {};

        bool pin() {
            pins.fetch_add(1);
            return true;
        }

        void unpin() {
            // FIXME: This if statement *shouldn't* be necessary, but
            // it currently is. Somehow a version is being unpinned twice (I think),
            // resulting in a -1 pin count holding up merging indefinitely.
            //
            // I haven't been able to track the root cause down as of yet,
            // so this is here as a workaround. Ideally the root problem should
            // be fixed, of course. 
            if (pins.load() > 0) {
                pins.fetch_add(-1);
            }
        }

        static tagged_ptr<version_data> create_copy(tagged_ptr<version_data> version) {
            auto ptr = version->copy();
            return {ptr, version.m_tag + 1};
        }

        MemTable *get_active_memtable() {
            return this->memtables[active_memtable.load()];
        }

        /*
         * Swap the memtable considered "active", i.e. sustaining inserts
         * for this version, and return a pointer to the memtable that was
         * originally active before this function was called.
         */
        MemTable *swap_active_memtable() {
            auto old_memtable = this->get_active_memtable();

            size_t new_idx, old_idx;
            do {
                old_idx = active_memtable.load();
                new_idx = (new_idx + 1) % LSM_MEMTABLE_CNT;
             } while (!this->active_memtable.compare_exchange_strong(old_idx, new_idx));
            
            return old_memtable;
        }
    private:
        version_data(size_t active_memtable, std::vector<MemTable *> &tables, std::vector<memory_level_ptr> &memlvls, level_index last_level_idx)
        : active_memtable(0), memtables(std::move(tables)), mem_levels(std::move(memlvls)), pins(0), last_level_idx(last_level_idx) {}

        version_data *copy() {
            auto active_mtable = active_memtable.load();
            auto tables = memtables;
            auto mlvls = mem_levels;

            return new version_data(active_mtable, tables, mlvls, last_level_idx);
        }
    };

public:
    LSMTree(std::string root_dir, size_t memtable_cap, size_t memtable_bf_sz,
            size_t scale_factor, double max_ts_prop,
            gsl_rng *rng) : m_scale_factor(scale_factor), m_ts_prop(max_ts_prop),
                            m_root_directory(root_dir), 
                            m_memtables(LSM_MEMTABLE_CNT) {
        m_version_num.store(0);
        for (size_t i=0; i < LSM_MEMTABLE_CNT; i++) {
            m_memtables[i] = new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng);
        }
        
        for (size_t i=0; i < LSM_MEMTABLE_CNT + 1; i++) {
            auto initial_version = new version_data();
            initial_version->memtables = m_memtables;
            m_version_data[i].store({initial_version, 0});
        }

        m_secondary_merge_possible.store(false);
        m_primary_merge_active.store(false);
    }

    ~LSMTree() {
        this->await_merge_completion();
        for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
            delete m_memtables[i];
        }

        for (size_t i=0; i<LSM_MEMTABLE_CNT+1; i++) {
            if (!m_version_data[i].load().m_ptr) {
                continue;
            }
            delete m_version_data[i].load().m_ptr;
        }
    }

    int append(key_t key, value_t val, bool tombstone, gsl_rng *rng) {
        size_t version_num;

        do {
            auto version = this->pin_version(&version_num);
            MemTable *mtable = version->get_active_memtable();

            if (mtable->is_full() && !mtable->merging()) {
                if (mtable->start_merge()) {
                    // move to other memtable
                    mtable = m_version_data[version_num].load()->swap_active_memtable();

                    // begin background merge of the full memtable
                    std::thread mthread = std::thread(&LSMTree::merge_memtable, this, mtable, version_num, rng);
                    mthread.detach();

                    // Get the new active memtable in preparation for
                    // potentially appending to it.
                    mtable = version->get_active_memtable();
                } 
            }

            // Attempt to insert
            if (mtable->append(key, val, tombstone)) {
                this->unpin_version(version_num);
                return 1;
            }

            // If insertion fails, we need to wait for the next version to
            // become available.
            this->unpin_version(version_num);
        } while (true);
        //} while (this->wait_for_version(version_num));

        return 0;
    }

    void range_sample(record_t *sample_set, const key_t lower_key, const key_t upper_key, size_t sample_sz, gsl_rng *rng) {
        TIMER_INIT();

        size_t version_num;
        auto version = this->pin_version(&version_num);

        // Allocate buffer into which to write the samples
        size_t sample_idx = 0;

        // Obtain the sampling ranges for each level
        std::vector<SampleRange> memory_ranges;
        std::vector<size_t> record_counts;

        MemTableView *memtable = nullptr;
        while (!(memtable = this->memtable_view()))
            ;

        TIMER_START();

        record_counts.push_back(memtable->get_record_count());

        for (auto &level : version->mem_levels) {
            if (level) {
                level->get_sample_ranges(memory_ranges, record_counts, lower_key, upper_key);
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
            const record_t *sample_record;

            // We will draw the records from the runs in order

            // First the memtable,
            while (run_samples[0] > 0) {
                TIMER_START();
                size_t idx = gsl_rng_uniform_int(rng, memtable->get_record_count() - 1 );
                sample_record = memtable->get_record_at(idx);
                TIMER_STOP();
                memtable_sample_time += TIMER_RESULT();

                run_samples[0]--;

                if (!add_to_sample(sample_record, INVALID_RID, upper_key, lower_key, sample_set, sample_idx, memtable, version)) {
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
                    sample_record = version->mem_levels[run_id.level_idx]->get_record_at(run_id.run_idx, idx + memory_ranges[i].low);
                    run_samples[i+run_offset]--;
                    TIMER_STOP();
                    memlevel_sample_time += TIMER_RESULT();

                    if (!add_to_sample(sample_record, memory_ranges[i].run_id, upper_key, lower_key, sample_set, sample_idx, memtable, version)) {
                        rejections++;
                    }
                }

            }
        } while (sample_idx < sample_sz);

        delete memtable;
        this->unpin_version(version_num);
    }

    // Checks the tree and memtable for a tombstone corresponding to
    // the provided record in any run *above* the rid, which
    // should correspond to the run containing the record in question
    // 
    // Passing INVALID_RID indicates that the record exists within the MemTable
    bool is_deleted(const record_t *record, const RunId &rid, MemTableView *memtable, version_data *version) {
        // check for tombstone in the memtable. This will require accounting for the cutoff eventually.
        if (memtable->check_tombstone(record->key, record->value)) {
            return true;
        }

        // if the record is in the memtable, then we're done.
        if (rid == INVALID_RID) {
            return false;
        }

        for (size_t lvl=0; lvl<rid.level_idx; lvl++) {
            if (version->mem_levels[lvl]->tombstone_check(0, record->key, record->value )) {
                return true;
            }
        }

        return version->mem_levels[rid.level_idx]->tombstone_check(rid.run_idx + 1, record->key, record->value);
    }


    size_t get_record_cnt() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        size_t cnt=0;
        for (size_t i=0; i<version->memtables.size(); i++) {
            cnt += version->memtables[i]->get_record_count();
        }

        for (size_t i=0; i<version->mem_levels.size(); i++) {
            if (version->mem_levels[i]) cnt += version->mem_levels[i]->get_record_cnt();
        }

        this->unpin_version(version_num);

        return cnt;
    }


    size_t get_tombstone_cnt() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        size_t cnt=0;
        for (size_t i=0; i<version->memtables.size(); i++) {
            cnt += version->memtables[i]->get_tombstone_count();
        }

        for (size_t i=0; i<version->mem_levels.size(); i++) {
            if (version->mem_levels[i]) cnt += version->mem_levels[i]->get_tombstone_count();
        }

        this->unpin_version(version_num);
        return cnt;
    }

    size_t get_height() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        auto height =  version->mem_levels.size();
        this->unpin_version(version_num);
        return height;
    }

    size_t get_memory_utilization() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        size_t cnt=0;
        for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
            cnt += m_memtables[i]->get_memory_utilization();
        }

        for (size_t i=0; i<version->mem_levels.size(); i++) {
            if (version->mem_levels[i]) cnt += version->mem_levels[i]->get_memory_utilization();
        }

        this->unpin_version(version_num);
        return cnt;
    }

    size_t get_aux_memory_utilization() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        size_t cnt=0;
        for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
            cnt += m_memtables[i]->get_aux_memory_utilization();
        }

        for (size_t i=0; i<version->mem_levels.size(); i++) {
            if (version->mem_levels[i]) cnt += version->mem_levels[i]->get_aux_memory_utilization();
        }

        this->unpin_version(version_num);
        return cnt;
    }

    bool validate_tombstone_proportion() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        long double ts_prop;
        for (size_t i=0; i<version->mem_levels.size(); i++) {
            if (version->mem_levels[i]) {
                ts_prop = (long double) version->mem_levels[i]->get_tombstone_count() / (long double) this->calc_level_record_capacity(i);
                if (ts_prop > (long double) m_ts_prop) {
                    this->unpin_version(version_num);
                    return false;
                }
            }
        }

        this->unpin_version(version_num);
        return true;
    }

    void await_merge_completion() {
        bool done = false;
        while (!done) {
            done = true;
            for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
                if (m_memtables[i]->merging()) {
                    done = false;
                }
            }
        }
    }

private:
    std::atomic<size_t> m_version_num;
    std::atomic<tagged_ptr<version_data>> m_version_data[LSM_MEMTABLE_CNT + 1];

    std::vector<MemTable *> m_memtables;

    std::mutex m_merge_lock;
    std::atomic<bool> m_secondary_merge_possible;
    std::atomic<bool> m_primary_merge_active;

    size_t m_scale_factor;
    double m_ts_prop;

    // The directory containing all of the backing files
    // for this LSM Tree.
    std::string m_root_directory;

    version_data *pin_version(size_t *version_num=nullptr) {
        // FIXME: ensure that the version pinned is the same as
        // the one retrieved. 
        do {
            size_t vnum = m_version_num.load();
            version_data *version = m_version_data[vnum].load().m_ptr;
            if (version->pin()) {
                if (version_num) {
                    *version_num = vnum;
                }

                return version;
            }
        } while (true);

        assert(true); // not reached
    }

    void unpin_version(size_t version_num) {
        m_version_data[version_num].load()->unpin();
    }

    MemTableView *memtable_view() {
        return MemTableView::create(m_memtables);
    }

    inline bool rejection(const record_t *record, RunId rid, const key_t lower_bound, const key_t upper_bound, MemTableView *memtable, version_data *version) {
        if (record->is_tombstone()) {
            tombstone_rejections++;
            return true;
        } else if (record->key < lower_bound || record->key > upper_bound) {
            bounds_rejections++;
            return true;
        } else if (this->is_deleted(record, rid, memtable, version)) {
            deletion_rejections++;
            return true;
        }

        return false;
    }

    inline size_t rid_to_disk(RunId rid, size_t version_num) {
        return rid.level_idx - m_version_data[version_num].load()->mem_levels.size();
    }

    inline bool add_to_sample(const record_t *record, RunId rid, const key_t upper_key, const key_t lower_key, 
                              record_t *sample_buffer, size_t &sample_idx, MemTableView *memtable, version_data *version) {
        TIMER_INIT();
        TIMER_START();
        sampling_attempts++;
        if (rejection(record, rid, lower_key, upper_key, memtable, version)) {
            sampling_rejections++;
            return false;
        }
        TIMER_STOP();
        rejection_check_time += TIMER_RESULT();

        sample_buffer[sample_idx++] = *record;
        return true;
    }

    /*
     * Add a new level to the LSM Tree and return that level's index. Will
     * automatically determine whether the level should be on memory or on disk,
     * and act appropriately.
     */
    inline level_index grow(version_data *version) {
        level_index new_idx;

        size_t new_run_cnt = (LSM_LEVELING) ? 1 : m_scale_factor;
        new_idx = version->mem_levels.size();
        if (new_idx > 0) {
            assert(version->mem_levels[new_idx - 1]->get_run(0)->get_tombstone_count() == 0);
        }
        version->mem_levels.emplace_back(new MemoryLevel(new_idx, new_run_cnt));

        version->last_level_idx++;
        return new_idx;
    }

    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    inline void merge_memtable(MemTable *mtable, size_t version_num, gsl_rng *rng) {
        //#define MERGE_LOGGING
        #ifdef MERGE_LOGGING
        fprintf(stderr, "Starting merge for %ld\n", version_num);
        fprintf(stderr, "\tMemtable Version: %ld\n", m_version_data[version_num].load()->active_memtable % LSM_MEMTABLE_CNT);
        #endif
        if (m_primary_merge_active) {
            version_num = (version_num + 1) % LSM_MEMTABLE_CNT;
            
            #ifdef MERGE_LOGGING
            fprintf(stderr, "\tUpdating version number to: %ld\n", version_num);
            #endif
        }

        #ifdef MERGE_LOGGING
        fprintf(stderr, "\tPrimary Merge: %d\n", m_primary_merge_active.load());
        fprintf(stderr, "\tSecondary Merge Possible: %d\n", m_secondary_merge_possible.load());
        fprintf(stderr, "\tCurrent record count: %ld\n", this->get_record_cnt());
        #endif
        

        memory_level_ptr new_l0 = nullptr;
        if (m_primary_merge_active.load() && m_secondary_merge_possible.load()) {
            #ifdef MERGE_LOGGING
            fprintf(stderr, "Starting secondary merge...\n");
            #endif
            m_secondary_merge_possible.store(false); // start the secondary merge
            new_l0 = this->create_new_l0(mtable, rng);
            #ifdef MERGE_LOGGING
            fprintf(stderr, "Awaiting primary merge completion...\n");
            #endif
        }

        m_merge_lock.lock();
        #ifdef MERGE_LOGGING
        fprintf(stderr, "Passed merge lock for %ld\n", version_num);

        if (new_l0) {
            fprintf(stderr, "Secondary merge continuation\n");
        }
        #endif

        m_primary_merge_active.store(true);
        auto new_version = version_data::create_copy(m_version_data[version_num]);

        // If this merge will leave the first level completely full, then a
        // secondary merge is possible, as we can initiate this process without
        // needing to know the state of memlevel[0].
        m_secondary_merge_possible.store(new_version.get()->mem_levels.size() > 0 && 
                                         new_version.get()->mem_levels[0]->merge_will_fill(mtable->get_record_count()));

        if (!this->can_merge_with(0, mtable->get_record_count(), new_version.get())) {
            this->merge_down(0, new_version.get(), rng);
        }

        this->merge_memtable_into_l0(mtable, new_version.get(), rng);
        this->enforce_tombstone_maximum(0, new_version.get(), rng);

        // TEMP: install the new version 
        size_t new_version_no = (version_num + 1) % LSM_MEMTABLE_CNT;
        m_version_data[new_version_no].load();

        // This should be sufficient, as once a version is no longer
        // active it will stop acculumating pins. So there isn't a risk
        // of this counter changing between when we observe it to be 0
        // and when we swap in the new version.

        while (m_version_data[new_version_no].load()->pins > 0) {
            ;
        }

        // Swap in the new version.
        auto old_version = m_version_data[new_version_no].load();
        m_version_data[new_version_no].store(new_version);

        // We need to truncate the memtable before we advance
        // the version counter, otherwise there is a moment where 
        // threads may see duplicate records.
        bool truncation_status = false;
        mtable->truncate(&truncation_status);

        while (!truncation_status) 
            ;

        new_version.m_ptr->active_memtable.store(m_version_data[m_version_num.load()].load().m_ptr->active_memtable.load());

        // Update the version counter
        m_version_num.store(new_version_no);

        delete old_version.m_ptr;

        m_primary_merge_active.store(false);
        m_merge_lock.unlock();
        #ifdef MERGE_LOGGING
        fprintf(stderr, "merge done for %ld\n", version_num);
        #endif
        return;
    }

    /*
     * Merge the specified level down into the tree. The level index must be
     * non-negative (i.e., this function cannot be used to merge the memtable). This
     * routine will recursively perform any necessary merges to make room for the 
     * specified level.
     */
    inline void merge_down(level_index idx, version_data *version, gsl_rng *rng) {
        level_index merge_base_level = this->find_mergable_level(idx, version);
        if (merge_base_level == -1) {
            merge_base_level = this->grow(version);
        }

        for (level_index i=merge_base_level; i>idx; i--) {
            this->merge_levels(i, i-1, version, rng);
            this->enforce_tombstone_maximum(i, version, rng);
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
    inline level_index find_mergable_level(level_index idx, version_data *version, MemTable *mtable=nullptr) {

        if (idx == 0 && version->mem_levels.size() == 0) return -1;

        bool level_found = false;
        bool disk_level;
        level_index merge_level_idx;

        size_t incoming_rec_cnt = this->get_level_record_count(idx, version);
        for (level_index i=idx+1; i<=version->last_level_idx; i++) {
            if (this->can_merge_with(i, incoming_rec_cnt, version)) {
                return i;
            }

            incoming_rec_cnt = this->get_level_record_count(i, version);
        }

        return -1;
    }

    /*
     * Merge the level specified by incoming level into the level specified
     * by base level. The two levels should be sequential--i.e. no levels
     * are skipped in the merge process--otherwise the tombstone ordering
     * invariant may be violated by the merge operation.
     */
    inline void merge_levels(level_index base_level, level_index incoming_level, version_data *version, gsl_rng *rng) {

        if (LSM_LEVELING) {
            version->mem_levels[base_level].reset(MemoryLevel::merge_levels(version->mem_levels[base_level].get(), version->mem_levels[incoming_level].get(), rng));
        } else {
            version->mem_levels[base_level]->append_merged_runs(version->mem_levels[incoming_level].get(), rng);
        }

        version->mem_levels[incoming_level].reset(new MemoryLevel(incoming_level, (LSM_LEVELING) ? 1 : m_scale_factor));
    }

    inline void merge_memtable_into_l0(MemTable *mtable, version_data *version, gsl_rng *rng) {
        assert(version->mem_levels[0]);
        if (LSM_LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto temp_level = new MemoryLevel(0, 1);
            temp_level->append_mem_table(mtable, rng);
            version->mem_levels[0].reset(MemoryLevel::merge_levels(version->mem_levels[0].get(), temp_level, rng));
            delete temp_level;
        } else {
            version->mem_levels[0]->append_mem_table(mtable, rng);
        }
    }


    memory_level_ptr create_new_l0(MemTable *mtable, gsl_rng *rng) {
        size_t run_capacity = (LSM_LEVELING) ? 1 : m_scale_factor;

        auto new_level = new MemoryLevel(0, run_capacity);
        new_level->append_mem_table(mtable, rng);

        return std::shared_ptr<MemoryLevel>(new_level);
    }

    /*
     * Mark a given memory level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(MemoryLevel *level) {
        level->mark_unused();
    }

    /*
     * Check the tombstone proportion for the specified level and
     * if the limit is exceeded, forcibly merge levels until all
     * levels below idx are below the limit.
     */
    inline void enforce_tombstone_maximum(level_index idx, version_data *version, gsl_rng *rng) {
        long double ts_prop = (long double) version->mem_levels[idx]->get_tombstone_count() / (long double) this->calc_level_record_capacity(idx);

        if (ts_prop > (long double) m_ts_prop) {
            this->merge_down(idx, version, rng);
        }

        return;
    }

    /*
     * Assume that level "0" should be larger than the memtable. The memtable
     * itself is index -1, which should return simply the memtable capacity.
     */
    inline size_t calc_level_record_capacity(level_index idx) {
        return m_memtables[0]->get_capacity() * pow(m_scale_factor, idx+1);
    }

    /*
     * Returns the actual number of records present on a specified level. An
     * index value of -1 indicates the memory table. Can optionally pass in
     * a pointer to the memory table to use, if desired. Otherwise, the sum
     * of records in the two memory tables will be returned.
     */
    inline size_t get_level_record_count(level_index idx, version_data *version) {
        assert(idx >= -1);

        if (idx == -1) {
            return version->get_active_memtable()->get_record_count();
        }

        return (version->mem_levels[idx]) ? version->mem_levels[idx]->get_record_cnt() : 0;
    }

    /*
     * Determines if the specific level can merge with another record containing
     * incoming_rec_cnt number of records. The provided level index should be 
     * non-negative (i.e., not refer to the memtable) 
     */
    inline bool can_merge_with(level_index idx, size_t incoming_rec_cnt, version_data *version) {
        if (idx >= version->mem_levels.size() || !version->mem_levels[idx]) {
            return false;
        }

        if (LSM_LEVELING) {
            return version->mem_levels[idx]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_record_capacity(idx);
        } else {
            return version->mem_levels[idx]->get_run_count() < m_scale_factor;
        }

        // unreachable
        assert(true);
    }

    bool wait_for_version(size_t version_num) {
        // TODO: replace with condition variable
        while (m_version_num.load() == version_num) 
            ;

        return true;

    }
};
}
