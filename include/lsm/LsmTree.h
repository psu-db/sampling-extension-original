#pragma once

#include <atomic>
#include <numeric>
#include <thread>
#include <mutex>

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

static constexpr size_t LSM_MEMTABLE_CNT = 2;


class LSMTree {
private:
    typedef ssize_t level_index;

    struct version_data {
        std::atomic_size_t active_memtable;
        std::vector<MemTable *> memtables;
        std::vector<memory_level_ptr> mem_levels; 
        std::vector<disk_level_ptr> disk_levels; 
        std::atomic_size_t pins;
        level_index last_level_idx;

        version_data() : active_memtable(0), pins(0), last_level_idx(-1) {};

        static tagged_ptr<version_data> create_copy(tagged_ptr<version_data> version) {
            auto ptr = version->copy();
            return {ptr, version.m_tag + 1};
        }

        MemTable *get_active_memtable() {
            return this->memtables[active_memtable];
        }

        //FIXME: Do this atomically
        MemTable *swap_active_memtable(MemTable *current_table) {
            if (active_memtable.load() == 0) {
                active_memtable.store(1);
            }  else {
                active_memtable.store(0);
            }

            return this->memtables[active_memtable];
        }
    private:
        version_data(std::vector<MemTable *> &tables, std::vector<memory_level_ptr> &memlvls, std::vector<disk_level_ptr> &disklvls, level_index last_level_idx)
        : memtables(std::move(tables)), mem_levels(std::move(memlvls)), disk_levels(std::move(disklvls)), pins(0), last_level_idx(last_level_idx) {}

        version_data *copy() {
            auto tables = memtables;
            auto mlvls = mem_levels;
            auto dlvls = disk_levels;

            return new version_data(tables, mlvls, dlvls, last_level_idx);
        }
    };

    struct version_number { 
        std::atomic_size_t num;
        size_t max;
        void init() {
            num.store(0);
            max = LSM_MEMTABLE_CNT;
        }

        void incr_version() {
            size_t tmp;
            size_t new_v;
            do {
                tmp = num.load();
                new_v = (tmp == max) ? 0 : num + 1;
            } while (!num.compare_exchange_strong(tmp, new_v));
        }

        bool incr_version(size_t tmp) {
            size_t new_v = (tmp == max) ? 0 : num + 1;
            return num.compare_exchange_strong(tmp, new_v);
        }
    };

public:
    LSMTree(std::string root_dir, size_t memtable_cap, size_t memtable_bf_sz,
            size_t scale_factor, size_t memory_level_cap, double max_ts_prop,
            gsl_rng *rng) : m_scale_factor(scale_factor), m_ts_prop(max_ts_prop),
                            m_root_directory(root_dir), 
                            m_memtables(LSM_MEMTABLE_CNT),
                            m_memory_level_cnt(memory_level_cap) {
        m_version.init();
        for (size_t i = 0; i < LSM_MEMTABLE_CNT; i++) {
            m_memtables[i] = new MemTable(memtable_cap, LSM_REJ_SAMPLE, memtable_bf_sz, rng);
            m_memtable_merging[i].store(false);
            m_version_data[i].store({nullptr, 0});
        }
        m_version_data[LSM_MEMTABLE_CNT].store({nullptr, 0});

        auto initial_version = new version_data();
        initial_version->memtables = m_memtables;

        m_version_data[0].store({initial_version, 0});
    }

    ~LSMTree() {
        this->await_merge_completion();
        for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
            delete m_memtables[i];
        }

        /*
        for (size_t i=0; i<LSM_MEMTABLE_CNT+1; i++) {
            if (!m_version_data[i].load().m_ptr) {
                continue;
            }
            for (size_t j=0; j<m_version_data[i].load()->disk_levels.size(); j++) {
                m_version_data[i].load()->disk_levels[j].reset();
            }
        }

        for (size_t i=0; i<LSM_MEMTABLE_CNT+1; i++) {
            if (!m_version_data[i].load().m_ptr) {
                continue;
            }
            for (size_t j=0; j<m_version_data[i].load()->memtables.size(); j++) {
                m_version_data[i].load()->mem_levels[j].reset();
            }
        }
        */

        for (size_t i=0; i<LSM_MEMTABLE_CNT+1; i++) {
            if (!m_version_data[i].load().m_ptr) {
                continue;
            }
            delete m_version_data[i].load().m_ptr;
        }
    }

    int append(const char *key, const char *val, bool tombstone, gsl_rng *rng) {
        size_t version_num;

        do {
            auto version = this->pin_version(&version_num);
            MemTable *mtable = version->get_active_memtable();

            if (mtable->is_full()) {
                if (mtable->start_merge()) {
                    MemTable *merge_memtable = mtable;
                    m_memtable_merging[version_num].store(true);
                    // take the pin for the merge process itself. The merge routine
                    // will unpin this.
                    size_t vnum;
                    this->pin_version(&vnum); 
                    std::thread mthread = std::thread(&LSMTree::merge_memtable, this, mtable, vnum, rng);
                    mthread.detach();
                } 
                // move to other memtable
                mtable = m_version_data[version_num].load()->swap_active_memtable(mtable);
            }

            // Attempt to insert
            if (mtable->append(key, val, tombstone)) {
                this->unpin_version(version_num);
                return 1;
            }

            // If insertion fails, we need to wait for the next version to
            // become available.
            this->unpin_version(version_num);
        } while (this->wait_for_version(version_num));

        return 0;
    }

    void range_sample(char *sample_set, const char *lower_key, const char *upper_key, size_t sample_sz, char *buffer, char *utility_buffer, gsl_rng *rng) {
        TIMER_INIT();

        size_t version_num;
        auto version = this->pin_version(&version_num);

        // Allocate buffer into which to write the samples
        size_t sample_idx = 0;

        // Obtain the sampling ranges for each level
        std::vector<SampleRange> memory_ranges;
        std::vector<SampleRange> disk_ranges;
        std::vector<size_t> record_counts;

        MemTableView *memtable = nullptr;
        while (!(memtable = this->memtable_view()))
            ;

        TIMER_START();

        size_t memtable_cutoff = memtable->get_record_count() - 1;
        record_counts.push_back(memtable_cutoff + 1);

        for (auto &level : version->mem_levels) {
            if (level) {
                level->get_sample_ranges(memory_ranges, record_counts, lower_key, upper_key);
            }
        }

        for (auto &level : version->disk_levels) {
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

                if (!add_to_sample(sample_record, INVALID_RID, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff, version)) {
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

                    if (!add_to_sample(sample_record, memory_ranges[i].run_id, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff, version)) {
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
                size_t level_idx = disk_ranges[i].run_id.level_idx - m_memory_level_cnt;
                size_t run_idx = disk_ranges[i].run_id.run_idx;

                while (run_samples[i+run_offset] > 0) {
                    TIMER_START();
                    size_t idx = gsl_rng_uniform_int(rng, range_length);
                    sample_record = version->disk_levels[level_idx]->get_run(run_idx)->sample_record(disk_ranges[i].low, idx, buffer, buffered_page);
                    run_samples[i+run_offset]--;
                    TIMER_STOP();
                    disklevel_sample_time += TIMER_RESULT();

                    if (!add_to_sample(sample_record, disk_ranges[i].run_id, upper_key, lower_key, utility_buffer, sample_set, sample_idx, memtable, memtable_cutoff, version)) {
                        rejections++;
                    }
                }
            }
        } while (sample_idx < sample_sz);

        this->unpin_version(version_num);
    }

    // Checks the tree and memtable for a tombstone corresponding to
    // the provided record in any run *above* the rid, which
    // should correspond to the run containing the record in question
    // 
    // Passing INVALID_RID indicates that the record exists within the MemTable
    bool is_deleted(const char *record, const RunId &rid, char *buffer, MemTableView *memtable, size_t memtable_cutoff, version_data *version) {
        // check for tombstone in the memtable. This will require accounting for the cutoff eventually.
        if (memtable->check_tombstone(get_key(record), get_val(record))) {
            return true;
        }

        // if the record is in the memtable, then we're done.
        if (rid == INVALID_RID) {
            return false;
        }

        for (size_t lvl=0; lvl<rid.level_idx; lvl++) {
            if (lvl < version->mem_levels.size()) {
                if (version->mem_levels[lvl]->tombstone_check(version->mem_levels[lvl]->get_run_count(), get_key(record), get_val(record))) {
                    return true;
                }
            } else {
                size_t isam_lvl = lvl - version->mem_levels.size();
                if (version->disk_levels[isam_lvl]->tombstone_check(version->disk_levels[isam_lvl]->get_run_count(), get_key(record), get_val(record), buffer)) {
                    return true;
                }

            }
        }

        // check the level containing the run
        if (rid.level_idx < version->mem_levels.size()) {
            size_t run_idx = std::min((size_t) rid.run_idx, version->mem_levels[rid.level_idx]->get_run_count() + 1);
            return version->mem_levels[rid.level_idx]->tombstone_check(run_idx, get_key(record), get_val(record));
        } else {
            size_t isam_lvl = rid.level_idx - version->mem_levels.size();
            size_t run_idx = std::min((size_t) rid.run_idx, version->disk_levels[isam_lvl]->get_run_count());
            return version->disk_levels[isam_lvl]->tombstone_check(run_idx, get_key(record), get_val(record), buffer);
        }
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

        for (size_t i=0; i<version->disk_levels.size(); i++) {
            if (version->disk_levels[i]) cnt += version->disk_levels[i]->get_record_cnt();
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

        for (size_t i=0; i<version->disk_levels.size(); i++) {
            if (version->disk_levels[i]) cnt += version->disk_levels[i]->get_tombstone_count();
        }

        this->unpin_version(version_num);
        return cnt;
    }

    size_t get_height() {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        auto height =  version->mem_levels.size() + version->disk_levels.size();
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

        for (size_t i=0; i<version->disk_levels.size(); i++) {
            if (version->disk_levels[i]) cnt += version->disk_levels[i]->get_aux_memory_utilization();
        }

        this->unpin_version(version_num);
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
        size_t version_num;
        auto version = this->pin_version(&version_num);

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

        this->unpin_version(version_num);
        return array;
    }

    /*
     * Flattens the entire LSM structure into a single ISAM Tree object
     * and return a pointer to it. Will be used as a simple baseline for
     * performance comparisons.
     */
    ISAMTree *get_flat_isam_tree(gsl_rng *rng) {
        size_t version_num;
        auto version = this->pin_version(&version_num);

        auto mem_level = new MemoryLevel(-1, 1);
        mem_level->append_mem_table(this->active_memtable(), rng);

        std::vector<InMemRun *> runs;
        std::vector<ISAMTree *> trees;


        for (int i=version->mem_levels.size() - 1; i>= 0; i--) {
            if (version->mem_levels[i]) {
                for (int j=0; j<version->mem_levels[i]->get_run_count(); j++) {
                    runs.push_back(version->mem_levels[i]->get_run(j));
                }
            }
        }

        runs.push_back(mem_level->get_run(0));

        for (int i=version->disk_levels.size() - 1; i >= 0; i--) {
            if (version->disk_levels[i]) {
                for (int j=0; j<version->disk_levels[i]->get_run_count(); j++) {
                    trees.push_back(version->disk_levels[i]->get_run(j));
                }
            }
        }

        auto pfile = PagedFile::create(m_root_directory + "flattened_tree.dat");
        auto bf = new BloomFilter(0, BF_HASH_FUNCS, rng);

        auto flat = new ISAMTree(pfile, rng, bf, runs.data(), runs.size(), trees.data(), trees.size());

        delete mem_level;
        delete bf;

        this->unpin_version(version_num);
        return flat;
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

        for (size_t i=0; i<version->disk_levels.size(); i++) {
            if (version->disk_levels[i]) {
                ts_prop = (long double) version->disk_levels[i]->get_tombstone_count() / (long double) this->calc_level_record_capacity(version->mem_levels.size() + i);
                if (ts_prop > (long double) m_ts_prop) {
                    this->unpin_version(version_num);
                    return false;
                }
            }
        }

        this->unpin_version(version_num);
        return true;
    }

private:
    version_number m_version;
    std::atomic<tagged_ptr<version_data>> m_version_data[LSM_MEMTABLE_CNT + 1];

    std::vector<MemTable *> m_memtables;
    std::atomic<bool> m_memtable_merging[LSM_MEMTABLE_CNT];

    std::mutex m_merge_lock;

    size_t m_scale_factor;
    double m_ts_prop;

    size_t m_memory_level_cnt;

    // The directory containing all of the backing files
    // for this LSM Tree.
    std::string m_root_directory;

    version_data *pin_version(size_t *version_num=nullptr) {
        // FIXME: ensure that the version pinned is the same as
        // the one retrieved. 
        size_t vnum = m_version.num;
        version_data *version = m_version_data[vnum].load().m_ptr;
        version->pins.fetch_add(1);

        if (version_num) {
            *version_num = vnum;
        }

        return version;
    }

    void unpin_version(size_t version_num) {
        m_version_data[version_num].load()->pins.fetch_add(-1);
    }

    MemTable *active_memtable(size_t *idx=nullptr) {
        size_t ver = m_version.num.load();
        if (ver >= LSM_MEMTABLE_CNT) { // all tables are currently merging
            // FIXME: temporary for single merge version
            m_version.incr_version(ver);
            return nullptr;
        }

        assert(ver < LSM_MEMTABLE_CNT);
        if (idx) {
            *idx=ver;
        }
        return m_memtables[ver];
    }


    MemTableView *memtable_view() {
        return MemTableView::create(m_memtables, sizeof(m_memtables));
    }

    inline bool rejection(const char *record, RunId rid, const char *lower_bound, const char *upper_bound, char *buffer, MemTableView *memtable, size_t memtable_cutoff, version_data *version) {
        if (is_tombstone(record)) {
            tombstone_rejections++;
            return true;
        } else if (key_cmp(get_key(record), lower_bound) < 0 || key_cmp(get_key(record), upper_bound) > 0) {
            bounds_rejections++;
            return true;
        } else if (this->is_deleted(record, rid, buffer, memtable, memtable_cutoff, version)) {
            deletion_rejections++;
            return true;
        }

        return false;
    }

    inline size_t rid_to_disk(RunId rid, size_t version_num) {
        return rid.level_idx - m_version_data[version_num].load()->mem_levels.size();
    }

    inline bool add_to_sample(const char *record, RunId rid, const char *upper_key, const char *lower_key, char *io_buffer,
                              char *sample_buffer, size_t &sample_idx, MemTableView *memtable, size_t memtable_cutoff,
                              version_data *version) {
        TIMER_INIT();
        TIMER_START();
        sampling_attempts++;
        if (!record || rejection(record, rid, lower_key, upper_key, io_buffer, memtable, memtable_cutoff, version)) {
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
    inline level_index grow(version_data *version) {
        level_index new_idx;

        size_t new_run_cnt = (LSM_LEVELING) ? 1 : m_scale_factor;
        if (version->mem_levels.size() < m_memory_level_cnt) {
            new_idx = version->mem_levels.size();
            if (new_idx > 0) {
                assert(version->mem_levels[new_idx - 1]->get_run(0)->get_tombstone_count() == 0);
            }
            version->mem_levels.emplace_back(new MemoryLevel(new_idx, new_run_cnt));
        } else {
            new_idx = version->disk_levels.size() + version->mem_levels.size();
            if (version->disk_levels.size() > 0) {
                assert(version->disk_levels[version->disk_levels.size() - 1]->get_run(0)->get_tombstone_count() == 0);
            }
            version->disk_levels.emplace_back(new DiskLevel(new_idx, new_run_cnt, m_root_directory));
        } 

        version->last_level_idx++;
        return new_idx;
    }

    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    inline void merge_memtable(MemTable *mtable, size_t version_num, gsl_rng *rng) {
        fprintf(stderr, "Starting merge for %ld\n", version_num);
        m_merge_lock.lock();

        auto new_version = version_data::create_copy(m_version_data[version_num]);

        m_version.incr_version();
        if (!this->can_merge_with(0, mtable->get_record_count(), new_version.get())) {
            this->merge_down(0, new_version.get(), rng);
        }

        this->merge_memtable_into_l0(mtable, new_version.get(), rng);
        this->enforce_tombstone_maximum(0, new_version.get(), rng);

        bool truncation_status = false;
        mtable->truncate(&truncation_status);

        while (!truncation_status) 
            ;

        m_memtable_merging[version_num].store(false);
        m_merge_lock.unlock();
        fprintf(stderr, "merge done for %ld\n", version_num);
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
                version->disk_levels[base_idx].reset(DiskLevel::merge_levels(version->disk_levels[base_idx].get(), version->disk_levels[incoming_idx].get(), rng));
            } else {
                version->disk_levels[base_idx]->append_merged_runs(version->disk_levels[incoming_idx].get(), rng);
            }
            version->disk_levels[incoming_idx].reset(new DiskLevel(incoming_level, (LSM_LEVELING) ? 1 : m_scale_factor, m_root_directory));
        } else if (base_disk_level) {
            // Merging the last memory level into the first disk level
            assert(base_idx == 0);
            assert(incoming_idx == m_memory_level_cnt - 1);
            if (LSM_LEVELING) {
                version->disk_levels[base_idx].reset(DiskLevel::merge_levels(version->disk_levels[base_idx].get(), version->mem_levels[incoming_idx].get(), rng));
            } else {
                version->disk_levels[base_idx]->append_merged_runs(version->mem_levels[incoming_idx].get(), rng);
            }

            version->mem_levels[incoming_idx].reset(new MemoryLevel(incoming_level, (LSM_LEVELING) ? 1 : m_scale_factor));
        } else {
            // merging two memory levels
            if (LSM_LEVELING) {
                version->mem_levels[base_idx].reset(MemoryLevel::merge_levels(version->mem_levels[base_idx].get(), version->mem_levels[incoming_idx].get(), rng));
            } else {
                version->mem_levels[base_idx]->append_merged_runs(version->mem_levels[incoming_idx].get(), rng);
            }

            version->mem_levels[incoming_idx].reset(new MemoryLevel(incoming_level, (LSM_LEVELING) ? 1 : m_scale_factor));
        }
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

    /*
     * Mark a given disk level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(DiskLevel *level) {
        level->mark_unused();
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
        bool disk_level;
        size_t level_idx = this->decode_level_index(idx, &disk_level);

        long double ts_prop = (disk_level) ? (long double) version->disk_levels[level_idx]->get_tombstone_count() / (long double) this->calc_level_record_capacity(idx)
                                           : (long double) version->mem_levels[level_idx]->get_tombstone_count() / (long double) this->calc_level_record_capacity(idx);

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

        bool disk_level;
        size_t vector_index = decode_level_index(idx, &disk_level);
        if (disk_level) {
            return (version->disk_levels[vector_index]) ? version->disk_levels[vector_index]->get_record_cnt() : 0;
        } 

        return (version->mem_levels[vector_index]) ? version->mem_levels[vector_index]->get_record_cnt() : 0;
    }

    /*
     * Determines if the specific level can merge with another record containing
     * incoming_rec_cnt number of records. The provided level index should be 
     * non-negative (i.e., not refer to the memtable) and will be automatically
     * translated into the appropriate index into either the disk or memory level
     * vector.
     */
    inline bool can_merge_with(level_index idx, size_t incoming_rec_cnt, version_data *version) {
        bool disk_level;
        ssize_t vector_index = decode_level_index(idx, &disk_level);
        assert(vector_index >= 0);

        if (disk_level) {
            if (LSM_LEVELING) {
                return version->disk_levels[vector_index]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_record_capacity(idx);
            } else {
                return version->disk_levels[vector_index]->get_run_count() < m_scale_factor;
            }
        } 

        if (vector_index >= version->mem_levels.size() || !version->mem_levels[vector_index]) {
            return false;
        }

        if (LSM_LEVELING) {
            return version->mem_levels[vector_index]->get_record_cnt() + incoming_rec_cnt <= this->calc_level_record_capacity(idx);
        } else {
            return version->mem_levels[vector_index]->get_run_count() < m_scale_factor;
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

        if (idx < m_memory_level_cnt) return idx;

        *disk_level = true;
        return idx - m_memory_level_cnt;
    }

    void await_merge_completion() {
        bool done = false;
        while (!done) {
            done = true;
            for (size_t i=0; i<LSM_MEMTABLE_CNT; i++) {
                if (m_memtable_merging[i] == true) {
                    done = false;
                }
            }
        }
    }


    bool wait_for_version(size_t version_num) {
        // TODO: replace with condition variable
        while (m_version.num == version_num) 
            ;

        return true;

    }
};
}
