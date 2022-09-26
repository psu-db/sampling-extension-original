
#pragma once

#include <cassert>
#include "lsm/IsamTree.h"
#include "lsm/SampleRange.h"
#include "util/record.h"

namespace lsm {

class DiskLevel {
public:
    DiskLevel(size_t run_capacity) 
    : runs(std::vector<ISAMTree*>(run_capacity)), run_tail_ptr(0)
    {}

    ~DiskLevel() {
        this->truncate();
    }

    std::vector<SampleRange *> get_sample_ranges(const char *lower_bound, const char *upper_bound, char *buffer) {
        // arguments cannot be null
        assert(lower_bound);
        assert(upper_bound);
        assert(buffer);

        // lower bound must be less than or equal to the upper bound
        assert(key_cmp(lower_bound, upper_bound) <= 0);

        std::vector<SampleRange *> ranges(this->run_tail_ptr);

        for (size_t i=0, j=0; i<this->run_tail_ptr; i++) {
            auto lb = this->runs[i]->get_lower_bound(lower_bound, buffer);
            auto ub = this->runs[i]->get_upper_bound(upper_bound, buffer);
            ranges[j++] = new DiskSampleRange(this->runs[i], lb, ub);
        }

        return ranges;
    }

    // This will flat out delete all of the runs on this level,
    // so if the intent is to move a run to a different level,
    // once the pointer is copied be sure to set the entry for
    // that run on this level to NULL.
    void truncate() {
        for (size_t i=0; i<this->run_tail_ptr; i++) {
            delete this->runs[i];
        }
    }

/*
 * Functions used for merging and compaction
 */

    /*
     * Proactively compact the runs in this level to 
     * remove deleted records, etc. Note that we could
     * also use this to proactively merge all the runs
     * on this level into one for teiring purposes--it
     * may make sense to proactively do this.
     */
    void compact_runs() { 
        // TODO: awaiting ISAMTree compaction interface
    }

    /*
     * Return a pointer to the specified run, if it
     * exists within this level, and set its current
     * reference within the level to nullptr.
     */
    ISAMTree *get_and_remove_run(size_t idx) {
        assert(idx < this->runs.size());

        ISAMTree *run = this->runs[idx];
        this->runs[idx] = nullptr;
        return run;
    }


    /*
     * Append a new run to this level. The current
     * run count must be less than the run capacity.
     */
    void add_run(ISAMTree *run) {
        assert(this->run_tail_ptr < this->runs.size()); // there must be room for the run
        assert(run);

        this->runs[this->run_tail_ptr++] = run;
    }

    /*
     * Merge all of the runs on this level into a single,
     * new ISAM Tree, fit for putting into a lower level.
     *
     * Leaves this level untouched. Once the new run has
     * been installed somewhere, this level should be
     * cleared via a call to truncate().
     *
     * NOTE: In the current state, if there is only 1 run
     * on this level, a reference to it is returned for
     * efficiency. This means that this level should *not*
     * be truncated, but rather the reference to the run
     * in question should be nulled out. It would be cleaner
     * to just return a copy--but that is extra work we
     * don't need to do.
     */
    ISAMTree *merge_all_runs() {
        assert(this->run_tail_ptr != 0); // must be runs to merge
        
        // if there is only one run, just return it.
        if (this->run_tail_ptr == 1) {
            return this->runs[0];
        }

        // pending ISAM refactor for new merging interface
        // return new ISAMTree(this->runs);
        // or create a vector of iterators and pass that in,
        // depending on the interface we settle on.

        return nullptr;
    }

/*
 * General use accessors
 */

    /*
     * Return the number of runs that this level can hold
     */
    size_t run_capacity() {
        return this->runs.size();
    }


    /*
     * Return the number of runs that this level currently
     * holds
     */
    size_t run_count() {
        return this->run_tail_ptr;
    }

    // not sure that we care about record or tombstone
    // counts here, but those can be exposed by walking
    // over the runs and tallying.

    size_t memory_utilization() {
        size_t mem = 0;
        for (size_t i=0; i<this->run_tail_ptr; i++) {
            mem += this->runs[i]->memory_utilization();
        }

        return mem;
    }

  private:
    std::vector<ISAMTree *> runs;
    size_t run_tail_ptr;
    
};
}
