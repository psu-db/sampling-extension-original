/*
 *
 */

#ifndef H_SORTEDLEVEL
#define H_SORTEDLEVEL

#include "util/base.hpp"
#include "util/types.hpp"
#include "io/record.hpp"
#include "util/global.hpp"

#include <cmath>

#include "ds/sortedrun.hpp"
#include "ds/bloomfilter.hpp"
#include "sampling/sorted_samplerange.hpp"
#include "sampling/lsmlevel.hpp"

namespace lsm { namespace sampling {

class SortedLevel : public LSMTreeLevel {
public:
    /*
     * Create a new SortedLevel object of specified run capacity (1=LEVELING,
     * >1=TIERING) and record capacity. The files vector contains the filenames
     * for pre-existing SortedRuns that make up this level. The length of this
     * vector must be no greater than the run capacity. An empty vector can be
     * provided if the run is to be created empty, with no pre-existing data.
     */
    SortedLevel(size_t run_capacity, size_t record_capacity,
               std::vector<io::IndexPagedFile *> files, global::g_state *state,
               double max_deletion_proportion, bool bloom_filters);

    ~SortedLevel() = default;

    /*
     * Return a raw pointer to the specified run within this level,
     * or nullptr if the specified index is out of range. There
     * aren't many situations where you should need to access a
     * run within an LSMTree directly. The returned pointer should
     * not be freed, and the owner of this pointer does NOT control
     * the lifetime of the object.
     */
    ds::SortedRun *get_run(size_t idx);


    /*
     * Returns true if the level's run count is less than its capacity
     * (i.e. it is a valid target for merging the previous level into),
     * and false if not.
     */
    bool can_emplace_run() override;

    int merge_with(LSMTreeLevel *level) override;
    int merge_with(std::unique_ptr<iter::GenericIterator<io::Record>> sorted_itr, size_t tombstone_count) override;

    /*
     * Determine if a run containing incoming_record_count records can be
     * merged into this one without exceeding its record capacity. If so,
     * return true, otherwise return false.
     */
    bool can_merge_with(size_t incoming_record_count) override;

    /*
     * Determine if a level containing incoming_record_count can be merged into
     * this one without exceeding its record capacity. If so, return true,
     * otherwise return false.
     */
    bool can_merge_with(LSMTreeLevel *level) override;


    std::vector<std::unique_ptr<SampleRange>> get_sample_ranges(byte *lower_key, byte *upper_key) override;

    /*
     * Remove the references to all runs within the level, and
     * reset the run count and record count to 0.
     */
    void truncate() override;

    /*
     * Perform a search for the specified key within this level, and return a
     * Record object representing the first record in which a match is found
     * having a timestamp less than or equal to the specified one, or an
     * invalid record if no match is found. 
     */
     Record get(const byte *key, FrameId *frid, Timestamp time=0) override;


    /*
     * Perform a search for a tombstone for a record with the specified key
     * and value, that is active with respect to time. Return this tombstone
     * if found, otherwise return an invalid record.
     */
     Record get_tombstone(const byte *key, const byte *val, FrameId *frid, Timestamp time=0) override;

    /*
     * Perform a search for the specified key within this level and mark the
     * first corresponding record found with a matching value and a timestamp less
     * than or equal to the specified one as deleted. If a record is marked
     * deleted, returns 1. Otherwise, returns 0.
     */
    int remove(byte *key, std::byte *value, Timestamp time=0) override;

    /*
     * Returns the maximum number of records that this level
     * can store.
     */
    size_t get_record_capacity() override;

    /*
     * Returns the maximum number of runs that can be present
     * on this level.
     */
    size_t get_run_capacity() override;

    /*
     * Returns the current number of records that are stored
     * on this level.
     */
    size_t get_record_count() override;

    /*
     * Returns the current number of tombstones that are stored
     * on this level.
     */
    size_t get_tombstone_count() override;

    /*
     * Returns the current number of runs that are stored on
     * this level.
     */
    size_t get_run_count() override;

    /*
     * Returns the current amount of memory used by all of
     * the runs within this level.
     */
    size_t memory_utilization() override;

    /*
     * Print a graphical representation of this level to stdout, 
     * showing each run on the level, with its record count and
     * capacity.
     */
    void print_level() override;


    bool is_memory_resident() override;

    /*
     * Opens an iterator on this level.
     */
    std::unique_ptr<iter::GenericIterator<Record>> start_scan() override;

private:
    size_t record_capacity; // the maximum number of records on this level
    size_t record_count;    // the current number of records on this level

    size_t run_capacity;    // the maximum number of runs on this level
    size_t run_count;       // the current_number of runs on this level

    double max_deleted_prop; // The largest allowed proportion of deleted
                             // records before performing a compaction
    
    global::g_state *state;  // tracks global state inherited from the parent
                             // environment (probably an LSMTree object)
    bool bloom_filters;
    catalog::RecordCmpFunc record_cmp;
    catalog::KeyCmpFunc key_cmp;

    std::vector<std::unique_ptr<ds::SortedRun>> runs; // the runs stored on this level

    /*
     * Place the specified pointer to a run into the level,
     * and return the index at which it was stored. If there is
     * no room, or the run cannot be placed for some other reason,
     * returns -1. Note that this uses std::move to place the
     * pointer into the Level, and so the passed in pointer will
     * be nulled.
     */
    int emplace_run(std::unique_ptr<ds::SortedRun> run);

    };
}}
#endif

