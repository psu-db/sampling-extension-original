/*
 *
 */

#ifndef H_LEVEL
#define H_LEVEL

#include "util/base.hpp"
#include "util/types.hpp"
#include "io/record.hpp"
#include "util/global.hpp"

#include <cmath>

#include "ds/isamtree.hpp"
#include "ds/bloomfilter.hpp"
#include "sampling/isamtree_samplerange.hpp"


namespace lsm { namespace sampling {

class LSMTreeLevel {
public:
    /*
     * Create a new LSMTreeLevel object
     */
    LSMTreeLevel() = default;

    ~LSMTreeLevel() = default;

    /*
     * Returns true if the level's run count is less than its capacity
     * (i.e. it is a valid target for merging the previous level into),
     * and false if not.
     */
    virtual bool can_emplace_run() = 0;

    virtual int merge_with(LSMTreeLevel *level) = 0;
    virtual int merge_with(std::unique_ptr<iter::GenericIterator<io::Record>> sorted_itr, size_t tombstone_count) = 0;

    /*
     * Determine if a run containing incoming_record_count records can be
     * merged into this one without exceeding its record capacity. If so,
     * return true, otherwise return false.
     */
    virtual bool can_merge_with(size_t incoming_record_count) = 0;

    /*
     * Determine if a level containing incoming_record_count can be merged into
     * this one without exceeding its record capacity. If so, return true,
     * otherwise return false.
     */
    virtual bool can_merge_with(LSMTreeLevel *level) = 0;

    virtual std::vector<std::unique_ptr<SampleRange>> get_sample_ranges(byte *lower_key, byte *upper_key) = 0;

    /*
     * Remove the references to all runs within the level, and
     * reset the run count and record count to 0.
     */
    virtual void truncate() = 0;

    /*
     * Perform a search for the specified key within this level, and return a
     * Record object representing the first record in which a match is found
     * having a timestamp less than or equal to the specified one, or an
     * invalid record if no match is found. 
     */
     virtual Record get(const byte *key, FrameId *frid, Timestamp time=0) = 0;


    /*
     * Perform a search for a tombstone for a record with the specified key
     * and value, that is active with respect to time. Return this tombstone
     * if found, otherwise return an invalid record.
     */
     virtual Record get_tombstone(const byte *key, const byte *val, FrameId *frid, Timestamp time=0) = 0;

    /*
     * Perform a search for the specified key within this level and mark the
     * first corresponding record found with a matching value and a timestamp less
     * than or equal to the specified one as deleted. If a record is marked
     * deleted, returns 1. Otherwise, returns 0.
     */
    virtual int remove(byte *key, std::byte *value, Timestamp time=0) = 0;

    /*
     * Returns the maximum number of records that this level
     * can store.
     */
    virtual size_t get_record_capacity() = 0;

    /*
     * Returns the maximum number of runs that can be present
     * on this level.
     */
    virtual size_t get_run_capacity() = 0;

    /*
     * Returns the current number of records that are stored
     * on this level.
     */
    virtual size_t get_record_count() = 0;

    /*
     * Returns the current number of runs that are stored on
     * this level.
     */
    virtual size_t get_run_count() = 0;

    /*
     * Returns the current number of tombstones that are stored
     * on this level.
     */
    virtual size_t get_tombstone_count() = 0;

    /*
     * Returns the current amount of memory used by all of
     * the runs within this level.
     */
    virtual size_t memory_utilization() = 0;

    /*
     * Print a graphical representation of this level to stdout, 
     * showing each run on the level, with its record count and
     * capacity.
     */
    virtual void print_level() = 0;

    /*
     * Returns True if the entire level is stored in memory, and false if the
     * level is stored externally.
     */
    virtual bool is_memory_resident() = 0;

    /*
     * Opens an iterator on this level.
     */
    virtual std::unique_ptr<iter::GenericIterator<Record>> start_scan() = 0;

    };
}}
#endif

