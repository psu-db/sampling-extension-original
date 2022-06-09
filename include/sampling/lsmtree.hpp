/*
 *
 */

#ifndef H_LSMTREE
#define H_LSMTREE

#include <map>

#include "util/global.hpp"
#include "io/record.hpp"
#include "sampling/sample.hpp"
#include "sampling/level.hpp"
#include "catalog/schema.hpp"
#include "ds/memtable.hpp"
#include "ds/walker.hpp"

namespace lsm { namespace sampling {

enum merge_policy {
    LEVELING,
    TIERING
};

class LSMTree {
public:
    /*
     * Create a new, empty LSMTree index and return a unique_ptr
     * to it. If the operation fails, returns nullptr.
     */
    std::unique_ptr<LSMTree> create();

    /*
     * Open an already existing LSMTree index from disk and return
     * a unique_ptr to it. If the operation fails, returns nullptr.
     */
    std::unique_ptr<LSMTree> open(std::string filename);

    ~LSMTree() = default;

    /*
     * Search the index for the first record with a matching key, and with
     * a timestamp less than or equal to time. If no such record is found,
     * returns and invalid record instead.
     */
    io::Record get(byte *key, Timestamp time=0);

    /*
     * Insert a new record into the LSMTree. The input key and value
     * pointers must refer to buffers of the correct length, as defined
     * in the index's scheme. The result is undefined, if the lengths are
     * not correct. 
     *
     * Returns 1 on success and 0 on failure.
     */
    int insert(byte *key, byte *value, Timestamp time=0);
    
    /*
     * Delete a record from the LSMTree having a matching key and value, and with
     * a timestamp less than or equal to the specified one. The input key and value
     * pointers must refer to buffers of the correct length, as defined in teh index's
     * schema. The result is undefined if the lengths are not correct.
     *
     * Returns 1 if a record is deleted, and 0 if one is not (due to an error, or due to
     * not finding the record in the index).
     */
    int remove(byte *key, byte *value, Timestamp time=0);

    /*
     * Locate and delete a record with matching key and old_value. If the
     * delete is successful, insert a new record with key, new_value into the
     * index.
     */
    int update(byte *key, byte *old_value, byte *new_value, Timestamp time=0);

    /*
     * Perform a range sampling operation and return a Sample object populated
     * with sample_size records, sampled with replacement from the index. If a
     * sample cannot be created, due to an error, or due to no records falling
     * within the sampling interval specified, returns nullptr.
     *
     * Note that the returned Sample object will manage copies of the records
     * within the sample. Thus, access to the records are guaranteed valid
     * for the lifetime of the Sample object. They will be automatically freed
     * upon its destruction.
     */
    std::unique_ptr<Sample> range_sample(byte *start_key, byte *stop_key, size_t sample_size, size_t *rejections=nullptr, size_t *attempts=nullptr);

private:
    global::g_state state;
    std::vector<std::unique_ptr<BTreeLevel>> levels;
    std::unique_ptr<ds::MemoryTable> memtable;

    size_t record_count;
    size_t memtable_capacity;

    size_t scale_factor;
    merge_policy policy;
    double max_deleted_proportion;

    bool bloom_filters;
    bool range_filters;

    LSMTree(size_t memtable_capacity, size_t scale_factor, std::unique_ptr<catalog::FixedKVSchema> schema,
            std::unique_ptr<io::ReadCache> cache, std::unique_ptr<io::FileManager> filemanager, merge_policy policy=LEVELING,
            bool bloom_filters=false, bool range_filers=false, double max_deleted_proportion=1.0);

    void merge_memtable();
    size_t grow();
};

}}
#endif
