/*
 *
 */

#ifndef H_SORTEDSAMPLERANGE
#define H_SORTEDSAMPLERANGE

#include "sampling/samplerange.hpp"
#include "ds/sortedrun.hpp"
#include "util/global.hpp"
#include "io/record.hpp"

namespace lsm { namespace sampling {

class SortedSampleRange : public SampleRange {
public:
    static std::unique_ptr<SampleRange> create(ds::SortedRun *run, byte *lower_key, 
                                               byte *upper_key, global::g_state *state);

    SortedSampleRange(ds::SortedRun *run, size_t start_idx, byte *lower_key, size_t stop_idx, 
                     byte *upper_key, size_t record_count, global::g_state *state);

    /*
     * Randomly select and return a record from this sample range, pinning the
     * page containing it in the cache and setting frid accordingly. It will
     * make one attempt. If it samples an invalid record (due to it being out
     * of range, deleted, etc.), the returned record and frid will be invalid.
     * Otherwise they will be valid. It is the responsibility of the caller to
     * unpin frid once it is done with the sampled record.
     *
     * If the underlying data structure does not support caching (such as if it
     * is already in memory) then frid will always be set to invalid. An
     * implementation can allow an nullptr to be based as a default argument in
     * such cases, for convenience. 
     */
    io::Record get(FrameId *frid) override;

    PageId get_page() override;

    /*
     * Return the number of records falling within the sampling range. Note
     * that, in rejection sampling, the length of the range may be larger than
     * than the number of valid records falling within that range.
     */
    size_t length() override;

    bool is_memtable() override;
    bool is_memory_resident() override;

    ~SortedSampleRange() {}

private:

    io::Record get_random_record(FrameId *frid);

    ds::SortedRun *run;
    PageNum start_idx;
    PageNum stop_idx;
    byte *upper_key;
    byte *lower_key;
    global::g_state *state;
    size_t record_count;
    PageNum range_len;
    catalog::KeyCmpFunc cmp;
};

}}

#endif