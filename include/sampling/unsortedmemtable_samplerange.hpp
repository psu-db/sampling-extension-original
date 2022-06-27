/*
 *
 */

#ifndef H_UNSORTEDMEMTABLESAMPLERANGE
#define H_UNSORTEDMEMTABLESAMPLERANGE

#include <vector>
#include <map>

#include "sampling/samplerange.hpp"
#include "util/global.hpp"

namespace lsm { namespace sampling {

class UnsortedMemTableSampleRange : public SampleRange {
public:
    /*
    static std::unique_ptr<SampleRange> create(ds::UnsortedMemTable *table, byte *lower_key, 
                                               byte *upper_key, global::g_state *state);
     */

    UnsortedMemTableSampleRange(std::vector<io::Record>::const_iterator begin, std::vector<io::Record>::const_iterator end,
                                const byte *lower_key, const byte *upper_key, global::g_state *state);


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

    /*
     * Return the number of records falling within the sampling range. Note
     * that, in rejection sampling, the length of the range may be larger than
     * than the number of valid records falling within that range.
     */
    size_t length() override;

    ~UnsortedMemTableSampleRange() {}

private:
    io::Record get_random_record();

    std::vector<byte *> records;
    global::g_state *state;
};

}}

#endif
