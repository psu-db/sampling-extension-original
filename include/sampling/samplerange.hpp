/*
 *
 */
#ifndef H_SAMPLERANGE
#define H_SAMPLERANGE

#include "io/record.hpp"
#include "io/readcache.hpp"

namespace lsm { namespace sampling {

class SampleRange {
public:
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
    virtual io::Record get(FrameId *frid) = 0;

    /*
     * Randomly select and return a page_id from this sample range, without
     * performing any IO. The returned page must be pinned externally to this
     * call.  If the underlying data structure does not support page access (as
     * is the case with memtables, generally), or the sample range is empty,
     * the returned value will be INVALID_PID.
     */
    virtual PageId get_page() = 0;

    /*
     * Return the number of records falling within the sampling range. Note
     * that, in rejection sampling, the length of the range may be larger than
     * than the number of valid records falling within that range.
     */
    virtual size_t length() = 0;

    virtual ~SampleRange() = default;
};

}}
#endif
