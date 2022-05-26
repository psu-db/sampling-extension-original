/*
 * readcache.hpp
 * Douglas Rumbaugh
 *
 * A simple readcache implementation. It is structured a lot like a 
 * general buffer manager, but does not support updating the underlying
 * files--there is no flushing or modification. Any updates to the memory
 * within the cache will be lost when the associated frame is evicted and
 * replaced, and will not be persisted in the underlying file.
 *
 * TODO: Once the file manager is implement, a few more overloads can be
 *       added to allow operations with PageIds alone, rather than PageId
 *       and files. Also, some signatures will need to be updated to support
 *       virtual files as well, once those are implemented.
 */
#ifndef READCACHE_H
#define READCACHE_H

#include <unordered_map>
#include <vector>

#include "util/base.hpp"
#include "util/types.hpp"

#include "io/pagedfile.hpp"

namespace lsm {
namespace io {

struct FrameMeta {
    PageId pid;          // the id of the page stored in this frame
    FrameId frid;        // the id of this frame (should match its index)
    size_t pin_cnt;      // the number of active pins on this frame
    int32_t clock_value; // used for eviction purposes
};

class ReadCache {
public:
    /*
     * A simple cache for storing data from PagedFile objects within memory.
     * frame_capacity is the maximum number of buffer frames, each with size
     * parm::PAGE_SIZE, allocated by the buffer. When these are exhausted, it
     * will begin evicting unpinned pages using the clock algorithm.
     */
    ReadCache(size_t frame_capacity=1000);

    /*
     * Default destructor for ReadCache. All memory is either stack-allocated,
     * or guarded with smartpointers, so no special cleanup action is required.
     */
    ~ReadCache() = default;

    /*
     * Attempt to pin a page, given by pid, from pfile, into the read cache.
     * On success, the page will be loaded into the frame specified by the
     * returned FrameId, which can be used to unpin the page later. Additionally,
     * the frame_ptr argument will be updated to refer to the start of this frame
     * in memory. No attempt should ever be made to free the memory pointed to by
     * frame_ptr--the ReadCache will manage its own memory.
     *
     * If the pin attempt fails, INVALID_FRID will be returned, and frame_ptr
     * will be updated to nullptr. This can occur if there is an IO error of 
     * some sort, or if the buffer is full and there are no valid targets for
     * eviction.
     */
    FrameId pin(PageId pid, PagedFile *pfile, byte **frame_ptr);

    /*
     * See pin(PageNum, PagedFile, byte **) for details. This works the same,
     * but accepts a Page Number rather than a Page ID.
     */
    FrameId pin(PageNum pnum, PagedFile *pfile, byte **frame_ptr);

    /*
     * If the frame specified by frid has at least 1 pin, reduce the number of
     * pins by 1. This should be called on each frame when you are done with
     * it, as the cache will only reuse frames that have zero active pins.
     *
     * If the specified frame id has no pins, or is invalid, this function will
     * do nothing.
     */
    void unpin(FrameId frid);

    /*
     * Looks up the PageId associated with the frame given by frid and returns it.
     * If the frid is invalid, or refers to a page without any active pins, returns
     * INVALID_PID instead.
     */
    PageId get_pid(FrameId frid);

    /*
     * Looks up the beginning of the frame represented by frid and returns a pointer
     * to it. The returned pointer should not be freed--the cache will manage its own
     * memory. If the specified frid is invalid, or if it has no active pins, returns
     * nullptr instead.
     */
    byte *get_frame_ptr(FrameId frid);
private:
    std::unique_ptr<byte> frame_data;
    std::vector<FrameMeta> metadata;
    std::unordered_map<PageId, FrameId, PageIdHash> frame_map;

    FrameId frame_cnt;
    FrameId frame_cap;
    FrameId clock_hand;

    /*
     * Internal version of get_frame_ptr without any error checking. Calling
     * this with a frid that is greater than the cache's frame_cap is undefined.
     */
    byte *get_frame(FrameId frid);

    /*
     * Attempts to read the specified page from the specified file, and place its
     * contents within the frame represented by frid, and insert associated
     * records into the metadata array and frame_map. The page is not pinned on
     * return of this function.
     *
     * Returns 1 on success, and 0 on failure. Can fail due to an IO error, or
     * if the specified frid is invalid.
     */
    int load_page(PageId pid, FrameId frid, PagedFile *pfile);

    /*
     * Use the clock algorithm to find a frame to evict, and return its id. If no
     * frame can be found, returns INVALID_FRID. This only locates the frame, it 
     * does not do any cleanup of metadata within the cache associated with an
     * eviction--that must be handled by the caller.
     */
    FrameId find_frame_to_evict();
};

}
}

#endif
