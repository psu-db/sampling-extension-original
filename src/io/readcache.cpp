/*
 * readcache.cpp
 * DouglasRumbaugh
 *
 * ReadCache implementation
 */

#include "io/readcache.hpp"

namespace lsm { namespace io {

ReadCache::ReadCache(size_t frame_capacity, bool benchmarks)
{
    this->frame_data = std::unique_ptr<byte, decltype(&free)>((byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE * frame_capacity), &free);
    this->metadata = std::vector<FrameMeta>(frame_capacity);
    this->frame_map = std::unordered_map<PageId, FrameId, PageIdHash>();
    this->frame_cap = frame_capacity;
    this->frame_cnt = 0;
    this->clock_hand = 0;
    this->misses = 0;
    this->io_block_time = 0;
    this->benchmarks = benchmarks;
}


FrameId ReadCache::pin(PageId pid, PagedFile *pfile, byte **frame_ptr)
{
    auto map_entry = this->frame_map.find(pid);

    FrameId frame;
    if (map_entry != this->frame_map.end()) {
        frame = map_entry->second;
    } else if (this->frame_cnt < this->frame_cap) {
        frame = this->frame_cnt++;
        this->load_page(pid, frame, pfile);
    } else {
        frame = this->find_frame_to_evict();
        if (frame == INVALID_FRID) { // couldn't evict anything
            *frame_ptr = nullptr;
            return INVALID_FRID;
        }

        this->frame_map.erase(this->metadata[frame].pid);
        this->load_page(pid, frame, pfile);
    }

    this->metadata[frame].pin_cnt++;
    this->metadata[frame].clock_value = 0;
    *frame_ptr = this->get_frame(frame);
    return frame;
}


FrameId ReadCache::pin(PageNum pnum, PagedFile *pfile, byte **frame_ptr)
{
    auto pid = pfile->pnum_to_pid(pnum);
    return this->pin(pid, pfile, frame_ptr);
}


void ReadCache::unpin(FrameId frid)
{
    if (frid == INVALID_FRID || frid > this->frame_cnt || this->metadata[frid].pin_cnt == 0) {
        // cannot unpin
        return;
    }

    this->metadata[frid].pin_cnt--;
}


FrameId ReadCache::find_frame_to_evict()
{
    // find a frame to evict
    auto starting_clock = this->clock_hand;
    int passed_starting = 0;
    // for now, once we do two complete sweeps we'll stop and
    // return an error. Once concurrency is active, it'll make
    // sense to keep looping here until something opens up.
    while (passed_starting < 2 || clock_hand != starting_clock) {
        auto meta = &this->metadata[this->clock_hand]; 

        if (meta->pin_cnt == 0) {
            if (meta->clock_value) {
                return meta->frid;
            }

            meta->clock_value = 1;
        }

        if (clock_hand == starting_clock) {
            passed_starting++;
        }

        this->clock_hand = (this->clock_hand + 1) % this->frame_cap;
    }

    return INVALID_FRID;
}


int ReadCache::load_page(PageId pid, FrameId frid, PagedFile *pfile) 
{
    this->misses++;

    int res;

    if (benchmarks) {
        auto start = std::chrono::high_resolution_clock::now();
        res = pfile->read_page(pid.page_number, this->get_frame(frid));
        auto stop = std::chrono::high_resolution_clock::now();
        this->io_block_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    } else {
        res = pfile->read_page(pid.page_number, this->get_frame(frid));
    }

    if (res == 0) {
        return 0;
    }

    this->metadata[frid] = {
        pid,
        frid,
        0,
        0
    };

    this->frame_map.insert({pid, frid});
    return 1;
}


byte *ReadCache::get_frame(FrameId frid) 
{
    return this->frame_data.get() + frid * parm::PAGE_SIZE;
}


byte *ReadCache::get_frame_ptr(FrameId frid)
{
    if (frid == INVALID_FRID || frid > this->frame_cnt) {
        return nullptr;
    }

    return this->get_frame(frid);
}


size_t ReadCache::cache_misses()
{
    return this->misses;
}


void ReadCache::reset_miss_counter()
{
    this->misses = 0;
}


size_t ReadCache::io_time()
{
    return this->io_block_time;
}


void ReadCache::reset_io_time() 
{
    this->io_block_time = 0;
}

}}
