#ifndef READCACHE_H
#define READCACHE_H

#include "util/base.hpp"
#include "util/types.hpp"

#include "io/directfile.hpp"

namespace lsm {
namespace io {

struct FrameMeta {
    PageId pid;
    FrameId fid;
    size_t pin_cnt;
    size_t clock_value;
};

class ReadCache {
public:
    ReadCache();
    ~ReadCache();

    FrameId pin(PageId pid, DirectFile *dfile, byte **frame_ptr);
    FrameId pin(PageNum pnum, DirectFile *dfile, byte **frame_ptr);
    void unpin(FrameId fid);

    PageId get_pid(FrameId fid);
    char *get_frame_ptr(FrameId fid);
private:
    std::unique_ptr<byte> frame_data;
    std::unique_ptr<FrameMeta> metadata;

};

}
}

#endif
