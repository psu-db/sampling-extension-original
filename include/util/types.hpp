#ifndef TYPES_H
#define TYPES_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <string>

#include "util/base.hpp"


namespace lsm {

using std::byte;

typedef uint32_t PageNum;
typedef uint32_t FileId;
typedef uint16_t PageOffset;
typedef uint16_t SlotId;

struct PageId {
    PageNum page_number;
    FileId file_id;
};

constexpr size_t PageIdSize = MAXALIGN(sizeof(PageId));

struct RecordId {
    PageId pid;
    PageOffset offset;
};

constexpr size_t RecordIdSize = MAXALIGN(sizeof(RecordId));

typedef uint32_t FrameId;
typedef uint64_t Timestamp;

const PageNum INVALID_PNUM = 0;
const FileId INVALID_FLID = 0;
const FrameId INVALID_FRID = 0;
const PageId INVALID_PID = {0, 0};
const SlotId INVALID_SID = 0;

}
#endif
