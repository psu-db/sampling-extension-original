/*
 * types.hpp 
 * Douglas Rumbaugh
 *
 * A centralized header file for various datatypes used throughout the
 * code base. There are a few very specific types, such as header formats,
 * that are defined within the header files that make direct use of them,
 * but all generally usable, simple types are defined here.
 */
#ifndef TYPES_H
#define TYPES_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <string>

#include "util/base.hpp"


namespace lsm {

using std::byte;

// Represents a page offset within a specific file (physical or virtual)
typedef uint32_t PageNum;

// Unique numerical identifier for a file, assigned by the file manager
// when it creates files.
typedef uint32_t FileId;

// Byte offset within a page. Also used for lengths of records, etc.,
// within the codebase. size_t isn't necessary, as the maximum offset
// is only parm::PAGE_SIZE 
typedef uint16_t PageOffset;

// Slot offset within a page, be it fixed length of variable length.
// The FixedLenDataPage/VarLenDataPage classes translate from SlotIds
// into PageOffsets automatically.
typedef uint16_t SlotId;

// Unique identifier for a page within the codebase: includes both the
// page number, and the file id, so the file manager, buffer manager,
// read cache, etc., can determine to which file the page belongs.
struct PageId {
    PageNum page_number;
    FileId file_id;

    friend bool operator==(const PageId& pid, const PageId &other_pid) {
        return pid.file_id == other_pid.file_id && pid.page_number == other_pid.page_number;
    }
};

// Hash function for PageIds required to use these as a key in hash maps,
// such as in the ReadCache.
struct PageIdHash {
    size_t operator()(const PageId &pid) const {
        return pid.page_number ^ pid.file_id;
    }
};

// Aligned size of a PageId, for use when writing it into a buffer
// or file. Theoretically, should be the same as the unaligned size
// as the moment (8 bytes).
constexpr size_t PageIdSize = MAXALIGN(sizeof(PageId));


// Unique identifier for a virtual file, including both the id of the
// underlying physical file, as well as the virtual file id within that file.
// Note that the virtual file IDs will be unique within a given physical file,
// but will not be globally unique.
struct VirtualFileId {
    FileId phys_flid;
    FileId virt_flid;
};

// A unique identifier for a record. Not currently used for
// anything.
struct RecordId {
    PageId pid;
    PageOffset offset;
};

// Aligned size of a RecordID, for use when writing it into a buffer
// or file. 
constexpr size_t RecordIdSize = MAXALIGN(sizeof(RecordId));

// A unique identifier for a frame within a buffer or cache.
typedef uint32_t FrameId;

// A unique timestamp for use in MVCC concurrency control. Currently stored in
// record headers, but not used by anything.
typedef uint64_t Timestamp;

// Invalid values for various IDs. Used throughout the code base to indicate
// uninitialized values and error conditions.
const PageNum INVALID_PNUM = 0;
const FileId INVALID_FLID = 0;
const FrameId INVALID_FRID = 0;
const PageId INVALID_PID = {0, 0};
const SlotId INVALID_SID = 0;

}
#endif
