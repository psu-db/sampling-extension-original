#ifndef TYPES_H
#define TYPES_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>


namespace lsm {

using std::byte;

typedef int32_t PageId;
typedef int16_t SlotId;
typedef uint64_t Timestamp;
typedef int16_t FieldOffset;

const PageId INVALID_PID = 0;
const SlotId INVALID_SID = 0;


}
#endif
