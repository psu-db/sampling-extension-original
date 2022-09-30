#pragma once

#include <cstdlib>

namespace lsm {

struct SampleRange {
    size_t level, run;
    size_t lower, upper;
};

}