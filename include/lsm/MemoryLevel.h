#pragma once

#include <vector>

#include "util/types.h"
#include "lsm/MemoryIsamTree.h"

namespace lsm {

class MemoryLevel {
public:
    std::vector<std::pair<RunId, std::pair<const char*, const char*>>> sample_ranges(const char *lower_bound, const char *upper_bound) {
        return std::vector<std::pair<RunId, std::pair<const char*, const char*>>>();
    }

    MemISAMTree *get_run(size_t idx);
};

}
