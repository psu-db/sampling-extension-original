#pragma once

#include <vector>

namespace lsm {

class MemoryLevel {
public:
    std::vector<std::pair<const char*, const char*>> sample_ranges(const char *lower_bound, const char *upper_bound) {
        return std::vector<std::pair<const char*, const char*>>();
    }
};

}
