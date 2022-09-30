#pragma once

#include <vector>

#include "util/types.h"
#include "lsm/InMemRun.h"
#include "ds/BloomFilter.h"

namespace lsm {

class MemoryLevel {
public:
    MemoryLevel(size_t run_cap)
    : m_run_cap(run_cap), m_run_cnt(0)
    , m_runs(new InMemRun*[run_cap]{nullptr})
    , m_bfs(new BloomFilter*[run_cap]{nullptr}) {}

    ~MemoryLevel() {
        for (size_t i = 0; i < m_run_cap; ++i) {
            if (m_runs[i]) delete m_runs[i];
            if (m_bfs[i]) delete m_bfs[i];
        }

        delete[] m_runs;
        delete[] m_bfs;
    }

    std::vector<std::pair<RunId, std::pair<const char*, const char*>>> sample_ranges(const char *lower_bound, const char *upper_bound) {
        return std::vector<std::pair<RunId, std::pair<const char*, const char*>>>();
    }

    InMemRun* get_run(size_t idx) {
        return m_runs[idx];
    }

    size_t get_run_count() {
        return m_run_cnt;
    }

private:
    size_t m_run_cap;
    size_t m_run_cnt;
    InMemRun** m_runs;
    BloomFilter** m_bfs;

};

}
