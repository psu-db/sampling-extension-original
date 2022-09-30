#pragma once

#include <vector>

#include "util/types.h"
#include "lsm/InMemRun.h"
#include "ds/BloomFilter.h"
#include "lsm/SampleRange.h"

namespace lsm {

class MemoryLevel {
public:
    MemoryLevel(size_t level_no, size_t run_cap)
    : m_level_no(level_no), m_run_cap(run_cap), m_run_cnt(0)
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

    // Append the sample range in-order.....
    void get_sample_ranges(std::vector<SampleRange>& dst, const char* low, const char* high) {
        for (size_t i = 0; i < m_run_cnt; ++i) {
            dst.emplace_back(SampleRange{m_level_no, i, m_runs[i]->get_lower_bound(low), m_runs[i]->get_upper_bound(high)});
        }
    }

    const char* get_record_at(size_t run_no, size_t idx) {
        m_runs[run_no]->get_record_at(idx);
    }
    
    InMemRun* get_run(size_t idx) {
        return m_runs[idx];
    }

    size_t get_run_count() {
        return m_run_cnt;
    }

private:
    size_t m_level_no;
    size_t m_run_cap;
    size_t m_run_cnt;
    InMemRun** m_runs;
    BloomFilter** m_bfs;

};

}
