#pragma once

#include <vector>

#include "util/types.h"
#include "util/bf_config.h"
#include "lsm/InMemRun.h"
#include "ds/BloomFilter.h"

namespace lsm {

class MemoryLevel {
public:
    MemoryLevel(ssize_t level_no, size_t run_cap)
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

    void append_mem_table(MemTable* memtable, const gsl_rng* rng) {
        assert(m_run_cnt < m_run_cap);
        m_bfs[m_run_cnt] = new BloomFilter(BF_FPR, memtable->get_tombstone_count(), BF_HASH_FUNCS, rng);
        m_runs[m_run_cnt] = new InMemRun(memtable, m_bfs[m_run_cnt]);
        ++m_run_cnt;
    }

    void append_merged_runs(MemoryLevel* level, const gsl_rng* rng) {
        assert(m_run_cnt < m_run_cap);
        m_bfs[m_run_cnt] = new BloomFilter(BF_FPR, level->get_tombstone_count(), BF_HASH_FUNCS, rng);
        m_runs[m_run_cnt] = new InMemRun(level->m_runs, level->m_run_cnt, m_bfs[m_run_cnt]);
        ++m_run_cnt;
    }

    // Append the sample range in-order.....
    void get_sample_ranges(std::vector<SampleRange>& dst, std::vector<size_t>& rec_cnts, const char* low, const char* high) {
        for (ssize_t i = 0; i < m_run_cnt; ++i) {
            auto low_pos = m_runs[i]->get_lower_bound(low);
            auto high_pos = m_runs[i]->get_upper_bound(high);
            assert(high_pos >= low_pos);
            dst.emplace_back(SampleRange{RunId{m_level_no, i}, low_pos, high_pos});
            rec_cnts.emplace_back(high_pos - low_pos);
        }
    }

    bool bf_rejection_check(size_t run_stop, const char* key) {
        for (size_t i = 0; i < run_stop; ++i) {
            if (m_bfs[i] && m_bfs[i]->lookup(key, key_size))
                return true;
        }
        return false;
    }

    bool tombstone_check(size_t run_stop, const char* key, const char* val) {
        for (size_t i = 0; i < run_stop;  ++i) {
            if (m_runs[i] && m_runs[i]->check_tombstone(key, val))
                return true;
        }
        return false;
    }

    const char* get_record_at(size_t run_no, size_t idx) {
        return m_runs[run_no]->get_record_at(idx);
    }
    
    InMemRun* get_run(size_t idx) {
        return m_runs[idx];
    }

    size_t get_run_count() {
        return m_run_cnt;
    }
    
    size_t get_tombstone_count() {
        size_t res = 0;
        for (size_t i = 0; i < m_run_cnt; ++i) {
            res += m_runs[i]->get_tombstone_count();
        }
        return res;
    }

private:
    ssize_t m_level_no;
    size_t m_run_cap;
    size_t m_run_cnt;
    InMemRun** m_runs;
    BloomFilter** m_bfs;

};

}
