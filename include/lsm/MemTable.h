#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <vector>
#include <algorithm>

#include "util/base.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "util/record.h"

namespace lsm {

class MemTable {
public:
    MemTable(size_t capacity, bool rej_sampling, size_t max_tombstone_cap, const gsl_rng* rng)
    : m_cap(capacity), m_tombstone_cap(max_tombstone_cap)
    , m_reccnt(0), m_tombstonecnt(0) {
        auto len = capacity * sizeof(record_t);
        size_t aligned_buffersize = len + (CACHELINE_SIZE - (len %  CACHELINE_SIZE));
        m_data = (record_t*) std::aligned_alloc(CACHELINE_SIZE, aligned_buffersize);
        m_tombstone_filter = nullptr;
        if (max_tombstone_cap > 0) {
            assert(rng != nullptr);
            m_tombstone_filter = new BloomFilter(BF_FPR, max_tombstone_cap, BF_HASH_FUNCS, rng);
        }
    }

    ~MemTable() {
        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
    }

    int append(const key_t& key, const value_t& value, bool is_tombstone = false) {
        if (is_tombstone && m_tombstonecnt + 1 > m_tombstone_cap) return 0;

        int32_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        m_data[pos].key = key;
        m_data[pos].value = value;
        m_data[pos].header = ((pos << 2) | (is_tombstone ? 1 : 0));
        
        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key);
        }
        //m_reccnt.fetch_add(1);

        return 1;     
    }

    bool truncate() {
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    record_t* sorted_output() {
        std::sort(m_data, m_data + m_reccnt.load(), memtable_record_cmp);
        return m_data;
    }
    
    size_t get_record_count() {
        return m_reccnt;
    }
    
    size_t get_capacity() {
        return m_cap;
    }

    bool is_full() {
        return m_reccnt == m_cap;
    }

    size_t get_tombstone_count() {
        return m_tombstonecnt.load();
    }

    bool delete_record(const key_t& key, const value_t& val) {
        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].match(key, val, false)) {
                m_data[offset].set_delete_status();
                return true;
            }
            offset++;
        }
        return false;
    }

    bool check_tombstone(const key_t& key, const value_t& value) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(key)) return false;

        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].match(key, value, true)) return true;
            offset++;
        }
        return false;
    }

    void create_sampling_vector(const key_t& min, const key_t& max, std::vector<const record_t*> &records) {
        records.clear();
        for (size_t i=0; i<m_reccnt.load(); i++) {
            auto rec = this->get_record_at(i);
            auto key = rec->key;
            if (min <= key && key <= max) {
                records.push_back(rec);
            }
        }
    }

    const record_t* get_record_at(size_t idx) {
        return m_data + idx;
    }

    size_t get_memory_utilization() {
        return m_cap * sizeof(record_t);
    }

    size_t get_aux_memory_utilization() {
        return m_tombstone_filter->get_memory_utilization();
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

private:
    int32_t try_advance_tail() {
        size_t new_tail = m_reccnt.fetch_add(1);

        if (new_tail < m_cap) return new_tail;
        else return -1;
    }

    size_t m_cap;
    //size_t m_buffersize;
    size_t m_tombstone_cap;
    
    record_t* m_data;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    //alignas(64) std::atomic<size_t> m_current_tail;
    alignas(64) std::atomic<size_t> m_reccnt;
};

}
