#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>

#include "util/base.h"
#include "ds/BloomFilter.h"
#include "util/record.h"

namespace lsm {

class MemTable {
public:
    MemTable(size_t capacity, bool rej_sampling, size_t filter_size, const gsl_rng* rng)
    : m_cap(capacity), m_buffersize(capacity * record_size), m_reccnt(0), m_sorted(false)
    , m_tombstonecnt(0), m_current_tail(0) {
        m_data = (char*) std::aligned_alloc(CACHELINE_SIZE, m_buffersize);
        m_tombstone_filter = nullptr;
        if (filter_size > 0) {
            assert(rng != nullptr);
            m_tombstone_filter = new BloomFilter(filter_size, 8, rng);
        }
    }

    int append(const char* key, const char* value, bool is_tombstone = false) {
        ssize_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        layout_memtable_record(m_data + pos, key, value, is_tombstone, (uint32_t)(pos / record_size));
        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key, key_size);
        }
        m_reccnt.fetch_add(1);

        return 1;     
    }

    bool truncate() {
        m_current_tail.store(0);
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    char* sorted_output() {
        if (!m_sorted) {
            m_sorted = true;
            qsort(m_data, m_reccnt.load(), record_size, memtable_record_cmp);
        }
        
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

    bool check_tombstone(const char* key, const char* value) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(key, key_size)) return false;

        auto offset = 0;
        while (offset < m_current_tail) {
            if (record_match(m_data + offset, key, value, true)) return true;
            offset += record_size;
        }
        return false;
    }

private:
    ssize_t try_advance_tail() {
        size_t new_tail = m_current_tail.fetch_add(record_size);

        if (new_tail < m_buffersize) return new_tail;
        else return -1;
    }

    size_t m_cap;
    size_t m_buffersize;
    
    char* m_data;
    BloomFilter* m_tombstone_filter;
    bool m_sorted;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_current_tail;
    alignas(64) std::atomic<size_t> m_reccnt;
};

}
