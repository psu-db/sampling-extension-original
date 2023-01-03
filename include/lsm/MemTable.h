#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <mutex>
#include <vector>

#include "util/base.h"
#include "ds/BloomFilter.h"
#include "util/record.h"

namespace lsm {

class MemTable {
public:
    MemTable(size_t capacity, bool rej_sampling, size_t filter_size, const gsl_rng* rng)
    : m_cap(capacity), m_buffersize(capacity * record_size), m_reccnt(0)
    , m_tombstonecnt(0), m_current_tail(0) {
        m_buffersize += (CACHELINE_SIZE - (CACHELINE_SIZE % m_buffersize));
        m_data = (char*) std::aligned_alloc(CACHELINE_SIZE, m_buffersize);
        m_sorted_data = (char*) std::aligned_alloc(CACHELINE_SIZE, m_buffersize);
        m_tombstone_filter = nullptr;
        if (filter_size > 0) {
            assert(rng != nullptr);
            m_tombstone_filter = new BloomFilter(filter_size, 8, rng);
        }

        std::atomic_store(&m_refcnt, 0);
        std::atomic_store(&m_deferred_truncate, false);
        std::atomic_store(&m_merging, false);
    }

    ~MemTable() {
        assert(std::atomic_load(&m_refcnt) == 0);
        assert(std::atomic_load(&m_merging) == false);
        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
        if (m_sorted_data) free(m_sorted_data);
    }

    int append(const char* key, const char* value, bool is_tombstone = false) {
        ssize_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        layout_memtable_record(m_data + pos, key, value, is_tombstone, (uint32_t)pos / record_size);
        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key, key_size);
        }
        m_reccnt.fetch_add(1);

        return 1;     
    }

    bool truncate(bool *truncation_complete) {
        if (!m_merging) {
            *truncation_complete = false;
            return false;
        }

        if (std::atomic_load(&m_refcnt) > 0) {
            *truncation_complete = false;
            this->truncation_signaller = truncation_complete;

            m_deferred_truncate = true;
            return true;
        }

        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        // Update the tail to allow inserts to succeed again.
        m_current_tail.store(0); 

        std::atomic_store(&m_merging, false);

        *truncation_complete = true;
        this->truncation_signaller = nullptr;

        m_merge_lock.unlock();

        return true;
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

    const char *get_record_at(size_t idx) {
      return m_data + (record_size * idx);
    }

    size_t get_memory_utilization() {
        return m_buffersize;
    }

    size_t get_aux_memory_utilization() {
        return m_tombstone_filter->get_memory_utilization();
    }

    char* sorted_output() {
        memset(m_sorted_data, 0, m_buffersize);
        memcpy(m_sorted_data, m_data, record_size * (std::atomic_load(&m_reccnt)));
        qsort(m_sorted_data, m_reccnt.load(), record_size, memtable_record_cmp);
        return m_sorted_data;
    }

    char *start_merge() {
        if (m_merge_lock.try_lock()) {
            m_merging = true;
            return this->sorted_output();
        }

        return nullptr;
    }

    bool pin() {
        // FIXME: Even if a table is merging, it may still be accessed
        // by a sampling operation. I don't think this is actually a 
        // valid condition.
        if (m_merging == true) {
            return false;
        }

        std::atomic_fetch_add(&m_refcnt, 1);
        return true;
    }

    bool unpin() {
        std::atomic_fetch_add(&m_refcnt, -1);

        if (std::atomic_load(&m_refcnt) == 0 && m_deferred_truncate) {
            assert(this->truncate(this->truncation_signaller));
        }

        return true;
    }

private:
    ssize_t try_advance_tail() {
        size_t new_tail = m_current_tail.fetch_add(record_size);

        if (new_tail < m_buffersize) return new_tail;
        else return -1;
    }


    size_t m_cap;
    size_t m_buffersize;

    bool *truncation_signaller;

    std::mutex m_merge_lock;

    std::atomic_uint64_t m_refcnt;

    std::atomic_bool m_merging;
    std::atomic_bool m_deferred_truncate;

    char* m_data;
    char* m_sorted_data;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_current_tail;
    alignas(64) std::atomic<size_t> m_reccnt;
};

class MemTableView {
public:
    static MemTableView *create(std::vector<MemTable*> &tables, size_t table_cnt) {
        std::vector<MemTable *> pinned_tables;
        // If a pin fails, then a version switch has just happened
        // emptying a table, and so we should bail out and try again
        for (size_t i=0; i<table_cnt; i++) {
            if (!tables[i]->pin()) {
                return nullptr;
            }

            // don't use any empty memtables
            if (tables[i]->get_record_count() == 0) {
                tables[i]->unpin();
            } else {
                pinned_tables.push_back(tables[i]);
            }
        }

        return nullptr;
    }

    ~MemTableView() {
        for (size_t i=0; i<m_tables.size(); i++) {
            m_tables[i]->unpin();
        }
    }

    size_t get_record_count() const {
        size_t cnt = 0;
        for (size_t i=0; i<m_tables.size(); i++) {
           cnt += m_tables[i]->get_record_count(); 
        }
        return 0;
    }

    const char *get_record_at(size_t idx) const {
        size_t cnt = 0;
        size_t i;
        while (idx > cnt + m_tables[i]->get_record_count()) {
            i++;
        }

        size_t t_idx = idx - cnt;
        return m_tables[i]->get_record_at(t_idx);
    }

    bool check_tombstone(const char *key, const char *val) const {
        for (size_t i=0; i<m_tables.size(); i++) {

        }

        return false;
    }

private:
    std::vector<MemTable *> m_tables;
    MemTableView(std::vector<MemTable *> tables) : m_tables(tables) {}
};

}
