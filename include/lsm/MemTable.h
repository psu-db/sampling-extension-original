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

        m_refcnt.store(0);
        m_deferred_truncate.store(false);
        m_merging.store(false);
    }

    ~MemTable() {
        assert(m_refcnt.load() == 0);
        assert(m_merging.load() == false);
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
        if (!m_merging.load()) {
            *truncation_complete = false;
            return false;
        }

        if (m_refcnt.load() > 0) {
            *truncation_complete = false;
            this->truncation_signaller = truncation_complete;

            m_deferred_truncate.store(true);
            return true;
        }

        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        // Update the tail to allow inserts to succeed again.
        m_current_tail.store(0); 

        m_merging.store(false);

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
        memcpy(m_sorted_data, m_data, record_size * (m_reccnt.load()));
        qsort(m_sorted_data, m_reccnt.load(), record_size, memtable_record_cmp);
        return m_sorted_data;
    }

    char *start_merge() {
        if (m_merge_lock.try_lock()) {
            // Only allow entry to a merge state when the memtable
            // is full. 
            if (this->get_record_count() < this->get_capacity()) {
                m_merge_lock.unlock();
                return nullptr;
            }

            m_merging.store(true);
            return this->sorted_output();
        }

        return nullptr;
    }

    bool pin() {
        // NOTE: I've removed the block on acquiring pins here,
        // but this *could* result in starvation of the merge thread, 
        // in principle. It might be better to have the sample threads
        // drop their version pin and try again.
        m_refcnt.fetch_add(1);
        return true;
    }

    bool unpin() {
        m_refcnt.fetch_add(-1);

        if (m_refcnt.load() == 0 && m_deferred_truncate.load()) {
            assert(this->truncate(this->truncation_signaller));
        }

        return true;
    }

    bool merging() {
        return m_merging.load();
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

    std::atomic<size_t> m_refcnt;

    std::atomic<bool> m_merging;
    std::atomic<bool> m_deferred_truncate;

    char* m_data;
    char* m_sorted_data;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_current_tail;
    alignas(64) std::atomic<size_t> m_reccnt;
};

class MemTableView {
public:
    static MemTableView *create(std::vector<MemTable*> &tables) {
        std::vector<MemTable *> pinned_tables;
        std::vector<size_t> table_cutoffs;
        // If a pin fails, then a version switch has just happened
        // emptying a table, and so we should bail out and try again
        for (size_t i=0; i<tables.size(); i++) {
            if (!tables[i]->pin()) {
                for (size_t j=0; j<pinned_tables.size(); j++) {
                    pinned_tables[i]->unpin();
                }

                return nullptr;
            } else {
                // don't use any empty memtables
                if (tables[i]->get_record_count() == 0) {
                    tables[i]->unpin();
                } else {
                    pinned_tables.push_back(tables[i]);
                    table_cutoffs.push_back(tables[i]->get_record_count());
                }
            }
        }

        return new MemTableView(pinned_tables, table_cutoffs);
    }

    ~MemTableView() {
        for (size_t i=0; i<m_tables.size(); i++) {
            m_tables[i]->unpin();
        }
    }

    size_t get_record_count() const {
        size_t cnt = 0;
        for (size_t i=0; i<m_cutoffs.size(); i++) {
           cnt += m_cutoffs[i]; 
        }

        return cnt;
    }

    const char *get_record_at(size_t idx) const {
        assert(idx < this->get_record_count());

        size_t i =0;
        ssize_t t_idx = idx;
        while (t_idx >= m_cutoffs[i]) {
            t_idx -= m_cutoffs[i];
            i++;
        }

        assert(i < m_tables.size());
        assert(t_idx >= 0 && t_idx < m_cutoffs[i]);

        return m_tables[i]->get_record_at(t_idx);
    }

    bool check_tombstone(const char *key, const char *val) const {
        for (size_t i=0; i<m_tables.size(); i++) {

        }

        return false;
    }

private:
    std::vector<MemTable *> m_tables;
    std::vector<size_t> m_cutoffs;
    MemTableView(std::vector<MemTable *> &tables, std::vector<size_t> &cutoffs) : m_tables(std::move(tables)), m_cutoffs(std::move(cutoffs)) {}
};

}
