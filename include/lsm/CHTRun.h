#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "lsm/MemTable.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "util/timer.h"
#include "ds/ts/builder.h"


namespace lsm {

class CHTRun  {
public:

    CHTRun(MemTable* mem_table, BloomFilter* bf, bool tagging)
    :m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_tagging(tagging) {

        size_t alloc_size = (mem_table->get_record_count() * sizeof(record_t)) + (CACHELINE_SIZE - (mem_table->get_record_count() * sizeof(record_t)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (record_t*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        TIMER_INIT();

        size_t max_error = 16;
        auto bldr = ts::Builder<key_t>(mem_table->get_min_key(), mem_table->get_max_key(), max_error);

        size_t offset = 0;
        m_reccnt = 0;
        TIMER_START();
        record_t* base = mem_table->sorted_output();
        TIMER_STOP();

        auto sort_time = TIMER_RESULT();
        record_t* stop = base + mem_table->get_record_count();

        TIMER_START();
        while (base < stop) {
            if (!m_tagging) {
                if (!base->is_tombstone() && (base + 1 < stop)
                    && base->match(base + 1) && (base + 1)->is_tombstone()) {
                    base += 2;
                    continue;
                } 
            } else if (base->get_delete_status()) {
                base += 1;
                continue;
            }

            if (m_reccnt == 0) {
                m_max_key = m_min_key = base->key;
            } else if (base->key > m_max_key) {
                m_max_key = base->key;
            } else if (base->key < m_min_key) {
                m_min_key = base->key;
            }

            //Masking off the ts.
            base->header &= 1;
            m_data[m_reccnt++] = *base;
            bldr.AddKey(base->key);
            if (bf && base->is_tombstone()) {
                ++m_tombstone_cnt;
                bf->insert(base->key);
            }

            base++;
        }
        TIMER_STOP();
        auto copy_time = TIMER_RESULT();

        TIMER_START();
        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
        TIMER_STOP();
        auto level_time = TIMER_RESULT();
    }

    CHTRun(CHTRun** runs, size_t len, BloomFilter* bf, bool tagging)
    :m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_tagging(tagging) {
        std::vector<Cursor> cursors;
        cursors.reserve(len);

        PriorityQueue pq(len);

        size_t attemp_reccnt = 0;

        for (size_t i = 0; i < len; ++i) {
            if (runs[i]) {
                auto base = runs[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count(), 0, runs[i]->get_record_count()});
                attemp_reccnt += runs[i]->get_record_count();
                pq.push(cursors[i].ptr, i);

                if (i == 0) {
                    m_max_key = runs[i]->get_max_key();
                    m_min_key = runs[i]->get_min_key();
                } else if (runs[i]->get_max_key() > m_max_key) {
                    m_max_key = runs[i]->get_max_key();
                } else if (runs[i]->get_min_key() < m_min_key) {
                    m_min_key = runs[i]->get_min_key();
                }
            } else {
                cursors.emplace_back(Cursor{nullptr, nullptr, 0, 0});
            }
        }


        size_t alloc_size = (attemp_reccnt * sizeof(record_t)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(record_t)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (record_t*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);
        size_t max_error = 8;
        auto bldr = ts::Builder<key_t>(m_min_key, m_max_key, max_error);

        size_t offset = 0;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record{nullptr, 0};
            if (!m_tagging && !now.data->is_tombstone() && next.data != nullptr &&
                now.data->match(next.data) && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!m_tagging || !cursor.ptr->get_delete_status()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    bldr.AddKey(cursor.ptr->key);
                    if (cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        bf->insert(cursor.ptr->key);
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
    }

    ~CHTRun() {
        if (m_data) free(m_data);
    }


    record_t* sorted_output() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    bool delete_record(const key_t& key, const value_t& val) {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        while (idx < m_reccnt && m_data[idx].lt(key, val)) ++idx;

        if (m_data[idx].match(key, val, false)) {
            m_data[idx].set_delete_status();
            m_deleted_cnt++;
            return true;
        }

        return false;
    }

    const record_t* get_record_at(size_t idx) const {
        return (idx < m_reccnt) ? m_data + idx : nullptr;
    }

    size_t get_lower_bound(const key_t& key) const {
        auto bound = m_ts.GetSearchBound(key);
        size_t idx = bound.begin;

        if (idx >= m_reccnt) {
            return m_reccnt;
        }

        // if the found location is larger than the key, we need to
        // move backwards towards the beginning of the array
        if (m_data[idx].key > key) {
            for (ssize_t i=idx; i>=0; i--) {
                if (m_data[i].key < key) {
                    return i+1;
                }
            }
        // otherwise, we move forward towards the end
        } else {
            for (size_t i=idx; i<m_reccnt; i++) {
                if (m_data[i].key >= key) {
                    return i-1;
                }
            }
        }

        return m_reccnt;
    }

    size_t get_upper_bound(const key_t& key) const {
        auto bound = m_ts.GetSearchBound(key);
        size_t idx = bound.begin;

        // if the found location is larger than the key, we need to
        // move backwards towards the beginning of the array
        if (m_data[idx].key > key) {
            for (ssize_t i=idx; i>=0; i--) {
                if (m_data[i].key <= key) {
                    return i+1;
                }
            }
        // otherwise, we move forward towards the end
        } else {
            for (size_t i=idx; i<m_reccnt; i++) {
                if (m_data[i].key > key) {
                    return i-1;
                }
            }
        }

        return 0;
    }

    bool check_tombstone(const key_t& key, const value_t& val) const {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        record_t* ptr = m_data + idx;

        while (ptr < m_data + m_reccnt && ptr->lt(key, val)) ptr++;
        return ptr->match(key, val, true);
    }

    size_t get_memory_utilization() {
        return m_ts.GetSize();
    }

    key_t get_max_key() {
        return m_max_key;
    }

    key_t get_min_key() {
        return m_min_key;
    }

private:
    // Members: sorted data, internal ISAM levels, reccnt;
    record_t* m_data;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_internal_node_cnt;
    size_t m_deleted_cnt;
    key_t m_max_key;
    key_t m_min_key;
    bool m_tagging;
    ts::TrieSpline<key_t> m_ts;
};

}
