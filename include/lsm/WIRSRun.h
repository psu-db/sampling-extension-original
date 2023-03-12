#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "lsm/MemTableBTree.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "util/timer.h"
#include "ds/Alias.h"
#include "util/record.h"

namespace lsm {

struct sample_state;
bool check_deleted(const char *record, sample_state *state);

thread_local size_t m_wirsrun_cancelations = 0;

class WIRSRun {
public:

    WIRSRun(MemTable* mem_table, BloomFilter* bf, bool tagging)
    : m_reccnt(0), m_tombstone_cnt(0), m_rejection_cnt(0), m_ts_check_cnt(0), m_deleted_cnt(0), m_total_weight(0), m_tagging(tagging) {

        std::vector<double> weights;
        weights.reserve(mem_table->get_record_count());
        size_t alloc_size = (mem_table->get_record_count() * record_size) + (CACHELINE_SIZE - (mem_table->get_record_count() * record_size) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        auto base = mem_table->begin();
        auto stop = mem_table->end();

        TIMER_INIT();
        TIMER_START();

        while (base != stop) {
            if (!m_tagging) {
                if (!(base->flags & 0x1)) {
                    auto next = std::next(base);
                    if (next->key == base->key && next->val == base->val && next->flags & 0x1) {
                        m_wirsrun_cancelations++;
                        base++;
                        base++;
                        continue;
                    }
                }
            }

            layout_record(m_data + offset, (char*) &base->key, (char*) &base->val, base->flags & 0x1, base->weight);
            offset += record_size;
            m_reccnt++;
            m_total_weight += base->weight;
            weights.push_back((double) base->weight);
            if (base->flags & 0x1) {
                m_tombstone_cnt++;
                bf->insert((char*) &base->key, key_size);
            }
            
            base++;
        }
        TIMER_STOP();

        auto merge_time = TIMER_RESULT();

        TIMER_START();
        // normalize the weights array
        for (size_t i=0; i<weights.size(); i++) {
            weights[i] = (double) weights[i] / (double) m_total_weight;
        }

        // build the alias structure
        m_alias = new Alias(weights);
        TIMER_STOP();

        auto alias_time = TIMER_RESULT();

        #ifdef INSTRUMENT_MERGING
        fprintf(stderr, "memtable\t%ld\t%ld\n", merge_time, alias_time);
        #endif
    }

    WIRSRun(WIRSRun** runs, size_t len, BloomFilter* bf, bool tagging)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_rejection_cnt(0), m_ts_check_cnt(0), m_deleted_cnt(0), m_tagging(tagging) {
        std::vector<Cursor> cursors;
        std::vector<double> weights;
        cursors.reserve(len);

        size_t attemp_reccnt = 0;
        
        for (size_t i = 0; i < len; ++i) {
            //assert(runs[i]);
            if (runs[i]) {
                auto base = runs[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count() * record_size, 0, runs[i]->get_record_count()});
                attemp_reccnt += runs[i]->get_record_count();
            } else {
                cursors.emplace_back(g_empty_cursor);
            }
        }

        size_t alloc_size = (attemp_reccnt * record_size) + (CACHELINE_SIZE - (attemp_reccnt * record_size) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);
        weights.reserve(attemp_reccnt);

        size_t offset = 0;

        size_t reccnt = 0;
        Cursor *next = get_next(cursors);
        do {
            Cursor *current = next;
            next = get_next(cursors, current); //something;

            // Handle tombstone cancellation case if record tagging is not
            // enabled
            if (!m_tagging && !is_tombstone(current->ptr) && current != next &&
                next->ptr < next->end && !record_cmp(current->ptr, next->ptr) && 
                is_tombstone(next->ptr)) {
                
                reccnt += 2;
                advance_cursor(current);
                advance_cursor(next);
                next = get_next(cursors);
                continue;
            }

            // If the record is tagged as deleted, we can drop it
            if (get_delete_status(current->ptr)) {
                advance_cursor(current);
                reccnt += 1;
                continue;
            }

            // If the current record is a tombstone and tagging isn't in
            // use, add it to the Bloom filter and increment the tombstone
            // counter.
            if (!m_tagging && is_tombstone(current->ptr)) {
                ++m_tombstone_cnt;
                bf->insert(get_key(current->ptr), key_size);
            }

            // Copy the record into the new run and increment the associated counters
            memcpy(m_data + offset, current->ptr, record_size);
            offset += record_size;
            ++m_reccnt;
            m_total_weight += get_weight(current->ptr);
            weights.push_back((double) get_weight(current->ptr));
            advance_cursor(current);
            reccnt += 1;
        } while (reccnt < attemp_reccnt);


        /*
        if (m_tagging) {
            while (pq.size()) {
                auto now = pq.peek(); pq.pop();
                auto &cur = cursors[now.version];
                if (get_delete_status(now.data)) {
                    if (advance_cursor(cur)) pq.push(cur.ptr, now.version);
                } else {
                    memcpy(m_data + offset, cur.ptr, record_size);
                    offset += record_size;
                    ++m_reccnt;
                    m_total_weight += get_weight(cur.ptr);
                    weights.push_back((double) get_weight(cur.ptr));
                    if (advance_cursor(cur)) pq.push(cur.ptr, now.version);
                }
            }
        } else {
            while (pq.size()) {
                auto now = pq.peek();
                auto next = pq.size() > 1 ? pq.peek(1) : queue_record{nullptr, 0};
                if (!is_tombstone(now.data) && next.data != nullptr &&
                    !record_cmp(now.data, next.data) && is_tombstone(next.data)) {
                    
                    pq.pop(); pq.pop();
                    auto& cursor1 = cursors[now.version];
                    auto& cursor2 = cursors[next.version];
                    if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                    if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
                } else {
                    auto& cursor = cursors[now.version];
                    memcpy(m_data + offset, cursor.ptr, record_size);
                    if (is_tombstone(cursor.ptr)) {
                        ++m_tombstone_cnt;
                        bf->insert(get_key(cursor.ptr), key_size);
                    }
                    offset += record_size;
                    ++m_reccnt;
                    m_total_weight += get_weight(cursor.ptr);
                    weights.push_back((double) get_weight(cursor.ptr));
                    pq.pop();
                    
                    if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
                }
            }
        }
    */
        
        // normalize the weights array
        for (size_t i=0; i<weights.size(); i++) {
            weights[i] = weights[i] / (double) m_total_weight;
        }

        // build the alias structure
        m_alias = new Alias(weights);
   }

    ~WIRSRun() {
        if (m_data) free(m_data);
        if (m_alias) delete m_alias;
    }


    bool delete_record(const char *key, const char *val) {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        auto ptr = m_data + (get_lower_bound(key) * record_size);

        char buf[record_size];
        layout_record(buf, key, val, false, 0.0);
        while (ptr < m_data + m_reccnt * record_size && record_cmp(ptr, buf) == -1) {
            ptr += record_size;
        }

        if (record_match(ptr, key, val, false)) {
            set_delete_status(ptr);
            m_deleted_cnt++;
            return true;
        }

        return false;
    }

    char* sorted_output() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const char* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx * record_size;
    }

    //
    // returns the number of records sampled
    // NOTE: This operation returns records strictly between the lower and upper bounds, not
    // including them.
    size_t get_samples(char *sample_set, size_t sample_sz, sample_state *state, gsl_rng *rng) {
        if (sample_sz == 0) {
            return 0;
        }

        size_t sampled_cnt=0;
        for (size_t i=0; i<sample_sz; i++) {
            size_t idx = m_alias->get(rng);
            const char *rec = this->get_record_at(idx);
            if (m_tagging) {
                if (get_delete_status(rec)) {
                    m_rejection_cnt++;
                    continue;
                }
            } else if (state) {
                if (check_deleted(rec, state)) {
                    m_rejection_cnt++;
                    continue;
                }
            }

            memcpy (sample_set + record_size*sampled_cnt++, rec, record_size);
        }

        return sampled_cnt;
    }

    size_t get_lower_bound(const char *key) const {
        size_t min = 0;
        size_t max = m_reccnt - 1;

        const char * record_key;
        while (min < max) {
            size_t mid = (min + max) / 2;
            record_key = get_key(m_data + (mid * record_size));

            if (key_cmp(key, record_key) > 0) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return min;
    }

    bool check_tombstone(const char* key, const char* val) {
        m_ts_check_cnt += 1;

        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        auto ptr = m_data + (get_lower_bound(key) * record_size);

        char buf[record_size];
        layout_record(buf, key, val, false, 0.0);
        while (ptr < m_data + m_reccnt * record_size && record_cmp(ptr, buf) == -1) {
            ptr += record_size;
        }

        bool result = record_match(ptr, key, val, true);
        if (result) {
            m_rejection_cnt++;
        }
        return result;
    }


    size_t get_memory_utilization() {
        return 0;
    }


    weight_type get_total_weight() {
        return m_total_weight;
    }

    size_t get_rejection_count() {
        return m_rejection_cnt;
    }

    size_t get_ts_check_count() {
        return m_ts_check_cnt;
    }

    size_t get_deleted_count() {
        assert(m_tagging);
        return m_deleted_cnt;
    }
    
private:
    char* m_data;
    Alias *m_alias;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_deleted_cnt;
    weight_type m_total_weight;

    // The number of rejections caused by tombstones
    // in this WIRSRun.
    size_t m_rejection_cnt;
    size_t m_ts_check_cnt;

    bool m_tagging;
};

}
