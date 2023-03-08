#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <numeric>

#include "util/base.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "util/record.h"
#include "ds/Alias.h"
#include  "util/timer.h"

namespace lsm {


class MemTable {
public:
    MemTable(size_t capacity, bool rej_sampling, size_t max_tombstone_cap, const gsl_rng* rng)
    : m_cap(capacity), m_tombstone_cap(max_tombstone_cap), m_buffersize(capacity * record_size), m_reccnt(0)
    , m_tombstonecnt(0), m_current_tail(0), m_weight(0), m_max_weight(0) {
        size_t aligned_buffersize = m_buffersize + (CACHELINE_SIZE - (m_buffersize % CACHELINE_SIZE));
        m_data = (char*) std::aligned_alloc(CACHELINE_SIZE, aligned_buffersize);
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

    int append(const char* key, const char* value, double weight=1.0, bool is_tombstone = false) {
        if (is_tombstone && m_tombstonecnt + 1 > m_tombstone_cap) return 0;

        ssize_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;


        layout_memtable_record(m_data + pos, key, value, is_tombstone, (uint32_t)pos / record_size, weight);
        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key, key_size);
        }
        m_reccnt.fetch_add(1);

        double old_val, new_val;
        do {
            old_val = m_weight.load();
            new_val = old_val + weight;
        } while (!m_weight.compare_exchange_strong(old_val, new_val));


        double old = m_max_weight.load();
        while (old < weight) {
            m_max_weight.compare_exchange_strong(old, weight);
            old = m_max_weight.load();
        }

        return 1;     
    }

    bool truncate() {
        m_current_tail.store(0);
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        m_weight.store(0);
        m_max_weight.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    char* sorted_output() {
        TIMER_INIT();
        TIMER_START();
        qsort(m_data, m_reccnt.load(), record_size, memtable_record_cmp);
        TIMER_STOP();

        #ifdef INSTRUMENT_MERGING
        fprintf(stderr, "sort\t%ld\n", TIMER_RESULT());
        #endif
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

    bool delete_record(const char *key, const char *val) {
        auto offset = 0;
        while (offset < m_current_tail) {
            if (record_match(m_data + offset, key, val, false)) {
                set_delete_status(m_data + offset);
                return true;
            }
            offset += record_size;
        }

        return false;
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

    // NOTE: This operation samples from records strictly between the upper and
    // lower bounds, not including them
    double get_sample_range(Alias **alias, size_t *cutoff) {
      std::vector<double> weights;

      *cutoff = std::atomic_load(&m_reccnt) - 1;
      for (size_t i = 0; i < (*cutoff) + 1; i++) {
        char *rec = m_data + (i * record_size);

        if (!is_tombstone(rec)) {
          weights.push_back(get_weight(rec));
        }
      }

      double total_weight = std::accumulate(weights.begin(), weights.end(), 0);
      for (size_t i = 0; i < weights.size(); i++) {
        weights[i] = weights[i] / total_weight;
      }

      *alias = new Alias(weights);

      return total_weight;
    }

    // rejection sampling
    const char *get_sample(gsl_rng *rng) {
        size_t reccnt = m_reccnt.load();
        if (reccnt == 0) {
            return nullptr;
        }

        auto idx = (reccnt == 1) ? 0 : gsl_rng_uniform_int(rng, reccnt - 1);
        auto rec = get_record_at(idx);

        auto test = gsl_rng_uniform(rng) * m_max_weight.load();

        return (test <= get_weight(rec)) ? rec : nullptr;
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

    double get_total_weight() {
        return m_weight.load();
    }

private:
    ssize_t try_advance_tail() {
        size_t new_tail = m_current_tail.fetch_add(record_size);

        if (new_tail < m_buffersize) return new_tail;
        else return -1;
    }

    size_t m_cap;
    size_t m_buffersize;
    size_t m_tombstone_cap;
    
    char* m_data;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_current_tail;
    alignas(64) std::atomic<size_t> m_reccnt;
    alignas(64) std::atomic<double> m_weight;
    alignas(64) std::atomic<double> m_max_weight;
};

}
