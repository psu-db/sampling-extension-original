#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <numeric>

#include "util/base.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "ds/BTree.h"
#include "util/record.h"
#include "ds/Alias.h"
#include "util/timer.h"

namespace lsm {

struct btkey {
    key_type key;
    value_type val;
    weight_type weight;
    rec_hdr flags;

    friend bool operator<=(const btkey &a, const btkey &b) {
        return a.key <= b.key;
    }

    friend bool operator >=(const btkey &a, const btkey &b) {
        return a.key >= b.key;
    }
};

struct btrec_key {
    static const btkey &get(const btkey &v) {
        return v;
    }
};

struct btkey_cmp {
    bool operator()(const btkey& first, const btkey& second) const {
        int cmp = key_cmp((char*) &first.key, (char*) &second.key);

        if (cmp == 0) {
            int cmp2 = val_cmp((char*) &first.val, (char*) &second.val);
            if (cmp2 == 0) {
                if (first.flags < second.flags) return true;
            else return false;
            } else return (cmp2 == -1);
        } else return (cmp == -1);
    }
};

static btkey_cmp cmptor;


typedef tlx::BTree<btkey, btkey, btrec_key, btkey_cmp> MemtableMap;

class MemTable {
public:
    MemTable(size_t capacity, size_t max_tombstone_cap, const gsl_rng* rng)
    : m_cap(capacity), m_tombstone_cap(max_tombstone_cap), m_reccnt(0)
    , m_tombstonecnt(0), m_weight(0), m_max_weight(0) {
        m_tombstone_filter = nullptr;
        if (max_tombstone_cap > 0) {
            assert(rng != nullptr);
            m_tombstone_filter = new BloomFilter(BF_FPR, max_tombstone_cap, BF_HASH_FUNCS, rng);
        }

        m_tree = new MemtableMap(cmptor);
        m_min_key = {0};
        m_max_key = {0};
    }

    ~MemTable() {
        if (m_tombstone_filter) delete m_tombstone_filter;
        if (m_tree) delete m_tree;
    }

    int append(const char* key, const char* value, weight_type weight, bool is_tombstone = false) {
        size_t pos = m_reccnt.load();
        if (pos >= m_cap) {
            return 0;
        }

        if (is_tombstone && m_tombstonecnt + 1 > m_tombstone_cap) {
            return 0;
        }


        /* format the BTree record entries */
        btkey nrec = to_btkey(key, value, weight);

        if (pos == 0){
            m_min_key = to_btkey(key, value, weight);
            m_max_key = to_btkey(key, value, weight);
        } else {
            if (cmptor(nrec, m_min_key)) {
                m_min_key = nrec;
            }

            if (cmptor(m_max_key, nrec)) {
                m_max_key = nrec;
            }
        }

        nrec.flags |= (pos << 2) | is_tombstone;

        m_tree->insert(nrec, weight);

        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key, key_size);
        }

        m_reccnt.fetch_add(1);
        m_weight.fetch_add(weight);

        return 1;     
    }

    bool truncate() {
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        m_weight.store(0);
        m_max_weight.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();
        m_tree->clear();

        return true;
    }

    MemtableMap::iterator begin() {
        return m_tree->begin();
    }

    MemtableMap::iterator end() {
        return m_tree->end(); 
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
        btkey bkey = to_btkey(key, val);
        auto itr = m_tree->lower_bound(bkey);

        if (itr->key == *(key_type*) key && itr->val == *(value_type*) val) {
            m_tree->erase(itr);
            return true;
        }

        return false;
    }

    bool check_tombstone(const char* key, const char* val) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(key, key_size)) return false;

        btkey bkey = to_btkey(key, val);
        auto itr = m_tree->lower_bound(bkey);
        while (itr->key == *(key_type*) key && itr->val == *(value_type*) val) {
            if (itr->flags & 0x1) {
                return true;
            }
            itr++;
        }

        return false;
    }

    size_t get_memory_utilization() {
        return m_buffersize;
    }

    size_t get_aux_memory_utilization() {
        return m_tombstone_filter->get_memory_utilization();
    }

    size_t get_samples(char *sample_set, size_t k, gsl_rng *rng) {
        size_t reccnt = m_reccnt.load();
        if (reccnt == 0 || k == 0) {
            return 0;
        }

        std::vector<btkey> ans;
        m_tree->range_sample(m_min_key, m_max_key, k, ans, rng, false);

        for (size_t i=0; i<ans.size(); i++) {
            layout_record(sample_set + i*record_size, (char*) &ans[i].key, (char*) &ans[i].val, false, ans[i].weight);
        }

        return ans.size();
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

    weight_type get_total_weight() {
        return m_weight.load();
    }

private:
    size_t m_cap;
    size_t m_buffersize;
    size_t m_tombstone_cap;

    btkey m_min_key;
    btkey m_max_key;
    
    MemtableMap *m_tree;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_reccnt;
    alignas(64) std::atomic<weight_type> m_weight;
    alignas(64) std::atomic<weight_type> m_max_weight;

    btkey to_btkey(const char *key, const char *value, weight_type weight=0) {
        btkey bkey;
        bkey.flags = 0;
        bkey.weight = weight;
        bkey.key = *(key_type*) key;
        bkey.val = *(value_type*) value;

        return bkey;
    }
};

}
