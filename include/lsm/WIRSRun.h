#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "lsm/MemTable.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/Alias.h"

namespace lsm {

struct wirs_node {
    struct wirs_node *left, *right;
    key_type low, high;
    double weight;
    Alias alias;
};

thread_local size_t mrun_cancelations = 0;

class WIRSRun {
public:
    WIRSRun(MemTable* mem_table, BloomFilter* bf)
    :m_reccnt(0), m_tombstone_cnt(0), m_group_size(0) {

        size_t alloc_size = (mem_table->get_record_count() * record_size) + (CACHELINE_SIZE - (mem_table->get_record_count() * record_size) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = mem_table->sorted_output();
        auto stop = base + mem_table->get_record_count() * record_size;
        while (base < stop) {
            if (!is_tombstone(base) && (base + record_size < stop)
                && !record_cmp(base + record_size, base) && is_tombstone(base + record_size)) {
                base += record_size * 2;
                mrun_cancelations++;
            } else {
                //Masking off the ts.
                *((rec_hdr*)get_hdr(base)) &= 1;
                memcpy(m_data + offset, base, record_size);
                if (is_tombstone(base)) {
                    ++m_tombstone_cnt;
                    bf->insert(get_key(base), key_size);
                }
                offset += record_size;
                ++m_reccnt;
                base += record_size;
            }
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    WIRSRun(WIRSRun** runs, size_t len, BloomFilter* bf)
    :m_reccnt(0), m_tombstone_cnt(0), m_group_size(0) {
        std::vector<Cursor> cursors;
        cursors.reserve(len);

        PriorityQueue pq(len);

        size_t attemp_reccnt = 0;
        
        for (size_t i = 0; i < len; ++i) {
            //assert(runs[i]);
            if (runs[i]) {
                auto base = runs[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count() * record_size, 0, runs[i]->get_record_count()});
                attemp_reccnt += runs[i]->get_record_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor{nullptr, nullptr, 0, 0});
            }
        }

        size_t alloc_size = (attemp_reccnt * record_size) + (CACHELINE_SIZE - (attemp_reccnt * record_size) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        
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
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    ~WIRSRun() {
        if (m_data) free(m_data);
        free_tree(m_root);
    }

    void free_tree(struct wirs_node* node) {
        if (node) {
            free_tree(node->left);
            free_tree(node->right);
            delete node;
        }
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

   double get_sample_weight(const char* lower_key, const char* upper_key) {
        return get_sample_weight_internal(m_root, lower_key, upper_key);
   }

    void get_samples(char *sample_set, const char *lower_key, const char *upper_key, size_t sample_sz, gsl_rng *rng) {
        // low - high -> decompose to a set of nodes.
        // Build Alias across the decomposed nodes.
        std::vector<struct wirs_node*> nodes;
        decompose_node(m_root, lower_key, upper_key, nodes);
        double tot_weight = 0.0;
        std::vector<double> weights;
        for (const auto& node: nodes) {
            tot_weight += node->weight;
            weights.emplace_back(node->weight);
        }

        assert(tot_weight > 0.0);
        for (auto& w: weights) w /= tot_weight;

        Alias top_level_alias(weights);
        // k -> sampling: three levels. 1. select a node -> select a fat point -> select a record.
        size_t cnt = 0;
        do {
            // first level....
            auto node = nodes[top_level_alias.get(rng)];
            // second level...
            auto fat_point = node->low + node->alias.get(rng);
            // third level...
            size_t rec_offset = fat_point * m_group_size + m_alias[fat_point].get(rng);
            auto record = m_data + rec_offset * record_size;
            if (!is_tombstone(record) && key_cmp(lower_key, get_key(record)) <= 0 && key_cmp(get_key(record), upper_key) <= 0) {
                memcpy(sample_set + cnt * record_size, record, record_size);
                ++cnt;
            }
        } while (cnt < sample_sz);
    }
    
private:
    bool covered_by(struct wirs_node* node, const char* lower_key, const char* upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return key_cmp(lower_key, get_key(m_data + low_index * record_size)) == -1 &&
               key_cmp(get_key(m_data + high_index * record_size), upper_key) == -1;
    }

    bool intersects(struct wirs_node* node, const char* lower_key, const char* upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return key_cmp(lower_key, get_key(m_data + high_index * record_size)) == -1 ||
               key_cmp(get_key(m_data + low_index * record_size), upper_key) == -1;
    }

    void decompose_node(struct wirs_node* node, const char* lower_key, const char* upper_key, std::vector<struct wirs_node*>& output) {
        if (node == nullptr) return;
        else if (covered_by(node, lower_key, upper_key)) {
            output.emplace_back(node);
        } else {
            if (node->left && intersects(node->left, lower_key, upper_key)) decompose_node(node->left, lower_key, upper_key, output);
            if (node->right && intersects(node->right, lower_key, upper_key)) decompose_node(node->right, lower_key, upper_key, output);
        }
    }

    double get_sample_weight_internal(struct wirs_node* node, const char* lower_key, const char* upper_key) {
        if (node == nullptr) return 0.0;
        else if (covered_by(node, lower_key, upper_key)) return node->weight;

        double ans = 0.0;
        if (node->left && intersects(node->left, lower_key, upper_key)) ans += get_sample_weight_internal(node->left, lower_key, upper_key);
        if (node->right && intersects(node->right, lower_key, upper_key)) ans += get_sample_weight_internal(node->right, lower_key, upper_key);
        return ans;
    }

    struct wirs_node* construct_wirs_node(const std::vector<double> weights, size_t low, size_t high) {
        if (low == high) {
            return new wirs_node{nullptr, nullptr, low, high, weights[low], Alias({1.0})};
        } else if (low > high) return nullptr;
        std::vector<double> node_weights;
        double sum = 0.0;
        for (size_t i = low; i < high; ++i) {
            node_weights.emplace_back(weights[i]);
            sum += weights[i];
        }

        for (auto& w: node_weights)
            if (sum) w /= sum;
            else w = 1.0 / node_weights.size();
        
        
        size_t mid = (low + high) / 2;
        return new wirs_node{construct_wirs_node(weights, low, mid),
                             construct_wirs_node(weights, mid + 1, high),
                             low, high, sum, Alias(node_weights)};
    }

    void build_wirs_structure() {
        m_group_size = std::ceil(std::log(m_reccnt));
        size_t n_groups = std::ceil(m_reccnt / m_group_size);
        
        // Fat point construction + low level alias....
        double sum_weight = 0.0;
        std::vector<double> weights;
        std::vector<double> group_norm_weight;
        size_t i = 0;
        size_t group_no = 0;
        while (i < m_reccnt) {
            double group_weight = 0.0;
            group_norm_weight.clear();
            for (size_t k = 0; k < m_group_size && i < m_reccnt; ++k, ++i) {
                auto w = get_weight(m_data + record_size * i);
                group_norm_weight.emplace_back(w);
                group_weight += w;
                sum_weight += w;
            }

            for (auto& w: group_norm_weight)
                if (group_weight) w /= group_weight;
                else w = 1.0 / group_norm_weight.size();
            m_alias.emplace_back(Alias(group_norm_weight));

            
            weights.emplace_back(group_weight);
        }

        assert(weights.size() == n_groups);

        m_root = construct_wirs_node(weights, 0, n_groups);
    }

    char* m_data;
    std::vector<Alias> m_alias;
    wirs_node* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
};

}