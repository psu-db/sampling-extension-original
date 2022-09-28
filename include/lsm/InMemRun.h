#pragma once

#include <vector>
#include <cassert>
#include <queue>

#include "lsm/MemTable.h"
#include "ds/PriorityQueue.h"

namespace lsm {

constexpr size_t inmem_isam_fanout = 8;
constexpr size_t inmem_isam_node_size = 128;

class InMemRun {
public:
    // InMemISAM Tree Node: | double | 7 keys | 8 pointers |
    // Each node: 128 bytes;
    struct MergeCursor {
        char* ptr;
        char* target;
    };

    struct InMemSampleRange {
        size_t start, stop;
        double weight;
    };

    InMemRun(MemTable* mem_table) {
        m_data = (char*)std::aligned_alloc(SECTOR_SIZE, mem_table->get_record_count() * record_size);
        size_t offset = 0;
        m_reccnt = 0;
        auto base = mem_table->sorted_output();
        auto stop = base + mem_table->get_record_count() * record_size;
        while (base < stop) {
            if (!is_tombstone(base) && (base + record_size < stop)
                && !key_cmp(get_key(base+ record_size), get_key(base)) && is_tombstone(base + record_size)) {
                base += record_size * 2;
            } else {
                memcpy(m_data + offset, base, record_size);
                offset += record_size;
                ++m_reccnt;
                base += record_size;
            }
        }

        build_internal_levels();
    }

    // Master interface to create an ondisk Run.
    InMemRun(std::vector<InMemRun*> runs) {
        std::vector<MergeCursor> cursors;
        cursors.reserve(runs.size() + 1);

        PriorityQueue pq(runs.size());

        size_t attemp_reccnt = 0;
        
        for (size_t i = 0; i < runs.size(); ++i) {
            assert(runs[i]);
            auto base = runs[i]->sorted_output();
            cursors.emplace_back(base, base + runs[i]->get_record_count() * record_size);
            attemp_reccnt += runs[i]->get_record_count();
            pq.push(cursors[i].ptr, i);
        }

        m_data = (char*)std::aligned_alloc(SECTOR_SIZE, attemp_reccnt * record_size);
        size_t offset = 0;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record{nullptr, 0};
            if (!is_tombstone(now.data) && next.data != nullptr &&
                !key_cmp(get_key(now.data), get_key(next.data)) && is_tombstone(next.data)) {
                
                pq.pop(); pq.pop();
                
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (cursor1.ptr + record_size < cursor1.target) {
                    cursor1.ptr += record_size;
                    pq.push(cursor1.ptr, now.version);
                }
                if (cursor2.ptr + record_size < cursor2.target) {
                    cursor2.ptr += record_size;
                    pq.push(cursor2.ptr, next.version);
                }
            } else {
                memcpy(m_data + offset, cursors[now.version].ptr, record_size);
                offset += record_size;
                pq.pop();
                auto& cursor = cursors[now.version];
                if (cursor.ptr + record_size < cursor.target) {
                    cursor.ptr += record_size;
                    pq.push(cursor.ptr, now.version);
                }
            }
        }

        build_internal_levels();
    }

    char* sorted_output() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    const char* get_record_at(size_t idx) const {
        assert(idx < m_reccnt);
        return m_data + idx * record_size;
    }

    size_t get_lower_bound(const char* key) const{
        
    }

    size_t get_upper_bound(const char* key) const {

    }

    
private:
    void build_internal_levels() {
        size_t leaf_level_nodes = m_reccnt / inmem_isam_fanout + (m_reccnt % inmem_isam_fanout != 0);
        size_t level_nodes = leaf_level_nodes;
        size_t all_nodes = level_nodes;
        while (level_nodes > 1) {
            level_nodes = level_nodes / inmem_isam_fanout;
            all_nodes += level_nodes;
        }

        m_isam_nodes = (char*)std::aligned_alloc(SECTOR_SIZE, all_nodes * inmem_isam_node_size);
        char* current_node = m_isam_nodes;
        const char* leaf_base = m_data;
        const char* leaf_stop = m_data + m_reccnt * record_size;
        size_t key_skip = key_size * inmem_isam_fanout;
        while (leaf_base < leaf_stop) {
            double node_weight = 0.0;
            for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                auto rec_ptr = leaf_base + i * record_size;
                if (rec_ptr >= leaf_stop) break;
                node_weight += 1.0;
                if (i == 0) continue;
                memcpy(current_node + key_size * i, get_key(rec_ptr), key_size);
                memcpy(current_node + key_skip + sizeof(char*) * i, &rec_ptr, sizeof(char*));
            }
            memcpy(current_node, &node_weight, sizeof(double));
            current_node += inmem_isam_node_size;
            leaf_base += inmem_isam_fanout * record_size;
        }

        auto level_start = m_isam_nodes;
        auto level_stop = current_node;
        auto current_level_node_cnt = (level_stop - level_start) / inmem_isam_node_size;
        assert(leaf_level_nodes == current_level_node_cnt);
        while (current_level_node_cnt > 1) {
            auto now = level_start;
            while (now < level_stop) {
                double node_weight = 0.0;
                for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                    auto node_ptr = now + i * inmem_isam_node_size;
                    if (node_ptr > level_stop) break;
                    double child_weight;
                    memcpy(&child_weight, node_ptr, sizeof(double));
                    node_weight += child_weight;
                    if (i == 0) continue;
                    memcpy(current_node + key_size * i, node_ptr, key_size);
                    memcpy(current_node + key_skip + sizeof(char *), &node_ptr, sizeof(char*));
                }
                now += inmem_isam_fanout * inmem_isam_node_size;
                current_node += inmem_isam_node_size;
            }
            level_start = level_stop;
            level_stop = current_node;
            current_level_node_cnt = (level_stop - level_start) / inmem_isam_node_size;
        }
        
        assert(current_level_node_cnt == 1);

        m_root = level_start;
    }

    // Members: sorted data, internal ISAM levels, reccnt;
    char* m_data;
    char* m_isam_nodes;
    char* m_root;
    size_t m_reccnt;
};

}