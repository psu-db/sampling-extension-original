#pragma once

#include <vector>
#include <cassert>
#include <queue>

#include "lsm/MemTable.h"

namespace lsm {

class InMemRun {
public:
    // InMemISAM Tree Node: |8 keys|8 pointers|
    // Each node: 128 bytes;

    constexpr size_t inmem_isam_fanout = 8;
    constexpr size_t inmem_isam_node_size = 128;

    struct MergeCursor {
        char* ptr;
        char* target;
    };

    InMemRun(MemTable* mem_table) {
        m_data = (char*)std::aligned_alloc(SECTOR_SIZE, mem_table->get_record_count() * record_size);
        size_t offset = 0;
        m_reccnt = 0;

        while (base < stop) {
            if (is_tombstone(base) || ((base + record_size < stop) && !record_cmp(base+ record_size, base) && is_tombstone(base + record_size))) {
                base += record_size * 2;
            } else {
                memcpy(m_data + offset, base, record_size);
                offset += record_size;
                ++m_reccnt;
                base += record_size;
            }
        }

        size_t leaf_level_nodes = m_reccnt / inmem_isam_fanout + (m_reccnt % inmem_isam_fanout != 0);
        size_t level_nodes = leaf_level_nodes;
        size_t all_nodes = level_nodes;
        while (level_nodes > 1) {
            level_nodes = level_nodes / inmem_isam_fanout;
            all_nodes += level_nodes;
        }

        m_isam_nodes = (char*)std::aligned_alloc(SECTOR_SIZE, all_nodes * inmem_isam_node_size);
        const char* current_node = m_isam_nodes;
        const char* leaf_base = m_data;
        const char* leaf_stop = m_data + m_reccnt * record_size;
        size_t key_skip = key_size * inmem_isam_fanout;
        while (leaf_base < leaf_stop) {
            for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                auto rec_ptr = leaf_base + i * record_size;
                if (rec_ptr >= leaf_stop) break;
                memcpy(current_node + key_size * i, get_key(rec_ptr), key_size);
                memcpy(current_node + key_skip + sizeof(char*) * i, &rec_ptr, sizeof(char*));
            }
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
                for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                    auto node_ptr = now + i * inmem_isam_node_size;
                    if (node_ptr > level_stop) break;
                    memcpy(current_node + key_size * i, node_ptr, key_size)
                    memcpy(current_node + key_skip + sizeof(char *), &node_ptr, sizeof(char*));
                }
                now += inmem_isam_fanout * inmem_isam_node_size;
                current_node += inmem_isam_node_size;
            }
            level_start = level_stop;
            level_stop = current_node;
            current_level_node_cnt = (level_stop - level_start) / inmem_isam_node_size;
        }
        
        assert(level_stop - level_start == inmem_isam_node_size);
    }

    // Master interface to create an ondisk Run.
    InMemRun(std::vector<InMemRun*> runs) {
        std::vector<MergeCursor> cursors;
        cursors.reserve(runs.size() + 1);

        auto cmp = [cursors](const uint8_t a, const uint8_t b) -> bool {
            return record_cmp(cursors[a].ptr, cursors[b].ptr) > 0;
        };

        std::priority_queue<uint8_t, std::vector<uint8_t>, decltype(cmp)> pq(cmp);

        size_t attemp_reccnt = 0;
        if (mem_table) {
            auto base = mem_table->sorted_output();
            cursors.emplace_back(base, base + mem_table->get_record_count() * record_size);
            attemp_reccnt += mem_table->get_record_count();
            pq.emplace(0);
        }
        
        for (size_t i = 0; i < runs.size(); ++i) {
            assert(runs[i]);
            auto base = runs[i]->sorted_output();
            cursors.emplace_back(base, base + runs[i]->get_record_count() * record_size);
            attemp_reccnt += runs[i]->get_record_count();
            pq.emplace(i + 1);
        }

        m_data = (char*)std::aligned_alloc(SECTOR_SIZE, attemp_reccnt * record_size);
        size_t offset = 0;
        
        while (!pq.empty()) {
            auto now = pq.top();
            memcpy(m_data + offset, cursors[now].ptr, record_size);
            offset += record_size;
            pq.pop();
            if (cursors[now].ptr + record_size < cursors[now].target) {
                cursors[now].ptr += record_size;
                pq.emplace(now);
            }
        }
        

    }

    char* sorted_output() {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    
private:
    char* m_data;
    char* m_isam_nodes;
    size_t m_reccnt;
};

}