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

    struct MergeCursor {
        char* ptr;
        char* target;
    };

    // Master interface to create an ondisk Run.
    InMemRun(MemTable* mem_table, std::vector<InMemRun*> runs) {
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