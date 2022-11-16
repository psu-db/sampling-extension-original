#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "lsm/MemTable.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"

namespace lsm {

constexpr size_t inmem_isam_node_size = 64;

constexpr size_t inmem_isam_fanout = inmem_isam_node_size / (key_size + sizeof(char*));
constexpr size_t inmem_isam_leaf_fanout = inmem_isam_node_size / record_size;
constexpr size_t inmem_isam_node_keyskip = key_size * inmem_isam_fanout;

thread_local size_t mrun_cancelations = 0;

class InMemRun {
public:
    InMemRun(MemTable* mem_table, BloomFilter* bf)
    :m_reccnt(0), m_tombstone_cnt(0), m_isam_nodes(nullptr) {
        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, mem_table->get_record_count() * record_size);
        //memset(m_data, 0, mem_table->get_record_count() * record_size);
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
            build_internal_levels();
        }
    }

    InMemRun(InMemRun** runs, size_t len, BloomFilter* bf)
    :m_reccnt(0), m_tombstone_cnt(0), m_isam_nodes(nullptr) {
        std::vector<Cursor> cursors;
        cursors.reserve(len);

        PriorityQueue pq(len);

        size_t attemp_reccnt = 0;
        
        for (size_t i = 0; i < len; ++i) {
            //assert(runs[i]);
            if (runs[i]) {
                auto base = runs[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count() * record_size});
                attemp_reccnt += runs[i]->get_record_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor{nullptr, nullptr});
            }
            
        }

        m_data = (char*)std::aligned_alloc(CACHELINE_SIZE, attemp_reccnt * record_size);
        size_t offset = 0;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record{nullptr, 0};
            if (!is_tombstone(now.data) && next.data != nullptr &&
                !key_cmp(get_key(now.data), get_key(next.data)) && is_tombstone(next.data)) {
                
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
            build_internal_levels();
        }
    }

    ~InMemRun() {
        if (m_data) free(m_data);
        if (m_isam_nodes) free(m_isam_nodes);
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

    size_t get_lower_bound(const char* key) const {
        char* now = m_root;
        while (!is_leaf(now)) {
            char* child_ptr = now + inmem_isam_node_keyskip;
            uint8_t ptr_offset = 0;
            const char* sep_key = now;
            char* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (nullptr == *(char**)(child_ptr + sizeof(char*)) || key_cmp(key, sep_key) <= 0) {
                    next = *(char**)child_ptr;
                    break;
                }
                sep_key += key_size;
                child_ptr += sizeof(char*);
            }
            
            now = next ? next : *(char**)(now + inmem_isam_node_size - sizeof(char*));
        }

        while (now < m_data + m_reccnt * record_size && key_cmp(now, key) == -1)
            now += record_size;

        return (now - m_data) / record_size;
    }

    size_t get_upper_bound(const char* key) const {
        char* now = m_root;
        
        while (!is_leaf(now)) {
            char* child_ptr = now + inmem_isam_node_keyskip;
            uint8_t ptr_offset = 0;
            const char* sep_key = now;
            char* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (nullptr == *(char**)(child_ptr + sizeof(char*)) || key_cmp(key, sep_key) == -1) {
                    next = *(char**)child_ptr;
                    break;
                }
                sep_key += key_size;
                child_ptr += sizeof(char*);
            }
            now = next ? next : *(char**)(now + inmem_isam_node_size - sizeof(char*));
            
        }

        while (now < m_data + m_reccnt * record_size && key_cmp(now, key) <= 0)
            now += record_size;

        return (now - m_data) / record_size;
    }

    bool check_tombstone(const char* key, const char* val) const {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        auto ptr = m_data + (get_lower_bound(key) * record_size);

        char buf[record_size];
        layout_record(buf, key, val, false);
        while (ptr < m_data + m_reccnt * record_size && record_cmp(ptr, buf) == -1) {
            ptr += record_size;
        }
        return record_match(ptr, key, val, true);
    }
    
private:
    void build_internal_levels() {
        size_t n_leaf_nodes = m_reccnt / inmem_isam_leaf_fanout + (m_reccnt % inmem_isam_leaf_fanout != 0);
        size_t level_node_cnt = n_leaf_nodes;
        size_t node_cnt = 0;
        do {
            level_node_cnt = level_node_cnt / inmem_isam_fanout + (level_node_cnt % inmem_isam_fanout != 0);
            node_cnt += level_node_cnt;
        } while (level_node_cnt > 1);

        m_isam_nodes = (char*)std::aligned_alloc(CACHELINE_SIZE, node_cnt * inmem_isam_node_size);
        memset(m_isam_nodes, 0, node_cnt * inmem_isam_node_size);

        char* current_node = m_isam_nodes;

        const char* leaf_base = m_data;
        const char* leaf_stop = m_data + m_reccnt * record_size;
        while (leaf_base < leaf_stop) {
            for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                auto rec_ptr = leaf_base + inmem_isam_leaf_fanout * record_size * i;
                if (rec_ptr >= leaf_stop) break;
                const char* sep_key = std::min(rec_ptr + (inmem_isam_leaf_fanout - 1) * record_size, leaf_stop - record_size);
                memcpy(current_node + key_size * i, get_key(sep_key), key_size);
                memcpy(current_node + inmem_isam_node_keyskip + sizeof(char*) * i, &rec_ptr, sizeof(char*));
            }
            current_node += inmem_isam_node_size;
            leaf_base += inmem_isam_fanout * inmem_isam_leaf_fanout * record_size;
        }

        auto level_start = m_isam_nodes;
        auto level_stop = current_node;
        auto current_level_node_cnt = (level_stop - level_start) / inmem_isam_node_size;
        while (current_level_node_cnt > 1) {
            auto now = level_start;
            while (now < level_stop) {
                size_t child_cnt = 0;
                for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                    auto node_ptr = now + i * inmem_isam_node_size;
                    ++child_cnt;
                    if (node_ptr >= level_stop) break;
                    memcpy(current_node + key_size * i, node_ptr + inmem_isam_node_keyskip - key_size, key_size);
                    memcpy(current_node + inmem_isam_node_keyskip + sizeof(char *) * i, &node_ptr, sizeof(char*));
                }
                now += child_cnt * inmem_isam_node_size;
                current_node += inmem_isam_node_size;
            }
            level_start = level_stop;
            level_stop = current_node;
            current_level_node_cnt = (level_stop - level_start) / inmem_isam_node_size;
        }
        
        assert(current_level_node_cnt == 1);
        m_root = level_start;
    }

    bool is_leaf(const char* ptr) const {
        return ptr >= m_data && ptr < m_data + m_reccnt * record_size;
    }

    // Members: sorted data, internal ISAM levels, reccnt;
    char* m_data;
    char* m_isam_nodes;
    char* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
};

}
