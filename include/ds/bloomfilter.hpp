/*
*
*/
#ifndef H_BLOOMFILTER
#define H_BLOOMFILTER

#include <cstdlib>
#include "ds/bitmap.hpp"

#include <cstdio>
#include <cmath>

namespace lsm { namespace ds {

const size_t BLOOMFILTER_K_MAX = 64;

struct BloomFilterMetaHeader {
    BitMapMetaHeader bitmap_header;
    size_t key_size;
    size_t hash_funcs;
    int64_t hash_data[2*BLOOMFILTER_K_MAX];
};

class BloomFilter {
public:
    static std::unique_ptr<BloomFilter> create_persistent(size_t filter_size, size_t key_size, size_t k, PageId meta_pid, global::g_state *state);
    static std::unique_ptr<BloomFilter> create_persistent(size_t filter_size, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state);
    static std::unique_ptr<BloomFilter> create_persistent(double max_fpr, size_t n, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state);

    static std::unique_ptr<BloomFilter> create_volatile(double max_fpr, size_t n, size_t key_size, size_t k, global::g_state *state);

    static std::unique_ptr<BloomFilter> open(PageId meta_pid, global::g_state *state);
    static std::unique_ptr<BloomFilter> open(PageNum meta_pnum, io::PagedFile *pfile);

    int insert(const byte *element);
    bool lookup(const byte *element);
    void clear();
    void flush();

    size_t memory_utilization();

    bool is_persistent();

private:
    std::unique_ptr<BitMap> bitmap;
    size_t key_size;

    size_t filter_size;
    size_t physical_size;

    size_t hash_funcs; // k
    
    size_t key_chunk_size;
    size_t key_chunks;

    bool persistent;
    
    std::vector<int64_t> a;
    std::vector<int64_t> b;

    std::vector<int64_t> coeffs;

    size_t hash(const byte *key, size_t hash_func);

    BloomFilter(PageNum meta_pnum, io::PagedFile *pfile);
    BloomFilter(std::unique_ptr<BitMap> bitmap, size_t key_size, std::vector<int64_t> a, std::vector<int64_t> b);
};
}}
#endif
