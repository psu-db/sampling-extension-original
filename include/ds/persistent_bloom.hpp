/*
*
*/
#ifndef H_PERSISTENTBLOOM
#define H_PERSISTENTBLOOM

#include <cstdlib>
#include "ds/bitmap.hpp"

#include <cstdio>

namespace lsm { namespace ds {

const size_t BLOOMFILTER_K_MAX = 64;

struct BloomFilterMetaHeader {
    BitMapMetaHeader bitmap_header;
    size_t key_size;
    size_t hash_funcs;
    int64_t hash_data[2*BLOOMFILTER_K_MAX];
};

class PersistentBloomFilter {
public:
    static std::unique_ptr<PersistentBloomFilter> create(size_t filter_size, size_t key_size, size_t k, PageId meta_pid, global::g_state *state);
    static std::unique_ptr<PersistentBloomFilter> create(size_t filter_size, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state);

    static std::unique_ptr<PersistentBloomFilter> open(PageId meta_pid, global::g_state *state);
    static std::unique_ptr<PersistentBloomFilter> open(PageNum meta_pnum, io::PagedFile *pfile);

    int insert(const byte *element);
    bool lookup(const byte *element);
    void clear();
    void flush();

private:
    std::unique_ptr<BitMap> bitmap;
    size_t key_size;
    size_t filter_size;
    size_t hash_funcs; // k
    
    size_t key_chunk_size;
    size_t key_chunks;
    
    std::vector<int64_t> a;
    std::vector<int64_t> b;

    size_t hash(const byte *key, size_t hash_func);

    PersistentBloomFilter(PageNum meta_pnum, io::PagedFile *pfile);
};
}}
#endif
