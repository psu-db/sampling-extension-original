/*
 *
 */

#include "ds/persistent_bloom.hpp"

namespace lsm { namespace ds {

std::unique_ptr<PersistentBloomFilter> PersistentBloomFilter::create(size_t filter_size, size_t key_size, size_t k, PageId meta_pid, global::g_state *state)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);
    return PersistentBloomFilter::create(filter_size, key_size, k, meta_pid.page_number, pfile, state);
}


std::unique_ptr<PersistentBloomFilter> PersistentBloomFilter::create(size_t filter_size, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state)
{
    if (k > BLOOMFILTER_K_MAX) {
        return nullptr;
    }

    auto bitmap = BitMap::create(filter_size, meta_pnum, pfile);

    auto buf = mem::page_alloc();
    pfile->read_page(meta_pnum, buf.get());

    auto meta = (BloomFilterMetaHeader *) buf.get();
    meta->hash_funcs = k;
    meta->key_size = key_size;

    for (size_t i=0; i<k; i += 2) {
        int64_t a = gsl_rng_get(state->rng);
        int64_t b = gsl_rng_get(state->rng);

        meta->hash_data[i] = a;
        meta->hash_data[i + 1] = b;
    }

    pfile->write_page(meta_pnum, buf.get());

    auto bf = new PersistentBloomFilter(meta_pnum, pfile);
    return std::unique_ptr<PersistentBloomFilter>(bf);
}


std::unique_ptr<PersistentBloomFilter> PersistentBloomFilter::open(PageId meta_pid, global::g_state *state)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);
    return PersistentBloomFilter::open(meta_pid.page_number, pfile);
}


std::unique_ptr<PersistentBloomFilter> PersistentBloomFilter::open(PageNum meta_pnum, io::PagedFile *pfile)
{
    auto bf = new PersistentBloomFilter(meta_pnum, pfile);
    return std::unique_ptr<PersistentBloomFilter>(bf);
}


PersistentBloomFilter::PersistentBloomFilter(PageNum meta_pnum, io::PagedFile *pfile)
{
    auto buf = mem::page_alloc();
    this->bitmap = BitMap::open(meta_pnum, pfile);

    pfile->read_page(meta_pnum, buf.get());
    auto meta = (BloomFilterMetaHeader *) buf.get();

    this->hash_funcs = meta->hash_funcs;
    this->key_size = meta->key_size;
    this->filter_size = meta->bitmap_header.logical_size;

    this->a.resize(this->hash_funcs);
    this->b.resize(this->hash_funcs);

    for (size_t i=0, j=0; i<this->hash_funcs; i++, j+=2) {
        this->a[i] = meta->hash_data[j];
        this->b[i] = meta->hash_data[j + 1];
    }

    if (this->key_size < 4) {
        this->key_chunk_size = 1;
    } else if (this->key_size < 8) {
        this->key_chunk_size = 4;
    } else {
        this->key_chunk_size = 8;
    }

    this->key_chunks = this->key_size / this->key_chunk_size;
}


int PersistentBloomFilter::insert(const byte *element)
{
    for (size_t i=0; i<this->hash_funcs; i++) {
        this->bitmap->set(this->hash(element, i));
    }

    return 1;
}


bool PersistentBloomFilter::lookup(const byte *element)
{
    for (size_t i=0; i<this->hash_funcs; i++) {
        if (!this->bitmap->is_set(this->hash(element, i))) {
            return false;
        }
    }

    return true;
}


void PersistentBloomFilter::clear()
{
    this->bitmap->unset_all();
}


void PersistentBloomFilter::flush()
{
    this->bitmap->flush();
}


size_t PersistentBloomFilter::hash(const byte *key, size_t hash_func)
{
    int64_t B_1 = this->a[hash_func];
    int64_t B_2 = this->b[hash_func];

    size_t hash_value = 0;
    for (size_t i=0; i<this->key_chunks; i++) {
        if (this->key_chunk_size == 1) {
            hash_value += B_1 * *((uint8_t *) (key + i * this->key_chunk_size)) + B_2;
        } else if (this->key_chunk_size == 4) {
            hash_value += B_1 * *((uint32_t *) (key + i * this->key_chunk_size)) + B_2;
        } else if (this->key_chunk_size == 8) {
            hash_value += B_1 * *((uint64_t *) (key + i * this->key_chunk_size)) + B_2;
        }
    }

    return hash_value % this->filter_size;
}

}}
