/*
 *
 */

#include "ds/bloomfilter.hpp"

namespace lsm { namespace ds {

std::unique_ptr<BloomFilter> BloomFilter::create_persistent(size_t filter_size, size_t key_size, size_t k, PageId meta_pid, global::g_state *state)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);
    if (pfile) {
        return BloomFilter::create_persistent(filter_size, key_size, k, meta_pid.page_number, pfile, state);
    }

    return nullptr;
}


std::unique_ptr<BloomFilter> BloomFilter::create_persistent(size_t filter_size, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state)
{
    if (k > BLOOMFILTER_K_MAX) {
        return nullptr;
    }

    auto bitmap = BitMap::create_persistent(filter_size, meta_pnum, pfile);

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

    auto bf = new BloomFilter(meta_pnum, pfile);
    return std::unique_ptr<BloomFilter>(bf);
}



std::unique_ptr<BloomFilter> BloomFilter::create_persistent(double max_fpr, size_t n, size_t key_size, size_t k, PageNum meta_pnum, io::PagedFile *pfile, global::g_state *state)
{
    if (k > BLOOMFILTER_K_MAX) {
        return nullptr;
    }

    size_t filter_size = - (double) (k * n) / log(1.0 - pow(max_fpr, 1.0 / (double) k));

    return BloomFilter::create_persistent(filter_size, key_size, k, meta_pnum, pfile, state);

    /*
    auto bitmap = BitMap::create_persistent(filter_size, meta_pnum, pfile);

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

    auto bf = new BloomFilter(meta_pnum, pfile);
    return std::unique_ptr<BloomFilter>(bf);
    */
}


std::unique_ptr<BloomFilter> BloomFilter::create_volatile(double max_fpr, size_t n, size_t key_size, size_t k, global::g_state *state)
{
    if (k > BLOOMFILTER_K_MAX) {
        return nullptr;
    }

    size_t filter_size = - (double) (k * n) / log(1.0 - pow(max_fpr, 1.0 / (double) k));

    auto bitmap = BitMap::create_volatile(filter_size);

    std::vector<int64_t> a_v(k);
    std::vector<int64_t> b_v(k);

    for (size_t i=0; i<k; i++) {
        int64_t a = gsl_rng_get(state->rng);
        int64_t b = gsl_rng_get(state->rng);

        a_v[i] = a;
        b_v[i] = b;
    }

    auto bf = new BloomFilter(std::move(bitmap), key_size, a_v, b_v);

    return std::unique_ptr<BloomFilter>(bf);
}



std::unique_ptr<BloomFilter> BloomFilter::open(PageId meta_pid, global::g_state *state)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);
    if (pfile) {
        return BloomFilter::open(meta_pid.page_number, pfile);
    }

    return nullptr;
}


std::unique_ptr<BloomFilter> BloomFilter::open(PageNum meta_pnum, io::PagedFile *pfile)
{
    auto bf = new BloomFilter(meta_pnum, pfile);
    return std::unique_ptr<BloomFilter>(bf);
}


BloomFilter::BloomFilter(PageNum meta_pnum, io::PagedFile *pfile)
{
    auto buf = mem::page_alloc();
    this->bitmap = BitMap::open(meta_pnum, pfile);

    this->persistent = true;

    pfile->read_page(meta_pnum, buf.get());
    auto meta = (BloomFilterMetaHeader *) buf.get();

    this->hash_funcs = meta->hash_funcs;
    this->key_size = meta->key_size;
    this->filter_size = meta->bitmap_header.logical_size;
    this->physical_size = meta->bitmap_header.physical_size;

    this->a.resize(this->hash_funcs);
    this->b.resize(this->hash_funcs);
    this->coeffs.resize(this->hash_funcs * 2);

    for (size_t i=0, j=0; i<this->hash_funcs; i++, j+=2) {
        this->a[i] = meta->hash_data[j];
        this->b[i] = meta->hash_data[j + 1];

        this->coeffs[j] = meta->hash_data[j];
        this->coeffs[j+1] = meta->hash_data[j+1];
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


BloomFilter::BloomFilter(std::unique_ptr<BitMap> bitmap, size_t key_size, std::vector<int64_t> a, std::vector<int64_t> b)
{
    this->bitmap = std::move(bitmap);

    this->hash_funcs = a.size();
    this->key_size = key_size;
    this->filter_size = this->bitmap->logical_size();
    this->physical_size = this->bitmap->physical_size();
    this->persistent = false;

    this->a = a;
    this->b = b;
    this->coeffs.resize(this->hash_funcs * 2);
    for (size_t i=0; i<this->hash_funcs-1; i++) {
        coeffs[2*i] = a[i];
        coeffs[2*i+1] = b[i];
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


int BloomFilter::insert(const byte *element)
{
    if (this->filter_size == 0) {
        return 0;
    }

    for (size_t i=0; i<this->hash_funcs; i++) {
        this->bitmap->set(this->hash(element, i));
    }

    return 1;
}


bool BloomFilter::lookup(const byte *element)
{
    if (this->filter_size == 0) {
        return false;
    }

    for (size_t i=0; i<this->hash_funcs; i++) {
        if (!this->bitmap->is_set(this->hash(element, i))) {
            return false;
        }
    }

    return true;
}


void BloomFilter::clear()
{
    this->bitmap->unset_all();
}


void BloomFilter::flush()
{
    this->bitmap->flush();
}


size_t BloomFilter::hash(const byte *key, size_t hash_func)
{
    int64_t B_1 = this->coeffs[hash_func * 2];
    int64_t B_2 = this->coeffs[hash_func * 2 + 1];

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


size_t BloomFilter::memory_utilization()
{
    return this->physical_size;
}


bool BloomFilter::is_persistent()
{
    return this->persistent;
}

}}
