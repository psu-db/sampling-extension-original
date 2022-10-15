#pragma once

#include <cmath>
#include <gsl/gsl_rng.h>

#include "util/BitArray.h"
#include "util/hash.h"
#include "util/base.h"

namespace lsm {

class BloomFilter {
public:
    BloomFilter(size_t n_bits, size_t k, const gsl_rng* rng)
    : m_n_bits(n_bits), m_n_salts(k), m_bitarray(n_bits) {
        salt = (uint16_t*) aligned_alloc(CACHELINE_SIZE, CACHELINEALIGN(k * sizeof(uint16_t)));
        for (size_t i = 0;  i < k; ++i) {
            salt[i] = (uint16_t) gsl_rng_uniform_int(rng, 1 << 16);
        }
        
    }

    BloomFilter(double max_fpr, size_t n, size_t k, const gsl_rng* rng)
    : BloomFilter((size_t)(-(double) (k * n) / std::log(1.0 - std::pow(max_fpr, 1.0 / k))), k, rng) {}

    int insert(const char* key, size_t sz) {
        if (m_bitarray.size() == 0) return 0;

        for (size_t i = 0; i < m_n_salts; ++i) {
            m_bitarray.set(hash_bytes_with_salt(key, sz, salt[i]) % m_n_bits);
        }

        return 1;
    }

    bool lookup(const char* key, size_t sz) {
        if (m_bitarray.size() == 0) return false;
        for (size_t i = 0; i < m_n_salts; ++i) {
            if (!m_bitarray.is_set(hash_bytes_with_salt(key, sz, salt[i]) % m_n_bits))
                return false;
        }

        return true;
    }

    void clear() {
        m_bitarray.clear();
    }

    size_t mem_utilization();
private: 
    size_t m_n_salts;
    size_t m_n_bits;
    uint16_t* salt;

    BitArray m_bitarray;
};

}
