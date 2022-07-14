/*
*
*/
#ifndef H_BLOOMFILTER
#define H_BLOOMFILTER

#include <cmath>
#include <cstdlib>
#include <cstdio>

#include "ds/bitarray.hpp"


namespace lsm { namespace ds {
template <typename T>
class BloomFilter {
private:
    bitarray::BitArray data;
    size_t size;
    size_t hash_funcs; // k

    std::hash<T> hash;
    /*
     * A more general solution would allow user-specified hashing schemes
     */
    size_t calculate_hash(T element, size_t hash_version)
    {
        size_t hash_val = this->hash(element);
        for (size_t i=0; i<hash_version; i++) {
            hash_val += this->hash(hash_val);
        }

        return hash_val % this->size;
    }

public:
    BloomFilter(size_t size, size_t k=3) 
    {
        this->data = bitarray::BitArray(size);
        this->size = size;
        this->hash_funcs = k;
    }


    BloomFilter(double max_fpr, size_t n, size_t k=3)
    {
        size_t filter_size = - (double) (k * n) / log(1.0 - pow(max_fpr, 1.0 / (double) k));

        /*
        size_t lower_val = pow(2, floor(log(filter_size)/log(2)));
        size_t upper_val = pow(2, ceil(log(filter_size)/log(2)));

        size_t diff_l = abs((int64_t) filter_size - (int64_t) lower_val);
        size_t diff_u = abs((int64_t) filter_size - (int64_t) upper_val);

        size_t size = (diff_l > diff_u) ? upper_val : lower_val;
        */

        this->data = bitarray::BitArray(size);
        this->size = filter_size;
        this->hash_funcs = k;
    }

    BloomFilter() {};


    int insert(T element)
    {
        for (size_t i=0; i<=this->hash_funcs; i++) {
            this->data.set(calculate_hash(element, i));
        }

        return 1;
    }


    bool lookup(T element)
    {
        size_t hits = 0;
        for (size_t i=0; i<this->hash_funcs; i++) {
            hits += this->data.is_set(calculate_hash(element, i));
        }

        return hits == this->hash_funcs;
    }


    void clear()
    {
        this->data.unset_all();
    }

    size_t memory_utilization()
    {
        return this->size;
    }
};
}}
#endif
