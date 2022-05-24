/*
*
*/
#ifndef H_BLOOMFILTER
#define H_BLOOMFILTER

#include <cstdlib>
#include "ds/bitarray.hpp"

#include <cstdio>

namespace bloomfilter {
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
    };
}
#endif
