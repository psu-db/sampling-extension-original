/*
*
*/

#ifndef H_BITARRAY
#define H_BITARRAY

#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <memory>

namespace bitarray {
    struct bit_masking_data {
        std::byte *check_byte;
        std::byte bit_mask;
    };

    class BitArray {
    private:
        std::unique_ptr<std::byte[]> bits;
        size_t logical_size; // the requested number of bits
        size_t physical_size; // the amount of space allocated

        bit_masking_data calculate_mask(size_t bit);

    public:
        BitArray(size_t size, bool default_value=0);
        BitArray();
        int set(size_t bit);
        int unset(size_t bit);
        bool is_set(size_t bit);
        void unset_all();
    };
}
#endif
