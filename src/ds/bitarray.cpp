#include "ds/bitarray.hpp"

bitarray::BitArray::BitArray(size_t size, bool default_value) 
{
    this->logical_size = size;
    this->physical_size = size + (size % 8) / 8;

    this->bits = std::make_unique<std::byte[]>(this->physical_size);

    if (default_value == true) {
        memset(bits.get(), 0xff, this->physical_size);
    }
}


bitarray::BitArray::BitArray()
{
    this->logical_size = this->physical_size = 0;
    this->bits = nullptr;
}


bitarray::bit_masking_data bitarray::BitArray::calculate_mask(size_t bit)
{
    bit_masking_data masking;

    auto byte_offset = bit / 8;
    auto bit_offset = bit % 8;

    masking.check_byte = this->bits.get() + byte_offset;
    masking.bit_mask = (std::byte) 0x1 << bit_offset;

    return masking;
}


bool bitarray::BitArray::is_set(size_t bit) {
    if (bit >= this->logical_size) {
        return false;
    }

    auto masking = this->calculate_mask(bit);

    return (bool) (*masking.check_byte & masking.bit_mask);
}


int bitarray::BitArray::set(size_t bit) {
    if (bit >= this->logical_size) {
        return 0;
    }

    auto masking = this->calculate_mask(bit);

    *masking.check_byte |= masking.bit_mask;

    return 1;
}


int bitarray::BitArray::unset(size_t bit) {
    if (bit >= this->logical_size) {
        return 0;
    }

    auto masking = this->calculate_mask(bit);

    *masking.check_byte &= ~masking.bit_mask;

    return 1;
}


void bitarray::BitArray::unset_all() {
   for (size_t i=0; i<this->physical_size; i++) {
        this->bits[i] = (std::byte) 0x0;
    } 
}
