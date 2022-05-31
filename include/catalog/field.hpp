/*
 *
 */

#ifndef FIELD_H
#define FIELD_H

#include "util/types.hpp"
#include "util/base.hpp"

namespace lsm { namespace catalog {

class Field {
public:
    Field(const byte *field_ptr, PageOffset length=0) : field_ptr(field_ptr), length(length) {};
    ~Field() = default;

    int64_t Int64() {
        return *((int64_t *) this->field_ptr);
    }

    int32_t Int32() {
        return *((int32_t *) this->field_ptr);
    }

    std::string Str() {
        return std::string((char *) this->field_ptr, this->length);
    }

    int16_t Int16() {
        return *((int16_t *) this->field_ptr);
    }

    double Double() {
        return *((double *) this->field_ptr);
    }

    const byte *Bytes() {
        return this->field_ptr;
    }

    void SetInt64(int64_t new_val) {
        *((int64_t *) this->field_ptr) = new_val;
    }


private:
    const byte *field_ptr;
    PageOffset length;
};

}}
#endif
