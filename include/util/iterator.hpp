/*
 *
 */
#ifndef ITERATOR_H
#define ITERATOR_H

#include "util/types.hpp"

namespace lsm {
namespace iter {

typedef uint64_t IteratorPosition;

template <class T>
class GenericIterator {
public:
    virtual bool next() = 0;
    virtual T get_item() = 0;

    virtual bool supports_rewind() = 0;
    virtual IteratorPosition save_position() = 0;
    virtual void rewind(IteratorPosition position) = 0;

    virtual void end_scan() = 0;
};

}
}

#endif
