#pragma once

#include "util/base.h"
#include "util/record.h"

#include "io/PagedFile.h"

namespace lsm {
struct Cursor {
    const char *ptr;
    const char *end;
};

/*
 * Advance the cursor to the next record. If the cursor is backed by an
 * iterator, will attempt to advance the iterator once the cursor reaches its
 * end and reset the cursor to the beginning of the read page.
 *
 * If the advance succeeds, ptr will be updated to point to the new record
 * and true will be returned. If the advance reaches the end, then ptr will
 * be updated to be equal to end, and false will be returned. Iterators will
 * not be closed.
 */
inline bool advance_cursor(Cursor &cur, PagedFileIterator *iter = nullptr) {
    cur.ptr += record_size;
    if (cur.ptr >= cur.end) {
        if (iter && iter->next()) {
            cur.ptr = iter->get_item();
            cur.end = cur.ptr + PAGE_SIZE;
            return true;
        }

        return false;
    }
    return true;
}
}
