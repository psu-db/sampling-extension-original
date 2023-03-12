#pragma once

#include "util/base.h"
#include "util/record.h"

#include "io/PagedFile.h"

namespace lsm {
struct Cursor {
    const char *ptr;
    const char *end;
    size_t cur_rec_idx;
    size_t rec_cnt;

    friend bool operator==(const Cursor &a, const Cursor &b) {
        return a.ptr == b.ptr && a.end == b.end;
    }
};

static Cursor g_empty_cursor = {0};

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
inline static bool advance_cursor(Cursor *cur, PagedFileIterator *iter = nullptr) {
    cur->ptr += record_size;
    cur->cur_rec_idx++;

    if (cur->cur_rec_idx >= cur->rec_cnt) return false;

    if (cur->ptr >= cur->end) {
        if (iter && iter->next()) {
            cur->ptr = iter->get_item();
            cur->end = cur->ptr + PAGE_SIZE;
            return true;
        }

        return false;
    }
    return true;
}

/*
 *   Process the list of cursors to return the cursor containing the next
 *   largest element. Does not advance any of the cursors. If current is
 *   specified, then skip the current head of that cursor during checking. 
 *   This allows for "peaking" at the next largest element after the current 
 *   largest is processed.
 */
inline static Cursor *get_next(std::vector<Cursor> &cursors, Cursor *current=&g_empty_cursor) {
    const char *min_rec = nullptr;
    Cursor *result = &g_empty_cursor;
    for (size_t i=0; i< cursors.size(); i++) {
        if (cursors[i] == g_empty_cursor) continue;

        const char *rec = (&cursors[i] == current) ? cursors[i].ptr + record_size : cursors[i].ptr;
        if (rec >= cursors[i].end) continue;

        if (min_rec == nullptr) {
            result = &cursors[i];
            min_rec = rec;
            continue;
        }

        if (record_cmp(rec, min_rec) == -1) {
            result = &cursors[i];
            min_rec = rec;
        }
    }

    return result;
} 

}
