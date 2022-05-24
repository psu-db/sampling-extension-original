/*
 *
 */

#ifndef PAGEDFILE_H
#define PAGEDFILE_H
#include <string>
#include <memory>
#include <cassert>

#include "util/types.hpp"
#include "util/base.hpp"
#include "util/iterator.hpp"
#include "io/directfile.hpp"
#include "io/page.hpp"

#define NO_BUFFER_MANAGER

namespace lsm {
namespace io {

// The header stored in Page 0 of the file, not file-level
// header information stored on each page.
struct PagedFileHeaderData {
    DirectFileHeaderData file_header;
    FileId flid;

    PageNum first_page;
    PageNum last_page;
    PageNum page_count;
    PageNum first_free_page;

};

constexpr size_t PagedFileHeaderSize = MAXALIGN(sizeof(PagedFileHeaderData));
static_assert(PagedFileHeaderSize <= parm::PAGE_SIZE);

class PagedFile;

class PageFileIterator : public iter::GenericIterator<Page *> {
friend PagedFile;

public:
    PageFileIterator(PagedFile *file, PageNum pnum);

    bool next() override;
    Page *get_item() override;

    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;

    void end_scan() override;

    ~PageFileIterator();
private:
    PagedFile *pfile;
    PageNum current_page;
    PageOffset current_offset;

    union iter_position {
        iter::IteratorPosition packed_position = 0;
        struct {
            PageNum pnum;
            PageOffset offset;
        };
    };
};

class PagedFile {
public:
    static std::unique_ptr<PagedFile> create(std::string fname, bool new_file);
    static std::unique_ptr<PagedFile> create_temporary();

    PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file);

    PageId allocate_page();

    int read_page(PageId pid, byte *buffer_ptr);
    int read_page(PageNum pnum, byte *buffer_ptr);

    int write_page(PageId pid, const byte *buffer_ptr);
    int write_page(PageNum pnum, const byte *buffer_ptr);

    int free_page(PageId pid);
    int free_page(PageNum pnum);

    PageId pnum_to_pid(PageNum pnum);

    bool is_temporary();
    void make_permanent();

    PageNum get_page_count();
    PageId get_first_pid();
    PageId get_last_pid();

    std::unique_ptr<PageFileIterator> start_scan(PageId pid=INVALID_PID);
    std::unique_ptr<PageFileIterator> start_scan(PageNum pnum=INVALID_PNUM);

    int remove_file();

    ~PagedFile();

private:
    static void initialize(DirectFile *dfile);
    static FileId next_flid();
    static inline off_t pnum_to_offset(PageNum pnum);
    static const PageNum header_page_pnum = INVALID_PNUM;


    void flush_metadata();
    bool check_pnum(PageNum pnum);

    std::unique_ptr<DirectFile> dfile;
    bool is_temp_file;
    PagedFileHeaderData header_data;

    #ifdef NO_BUFFER_MANAGER
    void flush_buffer(PageNum pnum);
    std::unique_ptr<byte> buffer;
    #endif
};

}
}

#endif
